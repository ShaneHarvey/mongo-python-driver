# Copyright 2015-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal network layer helper methods."""
from __future__ import annotations

import datetime
import errno
import socket
import struct
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
    cast,
)

from bson import _decode_all_selective
from pymongo import _csot, helpers, message, ssl_support
from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.compression_support import _NO_COMPRESSION, decompress
from pymongo.errors import (
    NotPrimaryError,
    OperationFailure,
    ProtocolError,
    _OperationCancelled,
)
from pymongo.message import _UNPACK_REPLY, _OpMsg, _OpReply
from pymongo.monitoring import _is_speculative_authenticate
from pymongo.socket_checker import _errno_from_exception

if TYPE_CHECKING:
    from bson import CodecOptions
    from pymongo.client_session import ClientSession
    from pymongo.compression_support import SnappyContext, ZlibContext, ZstdContext
    from pymongo.mongo_client import MongoClient
    from pymongo.monitoring import _EventListeners
    from pymongo.pool import Connection
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import _ServerMode
    from pymongo.typings import _Address, _CollationIn, _DocumentOut, _DocumentType
    from pymongo.write_concern import WriteConcern

_UNPACK_HEADER = struct.Struct("<iiii").unpack


def command(
    conn: Connection,
    dbname: str,
    spec: MutableMapping[str, Any],
    is_mongos: bool,
    read_preference: Optional[_ServerMode],
    codec_options: CodecOptions[_DocumentType],
    session: Optional[ClientSession],
    client: Optional[MongoClient],
    check: bool = True,
    allowable_errors: Optional[Sequence[Union[str, int]]] = None,
    address: Optional[_Address] = None,
    listeners: Optional[_EventListeners] = None,
    max_bson_size: Optional[int] = None,
    read_concern: Optional[ReadConcern] = None,
    parse_write_concern_error: bool = False,
    collation: Optional[_CollationIn] = None,
    compression_ctx: Union[SnappyContext, ZlibContext, ZstdContext, None] = None,
    use_op_msg: bool = False,
    unacknowledged: bool = False,
    user_fields: Optional[Mapping[str, Any]] = None,
    exhaust_allowed: bool = False,
    write_concern: Optional[WriteConcern] = None,
) -> _DocumentType:
    """Execute a command over the socket, or raise socket.error.

    :param conn: a Connection instance
    :param dbname: name of the database on which to run the command
    :param spec: a command document as an ordered dict type, eg SON.
    :param is_mongos: are we connected to a mongos?
    :param read_preference: a read preference
    :param codec_options: a CodecOptions instance
    :param session: optional ClientSession instance.
    :param client: optional MongoClient instance for updating $clusterTime.
    :param check: raise OperationFailure if there are errors
    :param allowable_errors: errors to ignore if `check` is True
    :param address: the (host, port) of `conn`
    :param listeners: An instance of :class:`~pymongo.monitoring.EventListeners`
    :param max_bson_size: The maximum encoded bson size for this server
    :param read_concern: The read concern for this command.
    :param parse_write_concern_error: Whether to parse the ``writeConcernError``
        field in the command response.
    :param collation: The collation for this command.
    :param compression_ctx: optional compression Context.
    :param use_op_msg: True if we should use OP_MSG.
    :param unacknowledged: True if this is an unacknowledged command.
    :param user_fields: Response fields that should be decoded
        using the TypeDecoders from codec_options, passed to
        bson._decode_all_selective.
    :param exhaust_allowed: True if we should enable OP_MSG exhaustAllowed.
    """
    name = next(iter(spec))
    ns = dbname + ".$cmd"
    speculative_hello = False

    # Publish the original command document, perhaps with lsid and $clusterTime.
    orig = spec
    if is_mongos and not use_op_msg:
        assert read_preference is not None
        spec = message._maybe_add_read_preference(spec, read_preference)
    if read_concern and not (session and session.in_transaction):
        if read_concern.level:
            spec["readConcern"] = read_concern.document
        if session:
            session._update_read_concern(spec, conn)
    if collation is not None:
        spec["collation"] = collation

    publish = listeners is not None and listeners.enabled_for_commands
    if publish:
        start = datetime.datetime.now()
        speculative_hello = _is_speculative_authenticate(name, spec)

    if compression_ctx and name.lower() in _NO_COMPRESSION:
        compression_ctx = None

    if client and client._encrypter and not client._encrypter._bypass_auto_encryption:
        spec = orig = client._encrypter.encrypt(dbname, spec, codec_options)

    # Support CSOT
    if client:
        conn.apply_timeout(client, spec)
    _csot.apply_write_concern(spec, write_concern)

    if use_op_msg:
        flags = _OpMsg.MORE_TO_COME if unacknowledged else 0
        flags |= _OpMsg.EXHAUST_ALLOWED if exhaust_allowed else 0
        request_id, msg, size, max_doc_size = message._op_msg(
            flags, spec, dbname, read_preference, codec_options, ctx=compression_ctx
        )
        # If this is an unacknowledged write then make sure the encoded doc(s)
        # are small enough, otherwise rely on the server to return an error.
        if unacknowledged and max_bson_size is not None and max_doc_size > max_bson_size:
            message._raise_document_too_large(name, size, max_bson_size)
    else:
        request_id, msg, size = message._query(
            0, ns, 0, -1, spec, None, codec_options, compression_ctx
        )

    if max_bson_size is not None and size > max_bson_size + message._COMMAND_OVERHEAD:
        message._raise_document_too_large(name, size, max_bson_size + message._COMMAND_OVERHEAD)

    if publish:
        encoding_duration = datetime.datetime.now() - start
        assert listeners is not None
        assert address is not None
        listeners.publish_command_start(
            orig,
            dbname,
            request_id,
            address,
            conn.server_connection_id,
            service_id=conn.service_id,
        )
        start = datetime.datetime.now()

    try:
        conn.conn.sendall(msg)
        if use_op_msg and unacknowledged:
            # Unacknowledged, fake a successful command response.
            reply = None
            response_doc: _DocumentOut = {"ok": 1}
        else:
            reply = receive_message(conn, request_id)
            conn.more_to_come = reply.more_to_come
            unpacked_docs = reply.unpack_response(
                codec_options=codec_options, user_fields=user_fields
            )

            response_doc = unpacked_docs[0]
            if client:
                client._process_response(response_doc, session)
            if check:
                helpers._check_command_response(
                    response_doc,
                    conn.max_wire_version,
                    allowable_errors,
                    parse_write_concern_error=parse_write_concern_error,
                )
    except Exception as exc:
        if publish:
            duration = (datetime.datetime.now() - start) + encoding_duration
            if isinstance(exc, (NotPrimaryError, OperationFailure)):
                failure: _DocumentOut = exc.details  # type: ignore[assignment]
            else:
                failure = message._convert_exception(exc)
            assert listeners is not None
            assert address is not None
            listeners.publish_command_failure(
                duration,
                failure,
                name,
                request_id,
                address,
                conn.server_connection_id,
                service_id=conn.service_id,
                database_name=dbname,
            )
        raise
    if publish:
        duration = (datetime.datetime.now() - start) + encoding_duration
        assert listeners is not None
        assert address is not None
        listeners.publish_command_success(
            duration,
            response_doc,
            name,
            request_id,
            address,
            conn.server_connection_id,
            service_id=conn.service_id,
            speculative_hello=speculative_hello,
            database_name=dbname,
        )

    if client and client._encrypter and reply:
        decrypted = client._encrypter.decrypt(reply.raw_command_response())
        response_doc = cast(
            "_DocumentOut", _decode_all_selective(decrypted, codec_options, user_fields)[0]
        )

    return response_doc  # type: ignore[return-value]


_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack


def receive_message(
    conn: Connection, request_id: Optional[int], max_message_size: int = MAX_MESSAGE_SIZE
) -> Union[_OpReply, _OpMsg]:
    """Receive a raw BSON message or raise socket.error."""
    if _csot.get_timeout():
        deadline = _csot.get_deadline()
    else:
        timeout = conn.conn.gettimeout()
        if timeout:
            deadline = time.monotonic() + timeout
        else:
            deadline = None
    # Ignore the response's request id.
    length, _, response_to, op_code = _UNPACK_HEADER(_receive_data_on_socket(conn, 16, deadline))
    # No request_id for exhaust cursor "getMore".
    if request_id is not None:
        if request_id != response_to:
            raise ProtocolError(f"Got response id {response_to!r} but expected {request_id!r}")
    if length <= 16:
        raise ProtocolError(
            f"Message length ({length!r}) not longer than standard message header size (16)"
        )
    if length > max_message_size:
        raise ProtocolError(
            f"Message length ({length!r}) is larger than server max "
            f"message size ({max_message_size!r})"
        )
    if op_code == 2012:
        op_code, _, compressor_id = _UNPACK_COMPRESSION_HEADER(
            _receive_data_on_socket(conn, 9, deadline)
        )
        data = decompress(_receive_data_on_socket(conn, length - 25, deadline), compressor_id)
    else:
        data = _receive_data_on_socket(conn, length - 16, deadline)

    try:
        unpack_reply = _UNPACK_REPLY[op_code]
    except KeyError:
        raise ProtocolError(
            f"Got opcode {op_code!r} but expected {_UNPACK_REPLY.keys()!r}"
        ) from None
    return unpack_reply(data)


_POLL_TIMEOUT = 0.5


def wait_for_read(conn: Connection, deadline: Optional[float]) -> None:
    """Block until at least one byte is read, or a timeout, or a cancel."""
    sock = conn.conn
    timed_out = False
    # Check if the connection's socket has been manually closed
    if sock.fileno() == -1:
        return
    while True:
        # SSLSocket can have buffered data which won't be caught by select.
        if hasattr(sock, "pending") and sock.pending() > 0:
            readable = True
        else:
            # Wait up to 500ms for the socket to become readable and then
            # check for cancellation.
            if deadline:
                remaining = deadline - time.monotonic()
                # When the timeout has expired perform one final check to
                # see if the socket is readable. This helps avoid spurious
                # timeouts on AWS Lambda and other FaaS environments.
                if remaining <= 0:
                    timed_out = True
                timeout = max(min(remaining, _POLL_TIMEOUT), 0)
            else:
                timeout = _POLL_TIMEOUT
            readable = conn.socket_checker.select(sock, read=True, timeout=timeout)
        if conn.cancel_context.cancelled:
            raise _OperationCancelled("operation cancelled")
        if readable:
            return
        if timed_out:
            raise socket.timeout("timed out")


# Errors raised by sockets (and TLS sockets) when in non-blocking mode.
BLOCKING_IO_ERRORS = (BlockingIOError, *ssl_support.BLOCKING_IO_ERRORS)

from collections import defaultdict

_lengths = defaultdict(int)


def _receive_data_on_socket(conn: Connection, length: int, deadline: Optional[float]) -> memoryview:
    buf = bytearray(length)
    mv = memoryview(buf)
    bytes_read = 0
    while bytes_read < length:
        try:
            wait_for_read(conn, deadline)
            # CSOT: Update timeout. When the timeout has expired perform one
            # final non-blocking recv. This helps avoid spurious timeouts when
            # the response is actually already buffered on the client.
            if _csot.get_timeout() and deadline is not None:
                conn.set_conn_timeout(max(deadline - time.monotonic(), 0))
            chunk = conn.conn.recv(len(mv[bytes_read:]))
            chunk_length = len(chunk)
            mv[bytes_read : bytes_read + chunk_length] = chunk
            _lengths[chunk_length] += 1
        except BLOCKING_IO_ERRORS:
            raise socket.timeout("timed out") from None
        except OSError as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk_length == 0:
            raise OSError("connection closed")

        bytes_read += chunk_length

    return mv


import atexit


def print_stats(lengths=_lengths):
    print("length\t\tcount")
    for length in sorted(lengths):
        print(f"{length}\t\t{_lengths[length]}")


atexit.register(print_stats)

"""
WITH TLS, local standalone:
length          count
0               10
16              78
22              4
29              8
90              1
91              1
98              2
113             1
114             1
191             10
288             1
313             5
982             1
2907            1
6315            10
8596            1
9198            1
14572           20
14573           10
16368           42
16384           31938

NO TLS:
length          count
16              80
22              4
29              8
90              1
91              1
98              2
113             1
114             1
191             10
208             944
288             1
313             6
787             1
2907            2
6240            1
6315            3
10300           1
10716           1
14728           2
14729           5
16332           1637
16540           1386
16748           2
17164           1
19768           1
22855           3
29436           1
31060           2
31061           1
32664           1923
32872           1280
33080           2
39187           1
44860           1
47392           4
48996           900
49204           753
55028           1
55519           1
63724           7
63725           2
63932           2
63933           1
64912           1
65328           596
65536           1451
65744           54
68552           1
71851           2
73979           1
74092           1
75808           1
78640           1
80264           1
81660           12
81868           392
82076           25
96596           1
97992           16
98200           260
98408           25
107482          1
114324          4
114532          49
114740          16
129468          1
129469          1
130656          1
130864          22
131072          52
131280          6
146988          2
147404          15
147612          2
163736          5
163944          3
180068          2
180276          7
196400          6
196608          18
196816          1
212940          8
213148          2
229272          3
229480          2
245604          2
245812          1
261312          1
261936          3
262144          17
262352          1
278476          4
294808          2
311140          1
327680          3
342972          1
344012          1
359304          1
360344          2
376676          1
393216          2
524272          1
737212          1
753544          2
786416          1
819080          1
851744          6
917488          7
982816          1
983024          9
999356          1
1015688         1
1048560         2
1064892         1
1130428         1
1179424         1
1212296         1
1245168         2
1774112         1
"""
