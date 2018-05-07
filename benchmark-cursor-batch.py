import sys
import timeit

from functools import partial

import bson
import pymongo

from pymongo import network
from pymongo.errors import AutoReconnect

import errno

import os
uri = os.environ.get('ATLAS_URI')
if uri is None:
    uri = 'localhost'

mongo_client = pymongo.MongoClient(uri)
coll = mongo_client.test.test


def iterate_cursor(limit, batch_size):
    cursor = coll.find(batch_size=batch_size)
    rowCount = 0
    for _ in cursor:
        rowCount += 1


def load_data():
    fields = (
        "aaa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|ab|ac|ad|ae|af|ag|ah|ai"
        "|aj|ak|al|am|an|ao|ap|aq|ar|as|at|au|av|aw|ax|ay|az|ba|bb|bc|bd|be|bf"
        "|bg|bh|bi|bj|bk|bl|bm|bn|bo|bp|bq|br|bs|bt|bu|bv|bw|bx|by|bz|ca|cb|cc"
        "|cd|ce|cf|cg|ch|ci|cj|ck|cl|cm|cn|co|cp|cq|cr").split('|')
    vals = (
        "xxxxxxxx|x|xxxxxxxx|xxxxxx|xxxx|xxxxxxxx|x||||xx|xxxxxxxxxx|||xxxx"
        "|xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|xxxx|xxxxxxxx|xxxxxxxx||x|xx|xx"
        "|xxxxxxxxxx|||xxxxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxx||xx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxxxx|xxxxx|xxxxxxxx|||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxxxxxxxxxxx|xxxxxx|||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxx|xxxxxxxx||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxxx|xxxxxxxx||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxx|xxxxxxxx|xxxxxxx||||||xxxx|xx|xxxxx"
        "|xxxxxxxxxxxxxx|xxxxxxxxxxxxxx||xx|xxxxx|xxxxxxxxx|xxxxxxxxxxxxx"
        "||xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|||||||||xx|xx").split('|')

    coll.drop()
    for i in range(10):
        docs = [bson.SON(zip(fields, vals)) for _ in range(10000)]
        coll.insert_many(docs)


def time(fn):
    """Measure how long it takes to call fn 1 times, take best of 3 trials."""
    return min(timeit.Timer(fn).repeat(3, number=1))



def _errno_from_exception(exc):
    if hasattr(exc, 'errno'):
        return exc.errno
    elif exc.args:
        return exc.args[0]
    else:
        return None


def _receive_data_extend(sock, length):
    msg = bytearray(length)
    i = 0
    while length:
        try:
            chunk = sock.recv(length)
        except (IOError, OSError) as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk == b"":
            raise AutoReconnect("connection closed")

        msg[i:i + len(chunk)] = chunk
        i += len(chunk)
        length -= len(chunk)

    return bytes(msg)


def _receive_data_extend_min(sock, length):
    msg = bytearray(length)
    i = 0
    while length:
        try:
            chunk = sock.recv(min(length, 65536))
        except (IOError, OSError) as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk == b"":
            raise AutoReconnect("connection closed")

        msg[i:i + len(chunk)] = chunk
        i += len(chunk)
        length -= len(chunk)

    return bytes(msg)

def _receive_data_addition(sock, length):
    msg = b""
    while length:
        try:
            chunk = sock.recv(length)
        except (IOError, OSError) as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk == b"":
            raise AutoReconnect("connection closed")

        length -= len(chunk)
        msg += chunk

    return msg

def _receive_data_join(sock, length):
    chunks = []
    while length:
        try:
            chunk = sock.recv(length)
        except (IOError, OSError) as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk == b"":
            raise AutoReconnect("connection closed")

        length -= len(chunk)
        chunks.append(chunk)

    return b"".join(chunks)


def _receive_data_addition_min(sock, length):
    msg = b""
    while length:
        try:
            chunk = sock.recv(min(length, 65536))
        except (IOError, OSError) as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk == b"":
            raise AutoReconnect("connection closed")

        length -= len(chunk)
        msg += chunk

    return msg

def _receive_data_join_min(sock, length):
    chunks = []
    while length:
        try:
            chunk = sock.recv(min(length, 32768))
        except (IOError, OSError) as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk == b"":
            raise AutoReconnect("connection closed")

        length -= len(chunk)
        chunks.append(chunk)

    return b"".join(chunks)


def _receive_data_memoryview(sock, length):
    buf = bytearray(length)
    mv = memoryview(buf)
    bytes_read = 0
    while bytes_read < length:
        try:
            chunk_length = sock.recv_into(mv[bytes_read:])
        except (IOError, OSError) as exc:
            if _errno_from_exception(exc) == errno.EINTR:
                continue
            raise
        if chunk_length == 0:
            raise AutoReconnect("connection closed")

        bytes_read += chunk_length

    return mv


if __name__ == "__main__":
    print('Python version: %s' % sys.version)
    print('PyMongo version: %s' % pymongo.version)
    print('bson.has_c(): %s' % (bson.has_c(),))
    if 'load_data' in sys.argv[1:]:
        print('loading data')
        load_data()

    print("%10s | %11s | %11s | %11s | %11s | %11s |" % ('batch_size', 'recv +=', 'recv += min', 'recv extend', 'recv extend min', 'recv_into'))
    for batch_size in (100, 1000000):
        sys.stdout.write("%10s |" % (batch_size,))
        sys.stdout.flush()
        for recv in (_receive_data_addition, _receive_data_addition_min,
                     _receive_data_extend, _receive_data_extend_min,
                     _receive_data_memoryview):
            network._receive_data_on_socket = recv
            t = time(partial(iterate_cursor, None, batch_size))
            sys.stdout.write(" %11.2f |" % (t,))
            sys.stdout.flush()
        print("")

"""
$ python benchmark-cursor-batch.py
Python version: 2.7.10
PyMongo version: 3.6.1
bson.has_c(): True
batch_size |     recv += | recv += min |   recv_into |
       100 |        2.69 |        2.65 |        2.65 |
   1000000 |        3.50 |        3.35 |        3.27 |
   
$ python3 benchmark-cursor-batch.py
Python version: 3.6.1
PyMongo version: 3.6.1
bson.has_c(): True
batch_size |     recv += | recv += min |   recv_into |
       100 |        4.32 |        4.20 |        4.18 |
   1000000 |        4.24 |       15.68 |        3.27 |
"""
