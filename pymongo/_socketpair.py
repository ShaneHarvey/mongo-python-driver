# Backport of the socketpair method from Python 3.8.3:
# https://github.com/python/cpython/blob/v3.8.3/Lib/socket.py#L578-L614
import sys

from pymongo.socket_checker import _errno_from_exception

try:
    # Use builtin sock.socketpair() when available (Python 3.5+).
    from socket import socketpair
except ImportError:
    import socket
    from socket import (AF_INET,
                        AF_INET6,
                        SOCK_STREAM)

    _LOCALHOST = '127.0.0.1'
    _LOCALHOST_V6 = '::1'

    if sys.platform == 'win32':
        from errno import WSAEWOULDBLOCK as EWOULDBLOCK
    else:
        from errno import EWOULDBLOCK

    # Origin: https://gist.github.com/4325783, by Geert Jansen.  Public domain.
    def socketpair(family=AF_INET, type=SOCK_STREAM, proto=0):
        if family == AF_INET:
            host = _LOCALHOST
        elif family == AF_INET6:
            host = _LOCALHOST_V6
        else:
            raise ValueError("Only AF_INET and AF_INET6 socket address families "
                             "are supported")
        if type != SOCK_STREAM:
            raise ValueError("Only SOCK_STREAM socket type is supported")
        if proto != 0:
            raise ValueError("Only protocol zero is supported")

        # We create a connected TCP socket. Note the trick with
        # setblocking(False) that prevents us from having to create a thread.
        lsock = socket.socket(family, type, proto)
        try:
            lsock.bind((host, 0))
            lsock.listen(1)
            # On IPv6, ignore flow_info and scope_id
            addr, port = lsock.getsockname()[:2]
            csock = socket.socket(family, type, proto)
            try:
                csock.setblocking(False)
                try:
                    csock.connect((addr, port))
                except socket.error as exc:
                    if _errno_from_exception(exc) != EWOULDBLOCK:
                        raise
                csock.setblocking(True)
                ssock, _ = lsock.accept()
            except:
                csock.close()
                raise
        finally:
            lsock.close()
        return (ssock, csock)
