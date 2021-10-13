import ssl
import sys
import socket
import threading

print(sys.version)

CLIENT_PEM = 'client.pem'
SERVER_PEM = 'server.pem'
CA_PEM = 'ca.pem'

server_up = threading.Event()

LARGE_MSG = b'\0'*(45*1024*1024)


def sendall(sock, msg):
    sock.sendall(msg)

# Chunking the data to send improves performance on PyPy but is
# still much slower than CPython.
CHUNK_SIZE = 16384
def sendallchunked(sock, msg):
    view = memoryview(msg)
    while view:
        sock.sendall(view[:CHUNK_SIZE])
        view = view[CHUNK_SIZE:]


def client():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    context.check_hostname = False
    context.load_cert_chain(CLIENT_PEM)
    context.load_verify_locations(CA_PEM)
    sock = socket.create_connection(('127.0.0.1', 27443))
    ssock = context.wrap_socket(sock)
    print('Sendall: %s bytes' % len(LARGE_MSG))
    sendall(ssock, LARGE_MSG)
    print('Done sending')
    ssock.close()
    sock.close()


client()
