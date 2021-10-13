import ssl
import sys
import socket
import threading

print(sys.version)

CLIENT_PEM = 'client.pem'
SERVER_PEM = 'server.pem'
CA_PEM = 'ca.pem'

server_up = threading.Event()

def server():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # verify certs and host name in client mode
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = False
    context.load_cert_chain(SERVER_PEM)
    context.load_verify_locations(CA_PEM)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', 27443))
        sock.listen(5)
        with context.wrap_socket(sock, server_side=True) as ssock:
            server_up.set()
            conn, addr = ssock.accept()
            while True:
                try:
                    data = conn.recv(16384)
                except IOError:
                    break  # Connection closed.
                if not data:
                    break  # Connection closed.
            print('Done reading')


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
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.load_cert_chain(CLIENT_PEM)
    context.load_verify_locations(CA_PEM)
    with socket.create_connection(('127.0.0.1', 27443)) as sock:
        with context.wrap_socket(sock) as ssock:
            print(f'Sendall: {len(LARGE_MSG)} bytes')
            sendall(ssock, LARGE_MSG)
            print('Done sending')


server_t = threading.Thread(target=server)
server_t.start()
server_up.wait(10)  # Wait for the server to start accepting connections.
client()
