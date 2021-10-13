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
            while True:
                conn, addr = ssock.accept()
                while True:
                    try:
                        data = conn.recv(16384)
                    except IOError:
                        break  # Connection closed.
                    if not data:
                        break  # Connection closed.
                print('Done reading')


server_t = threading.Thread(target=server)
server_t.start()
server_up.wait(10)  # Wait for the server to start accepting connections.
print('SERVER RUNNING')
