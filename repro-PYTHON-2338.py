import time
from pymongo import MongoClient


def create_client():
    client = MongoClient(directConnection=False, heartbeatFrequencyMS=500)
    client.admin.command('ping')
    return client


def cycle_clients():
    clients = [create_client() for _ in range(100)]
    for client in clients:
        client._topology.request_check_all()
    for client in clients:
        client.close()
    for client in clients:
        client.admin.command('ping')
    del clients


def main():
    for i in range(10):
        print(i)
        cycle_clients()
        time.sleep(.1)


if __name__ == '__main__':
    main()
