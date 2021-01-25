import concurrent.futures

import sys
import timeit

from functools import partial

import bson
import pymongo
from pymongo import MongoClient
from pymongo import common
from pymongo.server_selectors import writable_server_selector

N_WORKERS = 110

client = MongoClient(connect=False)
coll = client.test.test


def reset_client():
    global client, coll
    client.close()
    client = MongoClient('mongodb://user:password@localhost:27017/?authSource=admin&tls=true&tlsInsecure=true')
    coll = client.test.test


def delay(sec):
    return '''function() { sleep(%f * 1000); return true; }''' % sec


def find(i):
    doc = coll.find_one()#{'$where': delay(0.005)})


def benchmark(n_requests):
    reset_client()
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
        # Start the load operations and mark each future with its URL
        futures = [executor.submit(find, i) for i in range(n_requests)]
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (future, exc))


def time(fn):
    """Measure how long it takes to call fn 1 times, take best of 3 trials."""
    return min(timeit.Timer(fn).repeat(3, number=1))


def get_pool(client):
    """Get the standalone, primary, or mongos pool."""
    topology = client._get_topology()
    server = topology.select_server(writable_server_selector)
    return server.pool


def n_connections(client):
    return len(get_pool(client).sockets)


if __name__ == "__main__":
    reset_client()
    print(f'PyMongo version: {pymongo.__version__}')
    print(f'MongoDB version: {client.server_info()["version"]}')
    print(f'MongoDB cluster: {client._topology.description}')
    print(f'bson.has_c(): {bson.has_c()}')
    coll.insert_one({})
    for n_requests in (200, 10000):
        print(f'Executing {n_requests} findOne operations with {N_WORKERS} worker threads')
        print("%13s: %20s: %11s:" % ("maxConnecting", "find_one time", "connections"))
        for max_connecting in (2, 3, 5, 10, 100):
            common.MAX_CONNECTING = max_connecting
            t = time(partial(benchmark, n_requests))
            print("%13s %20.2fs %12s" % (max_connecting, t, n_connections(client)))

# Output:
# PyMongo version: 4.0.dev0
# MongoDB version: 4.4.3
# MongoDB cluster: <TopologyDescription id: 600f4f3a9a0f341f7f7eed36,
# topology_type: Single, servers: [<ServerDescription ('localhost', 27017)
# server_type: Standalone, rtt: 0.00042395099999999186>]>
# bson.has_c(): True
# Executing 200 findOne operations with 110 worker threads
# maxConnecting:        find_one time: connections:
#             2                 0.10s            8
#             3                 0.12s           10
#             5                 0.13s           11
#            10                 0.16s           16
#           100                 1.01s          100
# Executing 10000 findOne operations with 110 worker threads
# maxConnecting:        find_one time: connections:
#             2                 2.79s           60
#             3                 2.85s           73
#             5                 2.84s           96
#            10                 2.78s          100
#           100                 2.76s          100
