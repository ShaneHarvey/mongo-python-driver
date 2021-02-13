import concurrent.futures

import sys
import timeit

from functools import partial

import bson
import pymongo
from pymongo import MongoClient
from pymongo import common
from pymongo.monitoring import ServerListener
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import writable_server_selector, secondary_server_selector

N_WORKERS = 110

client = MongoClient(connect=False)
coll = client.test.test


class ServerLogger(ServerListener):
    def opened(self, event): pass

    def description_changed(self, event):
        if not event.new_description.is_server_type_known:
            print(f'Event [{event.server_address}]: {event.new_description}')

    def closed(self, event): pass


def reset_client():
    global client, coll
    client.close()
    client = MongoClient(
        'mongodb://user:password@localhost:27017/?authSource=admin&tls=true&tlsInsecure=true&directConnection=false&localThresholdMS=1000',
        # event_listeners=[ServerLogger()],
    )
    coll = client.test.test.with_options(read_preference=ReadPreference.SECONDARY)


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


def get_secondary_pools(client):
    """Get the secondary pools."""
    topology = client._get_topology()
    servers = topology.select_servers(secondary_server_selector)
    return sorted([server.pool for server in servers],
                  key=lambda pool: pool.address)


def n_connections(pool):
    return len(pool.sockets)


if __name__ == "__main__":
    reset_client()
    print(f'PyMongo version: {pymongo.__version__}')
    print(f'MongoDB version: {client.server_info()["version"]}')
    print(f'MongoDB cluster: {client._topology.description}')
    print(f'bson.has_c(): {bson.has_c()}')
    coll.insert_one({})
    for n_requests in (200, 10000):
        print(f'Executing {n_requests} findOne operations with {N_WORKERS} worker threads')
        print("%13s: %20s: %11s: %11s:" % ("maxConnecting", "find_one time", "connections A", "connections B"))
        for max_connecting in (2, 100):
            common.MAX_CONNECTING = max_connecting
            t = time(partial(benchmark, n_requests))
            a, b = get_secondary_pools(client)
            print("%13s %20.2fs %12s %12s" % (max_connecting, t, n_connections(a), n_connections(b)))

# Output:
# python3.9 benchmark-operationCount.py
# PyMongo version: 4.0.dev0
# MongoDB version: 4.4.3
# MongoDB cluster: <TopologyDescription id: 602c3b7739c5b64451dfe98a,
# topology_type: ReplicaSetWithPrimary, servers: [<ServerDescription (
# 'localhost', 27017) server_type: RSSecondary, rtt: 0.0005159730000000029>,
# <ServerDescription ('localhost', 27018) server_type: RSPrimary,
# rtt: 0.00038200400000000523>, <ServerDescription ('localhost', 27019)
# server_type: RSSecondary, rtt: 0.00035782500000000605>]>
# bson.has_c(): True
# Executing 200 findOne operations with 110 worker threads
# maxConnecting:        find_one time: connections A: connections B:
#             2                 0.16s            4            4
#           100                 1.45s          100           55
# Executing 10000 findOne operations with 110 worker threads
# maxConnecting:        find_one time: connections A: connections B:
#             2                 3.40s           43           44
#           100                 3.43s           55           94