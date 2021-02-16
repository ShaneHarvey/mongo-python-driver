import concurrent.futures

import sys
import timeit

from functools import partial

import bson
import pymongo
from pymongo import MongoClient
from pymongo import common
from pymongo.monitoring import ServerListener, ConnectionPoolListener
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import writable_server_selector, secondary_server_selector

N_WORKERS = 110


class ServerLogger(ServerListener):
    def opened(self, event): pass

    def description_changed(self, event):
        if not event.new_description.is_server_type_known:
            print(f'Event [{event.server_address}]: {event.new_description}')

    def closed(self, event): pass


class PoolListener(ConnectionPoolListener):
    def __init__(self):
        self.stats = {}

    def pool_created(self, event): pass
    def pool_ready(self, event): pass

    def pool_cleared(self, event): pass

    def pool_closed(self, event): pass

    def connection_created(self, event):
        """Abstract method to handle a :class:`ConnectionCreatedEvent`.

        Emitted when a Connection Pool creates a Connection object.

        :Parameters:
          - `event`: An instance of :class:`ConnectionCreatedEvent`.
        """
        self.stats.setdefault(event.address, {'conns': 0, 'ops': 0})['conns'] += 1

    def connection_ready(self, event): pass

    def connection_closed(self, event): pass

    def connection_check_out_started(self, event): pass

    def connection_check_out_failed(self, event): pass

    def connection_checked_out(self, event):
        """Abstract method to handle a :class:`ConnectionCheckedOutEvent`.

        Emitted when the driver successfully checks out a Connection.

        :Parameters:
          - `event`: An instance of :class:`ConnectionCheckedOutEvent`.
        """
        self.stats.setdefault(event.address, {'conns': 0, 'ops': 0})['ops'] += 1

    def connection_checked_in(self, event): pass


client = MongoClient(connect=False)
coll = client.test.test
listener = PoolListener()


def reset_client():
    global client, coll, listener
    client.close()
    listener = PoolListener()
    client = MongoClient(
        'mongodb://user:password@localhost:27017/?authSource=admin&tls=true&tlsInsecure=true&directConnection=false&localThresholdMS=1000',
        event_listeners=[listener],
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


def get_stats(client):
    """Get the secondary pools."""
    a, b = get_secondary_pools(client)
    stats = listener.stats
    return stats[a.address], stats[b.address]


def n_connections(pool):
    return len(pool.sockets)

from contextlib import contextmanager
from bson import SON
@contextmanager
def fail_point(command_args):
    cmd_on = SON([('configureFailPoint', 'failCommand')])
    cmd_on.update(command_args)
    c = MongoClient(
        'mongodb://user:password@localhost:27019/?authSource=admin&tls=true&tlsInsecure=true&directConnection=true')
    c.admin.command(cmd_on)
    try:
        yield
    finally:
        c.admin.command(
            'configureFailPoint', cmd_on['configureFailPoint'], mode='off')

def bench():
    for n_requests in (200, 10000):
        print(f'Executing {n_requests} findOne operations with {N_WORKERS} worker threads')
        print("%13s: %20s: %11s: %11s: %7s: %7s:" % ("maxConnecting", "find_one time", "connections A", "connections B", "ops A", "ops B"))
        for max_connecting in (2, 100):
            common.MAX_CONNECTING = max_connecting
            t = time(partial(benchmark, n_requests))
            a, b = get_stats(client)
            print("%13s %20.2fs %14s %14s %8s %8s" % (max_connecting, t, a['conns'], b['conns'], a['ops'], b['ops']))


if __name__ == "__main__":
    reset_client()
    print(f'PyMongo version: {pymongo.__version__}')
    print(f'MongoDB version: {client.server_info()["version"]}')
    print(f'MongoDB cluster: {client._topology.description}')
    print(f'bson.has_c(): {bson.has_c()}')
    coll.insert_one({})
    bench()
    print('Running benchmark with delayed server')
    delay_find = {
        'configureFailPoint': 'failCommand',
        'mode': {'times': 1000000},
        'data': {
            'failCommands': ['find'],
            'blockConnection': True,
            'blockTimeMS': 500,
        },
    }
    with fail_point(delay_find):
        bench()

# Output:
# PyMongo version: 4.0.dev0
# MongoDB version: 4.4.3
# MongoDB cluster: <TopologyDescription id: 602c537011c320122be9fd62, topology_type: ReplicaSetWithPrimary, servers: [<ServerDescription ('localhost', 27017) server_type: RSSecondary, rtt: 0.0004559100000000038>, <ServerDescription ('localhost', 27018) server_type: RSPrimary, rtt: 0.0004028969999999993>, <ServerDescription ('localhost', 27019) server_type: RSSecondary, rtt: 0.0003537460000000159>]>
# bson.has_c(): True
# Executing 200 findOne operations with 110 worker threads
# maxConnecting:        find_one time: connections A: connections B:   ops A:   ops B:
#             2                 0.17s              6              5      109       91
#           100                 1.44s            100             51      110       90
# Executing 10000 findOne operations with 110 worker threads
# maxConnecting:        find_one time: connections A: connections B:   ops A:   ops B:
#             2                 3.49s             45             41     5395     4605
#           100                 3.87s            100             55     6074     3926