import gc
import os
import resource

import time
# import tracemalloc

from pymongo import MongoClient
from pymongo.server_selectors import writable_server_selector
from pymongo.pool import _configured_socket

if hasattr(gc, 'set_debug'):
    gc.set_debug(
        gc.DEBUG_UNCOLLECTABLE |
        getattr(gc, 'DEBUG_OBJECTS', 0) |
        getattr(gc, 'DEBUG_INSTANCES', 0))


def get_pool(client):
    """Get the standalone, primary, or mongos pool."""
    topology = client._get_topology()
    server = topology.select_server(writable_server_selector)
    return server.pool


# tlsCAFile='/Users/shane/Downloads/cacert.pem'
# tlsInsecure=true removes the leak?
def main(num):
    with MongoClient(os.environ['URI'], heartbeatFrequencyMS=5*60*1000) as client:
        pool = get_pool(client)
        last_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        for i in range(num):
            #sock = _configured_socket(pool.address, pool.opts)
            #sock.close()
            client.admin.command('ping')
            pool.reset()
            time.sleep(0.01)
            print(i)
            mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            delta = mem - last_mem
            last_mem = mem
            print('Memory usage: %s (kb), delta: %s (kb)' % (mem, delta))

    time.sleep(10)


def trace():
    import tracemalloc
    tracemalloc.start(20)
    print('Warm up')
    main(10)
    snapshot1 = tracemalloc.take_snapshot()
    print('Main run')
    main(10)
    print('Memory usage: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    snapshot2 = tracemalloc.take_snapshot()
    # top_stats = snapshot2.compare_to(snapshot1, 'lineno')
    top_stats = snapshot2.compare_to(snapshot1, 'lineno')

    print("[ Top 25 ]")
    for stat in top_stats[:25]:
        print(stat)
    print()

    # pick the biggest memory block
    top_stats = snapshot2.statistics('traceback')
    stat = top_stats[0]
    print("%s memory blocks: %.1f KiB" % (stat.count, stat.size / 1024))
    for line in stat.traceback.format():
        print(line)


if __name__ == '__main__':
    # trace()
    main(50)
