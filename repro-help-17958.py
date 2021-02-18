import logging
import random
import sys
import time
import threading

from bson import SON

from pymongo import MongoClient
from pymongo.event_loggers import CommandLogger, HeartbeatLogger, ServerLogger
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import writable_server_selector
from pymongo.operations import ReplaceOne

URI = None

LOGGING_FORMAT = '%(asctime)s [%(levelname)s] %(threadName)s:%(lineno)d - %(message)s'


class MyCommandLogger(CommandLogger):
    def succeeded(self, event):
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "succeeded in {0.duration_micros} "
                     "microseconds. Reply:\n{0.reply}".format(event))

    def failed(self, event):
        reply = getattr(event.failure, 'details', event.failure)
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "failed in {0.duration_micros} "
                     "microseconds. Failure:\n{1}".format(event, reply))


def get_pool(client):
    """Get the standalone, primary, or mongos pool."""
    topology = client._get_topology()
    server = topology.select_server(writable_server_selector)
    return server.pool


def delay(sec):
    return '''function() { sleep(%f * 1000); return true; }''' % sec


def find(client):
    start = time.time()
    client.test.test.find_one({'$where': delay(1)})
    time_taken = time.time() - start
    logging.info('FIND took: %s', time_taken)


def find_and_modify(client):
    start = time.time()
    client.test.test.find_one_and_replace({'$where': delay(5)}, {'a': random.random()})
    time_taken = time.time() - start
    logging.info('FIND_AND_MODIFY took: %s', time_taken)

def bulk_update(client):
    start = time.time()
    res = client.test.test.bulk_write([
        ReplaceOne({'$where': delay(1)}, {'a': random.random()})
        for _ in range(5)])
    time_taken = time.time() - start
    logging.info('BULK_WRITE took: %s, %s', time_taken, res)

def insert(client):
    start = time.time()
    client.test.test.insert_many([{}, {}])
    time_taken = time.time() - start
    logging.info('INSERT took: %s', time_taken)


def repl_set_step_down(client, **kwargs):
    """Run replSetStepDown, first unfreezing a secondary with replSetFreeze."""
    cmd = SON([('replSetStepDown', 1)])
    cmd.update(kwargs)

    time.sleep(3)
    # Unfreeze a secondary to ensure a speedy election.
    client.admin.command(
        'replSetFreeze', 0, read_preference=ReadPreference.SECONDARY)
    client.admin.command(cmd)


def shutdown_shard_after(shard_port, seconds):
    with get_client(port=shard_port) as client:
        time.sleep(seconds)
        logging.info('Causing failover via {shutdown:1, force:1}\n')
        client.admin.command('shutdown', 1, force=1)
        # logging.info('Causing failover via replSetStepDown\n')
        # repl_set_step_down(client, replSetStepDown=10)


def shutdown_mongos_after(port, seconds):
    with get_client(port=port) as client:
        time.sleep(seconds)
        logging.info('Causing mongos shutdown via {shutdown:1, force:1}\n')
        client.admin.command('shutdown', 1, force=True)


def get_client(**kwargs):
    """Return a client connected to the whole replica set"""
    if URI:
        return MongoClient(URI, directConnection=False, **kwargs)
    return MongoClient(directConnection=False, **kwargs)


def run_continually(op, client):
    try:
        start = time.time()
        while time.time() - start < 8:
            op(client)
            time.sleep(0.005)
    except:
        logging.exception('Thread failed')


def main():
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    listeners = [ServerLogger(), HeartbeatLogger(), MyCommandLogger()]
    client = get_client(event_listeners=listeners, retryWrites=True, retryReads=True)#, w='majority', wtimeout=15000)
    client.admin.command('isMaster')
    logging.info('Topology: %s', client._topology.description)
    insert(client)
    print()
    logging.info('Running 4 insert threads\n')

    # Run retryable writes for 10 seconds.
    # threads = [threading.Thread(target=shutdown_shard_after, args=(27019, 2))]
    # threads = [threading.Thread(target=shutdown_mongos_after, args=(27017, 2))]
    threads = [threading.Thread(target=repl_set_step_down, args=(client,), kwargs=dict(replSetStepDown=10))]
    threads.extend(threading.Thread(target=run_continually, args=(bulk_update, client)) for _ in range(4))
    for t in threads:
        t.start()
    # Wait
    for t in threads:
        t.join()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        URI = sys.argv[1]
    main()
