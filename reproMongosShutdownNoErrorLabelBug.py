import logging
import random
import time
import threading

from bson.json_util import dumps, CANONICAL_JSON_OPTIONS

from pymongo import MongoClient, version
from pymongo.event_loggers import CommandLogger, HeartbeatLogger, ServerLogger

URI = None

LOGGING_FORMAT = '%(asctime)s [%(levelname)s] %(threadName)s:%(lineno)d - %(message)s'


def to_json(obj):
    return dumps(obj, json_options=CANONICAL_JSON_OPTIONS)


class MyCommandLogger(CommandLogger):
    def started(self, event):
        cmd = to_json(event.command)
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} started on server {0.connection_id}. "
                     "Full command:\n{1}".format(event, cmd))

    def succeeded(self, event):
        reply = to_json(event.reply)
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "succeeded in {0.duration_micros} "
                     "microseconds. Reply:\n{1}".format(event, reply))

    def failed(self, event):
        reply = getattr(event.failure, 'details', event.failure)
        reply_json = to_json(reply)
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "failed in {0.duration_micros} "
                     "microseconds. Failure:\n{1}".format(event, reply_json))


def delay(sec):
    return '''function() { sleep(%f * 1000); return true; }''' % sec


def find_and_modify(client):
    try:
        start = time.time()
        client.test.test.find_one_and_replace({'$where': delay(5)}, {'a': random.random()})
        time_taken = time.time() - start
        logging.info('FIND_AND_MODIFY took: %s', time_taken)
    except:
        logging.exception('Thread failed')


def shutdown_mongos_after(client, seconds):
    time.sleep(seconds)
    logging.info('Causing mongos shutdown via {shutdown:1, force:1}\n')
    client.admin.command('shutdown', 1, force=True)


def main():
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    listeners = [MyCommandLogger()]
    client = MongoClient('mongodb://localhost:27017,localhost:27018',
                         event_listeners=listeners, retryWrites=True)
    logging.info('Pymongo version: %s', version)
    logging.info('Mongos version: %s', client.server_info()['version'])
    client.test.test.insert_many([{}, {}])
    logging.info('Topology: %s', client._topology.description)

    # Run some retryable find_and_modify which each take a long time (
    # artificially delayed with $where).
    # At the same time, shutdown one of the mongoses.
    threads = [threading.Thread(target=shutdown_mongos_after, args=(client, 2))]
    threads.extend(threading.Thread(target=find_and_modify, args=(client,)) for _ in range(4))
    for t in threads:
        t.start()
    # Wait
    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
