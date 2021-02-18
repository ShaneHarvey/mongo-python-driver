import logging
import sys
import time
import threading

from bson import SON

from pymongo import monitoring, MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import writable_server_selector

URI = None


class ServerLogger(monitoring.ServerListener):

    def opened(self, event):
        logging.info("Server {0.server_address} added to topology "
                     "{0.topology_id}".format(event))

    def description_changed(self, event):
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            # server_type_name was added in PyMongo 3.4
            logging.info(
                "Server {0.server_address} changed type from "
                "{0.previous_description.server_type_name} to "
                "{0.new_description.server_type_name}".format(event))

    def closed(self, event):
        logging.warning("Server {0.server_address} removed from topology "
                        "{0.topology_id}".format(event))


class HeartbeatLogger(monitoring.ServerHeartbeatListener):

    def started(self, event):
        logging.info("Heartbeat sent to server "
                     "{0.connection_id}".format(event))

    def succeeded(self, event):
        # The reply.document attribute was added in PyMongo 3.4.
        logging.info("Heartbeat to server {0.connection_id} "
                     "succeeded with reply "
                     "{0.reply.document}".format(event))

    def failed(self, event):
        if hasattr(event.reply, 'details'):
            resp = event.reply.details
            extra = ": {0}".format(resp)
        else:
            extra = ""
        logging.warning("Heartbeat to server {0.connection_id} "
                        "failed with error {0.reply}{1}".format(event, extra))


class TopologyLogger(monitoring.TopologyListener):

    def opened(self, event):
        logging.info("Topology with id {0.topology_id} "
                     "opened".format(event))

    def description_changed(self, event):
        logging.info("Topology description updated for "
                     "topology id {0.topology_id}".format(event))
        previous_topology_type = event.previous_description.topology_type
        new_topology_type = event.new_description.topology_type
        if new_topology_type != previous_topology_type:
            # topology_type_name was added in PyMongo 3.4
            logging.info(
                "Topology {0.topology_id} changed type from "
                "{0.previous_description.topology_type_name} to "
                "{0.new_description.topology_type_name}".format(event))
        # The has_writable_server and has_readable_server methods
        # were added in PyMongo 3.4.
        if not event.new_description.has_writable_server():
            logging.warning("No writable servers available.")

    def closed(self, event):
        logging.info("Topology with id {0.topology_id} "
                     "closed".format(event))


class CommandLogger(monitoring.CommandListener):

    def started(self, event):
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} started on server "
                     "{0.connection_id}".format(event))

    def succeeded(self, event):
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "succeeded in {0.duration_micros} "
                     "microseconds".format(event))

    def failed(self, event):
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "failed in {0.duration_micros} "
                     "microseconds".format(event))

LOGGING_FORMAT = '%(asctime)s [%(levelname)s] %(threadName)s:%(lineno)d - %(message)s'


def get_pool(client):
    """Get the standalone, primary, or mongos pool."""
    topology = client._get_topology()
    server = topology.select_server(writable_server_selector)
    return server.pool


def find(client):
    start = time.time()
    client.test.test.find_one()
    time_taken = time.time() - start
    logging.info('FIND took: %s', time_taken)


def insert(client):
    start = time.time()
    client.test.test.insert_one({})
    time_taken = time.time() - start
    logging.info('INSERT took: %s', time_taken)


def _fail_point(client, command_args):
    cmd_on = SON([('configureFailPoint', 'failCommand')])
    cmd_on.update(command_args)
    client.admin.command(cmd_on)


def fail_insert(client, network_error=False):
    config = {
        "configureFailPoint": "failCommand",
        "mode": {"times": 1},
        "data": {
            "failCommands": ["insert"],
            "errorCode": 10107,  # NotMaster
            "closeConnection": network_error,
        }
    }
    _fail_point(client, config)


def fail_find(client, network_error=False):
    config = {
        "configureFailPoint": "failCommand",
        "mode": {"times": 1},
        "data": {
            "failCommands": ["find"],
            "errorCode": 10107,  # NotMaster
            "closeConnection": network_error,
        }
    }
    _fail_point(client, config)


def repl_set_step_down(client, **kwargs):
    """Run replSetStepDown, first unfreezing a secondary with replSetFreeze."""
    cmd = SON([('replSetStepDown', 1)])
    cmd.update(kwargs)

    # Unfreeze a secondary to ensure a speedy election.
    client.admin.command(
        'replSetFreeze', 0, read_preference=ReadPreference.SECONDARY)
    client.admin.command(cmd)


def cause_failover_after(seconds):
    with get_client() as client:
        time.sleep(seconds)
        logging.info('Causing failover via replSetStepDown\n')
        repl_set_step_down(client, replSetStepDown=10)
        # logging.info('Causing failover via {shutdown:1, force:1}\n')
        # client.admin.command('shutdown', 1, force=1)


def get_client(**kwargs):
    """Return a client connected to the whole replica set"""
    if URI:
        return MongoClient(URI, retryWrites=True, **kwargs)
    # Discover the replica set name.
    with MongoClient() as client:
        doc = client.admin.command('isMaster')
        name = doc['setName']
        hosts = doc['hosts']
    return MongoClient(hosts, replicaSet=name, retryWrites=True, **kwargs)


def insert_continually(client):
    start = time.time()
    while time.time() - start < 8:
        insert(client)
        time.sleep(0.005)


def main():
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    listeners = [ServerLogger(), HeartbeatLogger(), TopologyLogger()]
    client = get_client(event_listeners=listeners)
    client.admin.command('isMaster')
    logging.info('Primary: %s', client.primary)

    insert(client)
    # Cause a network error
    logging.info('Causing a network error for insert with failCommand\n')
    fail_insert(client, network_error=True)
    insert(client)
    # Cause a NotMaster error
    logging.info('Causing a NotMaster error for insert with failCommand\n')
    fail_insert(client, network_error=False)
    insert(client)

    # Same for find
    # Cause a network error
    logging.info('Causing a network error for find with failCommand\n')
    fail_find(client, network_error=True)
    find(client)
    # Cause a NotMaster error
    logging.info('Causing a NotMaster error for find with failCommand\n')
    fail_find(client, network_error=False)
    find(client)

    print()
    logging.info('Testing a real server event\n')

    # Run retryable writes for 10 seconds.
    threads = [threading.Thread(target=cause_failover_after, args=(2,))]
    threads.extend(threading.Thread(target=insert_continually, args=(client,)) for _ in range(4))
    for t in threads:
        t.start()
    # Wait
    for t in threads:
        t.join()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        URI = sys.argv[1]
    main()

# from gridfs import GridIn
# with client.start_session() as s, s.start_transaction():
#     with GridIn(root_collection=client.db["fs.files"], session=s) as gin:
#         gin.write(b'my data')
