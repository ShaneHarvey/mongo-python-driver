# Copyright 2020-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the topology module's Server Selection Spec implementation."""

import os
import threading

from pymongo.common import clean_node, HEARTBEAT_FREQUENCY
from pymongo.read_preferences import ReadPreference
from pymongo.settings import TopologySettings
from pymongo.topology import Topology
from test import client_context, IntegrationTest, unittest
from test.utils_selection_tests import (
    get_addresses,
    get_topology_settings_dict,
    make_server_description)
from test.utils import TestCreator, rs_client, OvertCommandListener


# Location of JSON test specifications.
TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('server_selection', 'in_window'))

# Number of times to repeat server selection
REPEAT = 10000


class TestAllScenarios(unittest.TestCase):
    def run_scenario(self, scenario_def):
        # Initialize topologies.
        if 'heartbeatFrequencyMS' in scenario_def:
            frequency = int(scenario_def['heartbeatFrequencyMS']) / 1000.0
        else:
            frequency = HEARTBEAT_FREQUENCY

        seeds, hosts = get_addresses(
            scenario_def['topology_description']['servers'])

        settings = get_topology_settings_dict(
            heartbeat_frequency=frequency,
            seeds=seeds
        )
        topology = Topology(TopologySettings(**settings))
        topology.open()

        # Update topologies with server descriptions.
        for server in scenario_def['topology_description']['servers']:
            server_description = make_server_description(server, hosts)
            topology.on_change(server_description)

        # Update mock operation_count state:
        for in_window in scenario_def['in_window']:
            address = clean_node(in_window['address'])
            server = topology.get_server_by_address(address)
            server.pool.operation_count = in_window['operation_count']

        pref = ReadPreference.NEAREST
        counts = dict((address, 0) for address in
                      topology._description.server_descriptions())

        for _ in range(REPEAT):
            server = topology.select_server(pref, server_selection_timeout=0)
            counts[server.description.address] += 1

        # Verify expected_frequencies
        expected_frequencies = scenario_def['expected_frequencies']
        for host_str, freq in expected_frequencies.items():
            address = clean_node(host_str)
            actual_freq = float(counts[address])/REPEAT
            if freq == 0:
                # Should be exactly 0.
                self.assertEqual(actual_freq, 0)
            else:
                # Should be within ~5%
                self.assertAlmostEqual(actual_freq, freq, delta=0.02)


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def)

    return run_scenario


class CustomTestCreator(TestCreator):
    def tests(self, scenario_def):
        """Extract the tests from a spec file.

        Server selection in_window tests do not have a 'tests' field.
        The whole file represents a single test case.
        """
        return [scenario_def]


CustomTestCreator(create_test, TestAllScenarios, TEST_PATH).create_tests()


class FinderThread(threading.Thread):
    def __init__(self, collection, iterations):
        super(FinderThread, self).__init__()
        self.daemon = True
        self.collection = collection
        self.iterations = iterations
        self.passed = False

    def run(self):
        for _ in range(self.iterations):
            self.collection.find_one({})
        self.passed = True


class TestProse(IntegrationTest):
    def frequencies(self, client, listener):
        coll = client.test.test
        N_FINDS = 10
        N_THREADS = 10
        threads = [FinderThread(coll, N_FINDS) for _ in range(N_THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        for thread in threads:
            self.assertTrue(thread.passed)

        events = listener.results['started']
        self.assertEqual(len(events), N_FINDS * N_THREADS)
        nodes = client.nodes
        self.assertEqual(len(nodes), 2)
        freqs = {address: 0 for address in nodes}
        for event in events:
            freqs[event.connection_id] += 1
        for address in freqs:
            freqs[address] = freqs[address]/float(len(events))
        return freqs

    @client_context.require_failCommand_appName
    @client_context.require_multiple_mongoses
    def test_load_balancing(self):
        listener = OvertCommandListener()
        client = rs_client(client_context.mongos_seeds(),
                           appName='loadBalancingTest',
                           event_listeners=[listener])
        self.addCleanup(client.close)
        # Delay find commands on
        delay_finds = {
            'configureFailPoint': 'failCommand',
            'mode': {'times': 10000},
            'data': {
                'failCommands': ['find'],
                'blockConnection': True,
                'blockTimeMS': 500,
                'appName': 'loadBalancingTest',
            },
        }
        with self.fail_point(delay_finds):
            nodes = client_context.client.nodes
            self.assertEqual(len(nodes), 1)
            delayed_server = next(iter(nodes))
            freqs = self.frequencies(client, listener)
            self.assertLessEqual(freqs[delayed_server], 0.20)
        listener.reset()
        freqs = self.frequencies(client, listener)
        self.assertAlmostEqual(freqs[delayed_server], 0.50, delta=0.05)


if __name__ == "__main__":
    unittest.main()