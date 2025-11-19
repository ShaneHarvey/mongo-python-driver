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
from __future__ import annotations

import asyncio
import os
import random
import threading
from pathlib import Path
from string import ascii_lowercase
from test import IntegrationTest, client_context, unittest
from test.helpers import ConcurrentRunner
from test.utils import flaky
from test.utils_selection_tests import create_topology
from test.utils_shared import (
    CMAPListener,
    OvertCommandListener,
    delay,
    wait_until,
)
from test.utils_spec_runner import SpecTestCreator

from pymongo.common import clean_node
from pymongo.monitoring import ConnectionReadyEvent
from pymongo.operations import _Op
from pymongo.read_preferences import ReadPreference

_IS_SYNC = True
# Location of JSON test specifications.
if _IS_SYNC:
    TEST_PATH = os.path.join(Path(__file__).resolve().parent, "server_selection", "in_window")
else:
    TEST_PATH = os.path.join(
        Path(__file__).resolve().parent.parent, "server_selection", "in_window"
    )


class TestAllScenarios(unittest.TestCase):
    def run_scenario(self, scenario_def):
        topology = create_topology(scenario_def)

        # Update mock operation_count state:
        for mock in scenario_def["mocked_topology_state"]:
            address = clean_node(mock["address"])
            server = topology.get_server_by_address(address)
            server.pool.operation_count = mock["operation_count"]

        pref = ReadPreference.NEAREST
        counts = {address: 0 for address in topology._description.server_descriptions()}

        # Number of times to repeat server selection
        iterations = scenario_def["iterations"]
        for _ in range(iterations):
            server = topology.select_server(pref, _Op.TEST, server_selection_timeout=0)
            counts[server.description.address] += 1

        # Verify expected_frequencies
        outcome = scenario_def["outcome"]
        tolerance = outcome["tolerance"]
        expected_frequencies = outcome["expected_frequencies"]
        for host_str, freq in expected_frequencies.items():
            address = clean_node(host_str)
            actual_freq = float(counts[address]) / iterations
            if freq == 0:
                # Should be exactly 0.
                self.assertEqual(actual_freq, 0)
            else:
                # Should be within 'tolerance'.
                self.assertAlmostEqual(actual_freq, freq, delta=tolerance)


def create_test(scenario_def, test, name):
    def run_scenario(self):
        self.run_scenario(scenario_def)

    return run_scenario


class CustomSpecTestCreator(SpecTestCreator):
    def tests(self, scenario_def):
        """Extract the tests from a spec file.

        Server selection in_window tests do not have a 'tests' field.
        The whole file represents a single test case.
        """
        return [scenario_def]


CustomSpecTestCreator(create_test, TestAllScenarios, TEST_PATH).create_tests()


class FinderTask(ConcurrentRunner):
    def __init__(self, collection, iterations):
        super().__init__()
        self.daemon = True
        self.collection = collection
        self.iterations = iterations
        self.passed = False
        self.n_overload_errors = 0

    def run(self):
        from pymongo.errors import PyMongoError

        for _ in range(self.iterations):
            try:
                self.collection.find_one({"$where": delay(0.025)})
            except PyMongoError as exc:
                if not exc.has_error_label("SystemOverloadedError"):
                    raise
                self.n_overload_errors += 1
        self.passed = True


class TestProse(IntegrationTest):
    def frequencies(self, client, listener, n_finds=10):
        coll = client.test.test
        N_TASKS = 20
        tasks = [FinderTask(coll, n_finds) for _ in range(N_TASKS)]
        for task in tasks:
            task.start()
        for task in tasks:
            task.join()
        for task in tasks:
            self.assertTrue(task.passed)

        events = listener.started_events
        self.assertGreaterEqual(len(events), n_finds * N_TASKS)
        nodes = client.nodes
        self.assertEqual(len(nodes), 2)
        freqs = {address: 0.0 for address in nodes}
        for event in events:
            freqs[event.connection_id] += 1
        for address in freqs:
            freqs[address] = freqs[address] / float(len(events))
        freqs["overload_errors"] = sum(task.n_overload_errors for task in tasks)
        freqs["operations"] = sum(task.iterations for task in tasks)
        freqs["error_rate"] = freqs["overload_errors"] / float(freqs["operations"])
        return freqs

    @client_context.require_failCommand_appName
    @client_context.require_multiple_mongoses
    @flaky(reason="PYTHON-3689")
    def test_load_balancing(self):
        listener = OvertCommandListener()
        cmap_listener = CMAPListener()
        # PYTHON-2584: Use a large localThresholdMS to avoid the impact of
        # varying RTTs.
        client = self.rs_client(
            client_context.mongos_seeds(),
            appName="loadBalancingTest",
            event_listeners=[listener, cmap_listener],
            localThresholdMS=30000,
            minPoolSize=10,
        )
        wait_until(lambda: len(client.nodes) == 2, "discover both nodes")
        # Wait for both pools to be populated.
        cmap_listener.wait_for_event(ConnectionReadyEvent, 20)
        # Delay find commands on only one mongos.
        delay_finds = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 10000},
            "data": {
                "failCommands": ["find"],
                "blockConnection": True,
                "blockTimeMS": 500,
                "appName": "loadBalancingTest",
            },
        }
        with self.fail_point(delay_finds):
            nodes = client_context.client.nodes
            self.assertEqual(len(nodes), 1)
            delayed_server = next(iter(nodes))
            freqs = self.frequencies(client, listener)
            self.assertLessEqual(freqs[delayed_server], 0.25)
        listener.reset()
        freqs = self.frequencies(client, listener, n_finds=150)
        self.assertAlmostEqual(freqs[delayed_server], 0.50, delta=0.15)

    @client_context.require_failCommand_appName
    @client_context.require_multiple_mongoses
    def test_load_balancing_overload(self):
        listener = OvertCommandListener()
        cmap_listener = CMAPListener()
        # PYTHON-2584: Use a large localThresholdMS to avoid the impact of
        # varying RTTs.
        client = self.rs_client(
            client_context.mongos_seeds(),
            appName="loadBalancingTest",
            event_listeners=[listener, cmap_listener],
            localThresholdMS=30000,
            minPoolSize=10,
        )
        wait_until(lambda: len(client.nodes) == 2, "discover both nodes")
        # Wait for both pools to be populated.
        cmap_listener.wait_for_event(ConnectionReadyEvent, 20)
        # enable rate limiter
        client.test.test.insert_many(
            [{"str": "".join(random.choices(ascii_lowercase, k=512))} for _ in range(10)]
        )
        listener.reset()

        # Mock rate limiter errors on only one mongos.
        rejection_rate = 0.75
        delay_finds = {
            "configureFailPoint": "failCommand",
            "mode": {"activationProbability": rejection_rate},
            "data": {
                "failCommands": ["find"],
                "errorCode": 462,
                # Intentionally omit "RetryableError" to avoid retry behavior from impacting this test.
                "errorLabels": ["SystemOverloadedError"],
                "appName": "loadBalancingTest",
            },
        }
        nodes = client_context.client.nodes
        self.assertEqual(len(nodes), 1)
        delayed_server = next(iter(nodes))
        listener.reset()
        with self.fail_point(delay_finds):
            freqs = self.frequencies(client, listener, n_finds=200)
            print(f"\nOverloaded server: {delayed_server}")
            print(freqs)
            self.assertAlmostEqual(freqs[delayed_server], 1 - rejection_rate, delta=0.15)


if __name__ == "__main__":
    unittest.main()
