# Copyright 2009-present MongoDB, Inc.
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

"""Test the database module."""

import sys
import time

sys.path[0:0] = [""]

from pymongo import monitoring

from test import (client_context,
                  IntegrationTest,
                  unittest)
from test.utils import (HeartbeatEventListener,
                        rs_or_single_client,
                        ServerEventListener,
                        wait_until)


class TestStreamingProtocol(IntegrationTest):
    @client_context.require_failCommand_appName
    def test_failCommand_streaming(self):
        listener = ServerEventListener()
        hb_listener = HeartbeatEventListener()
        client = rs_or_single_client(
            event_listeners=[listener, hb_listener], heartbeatFrequencyMS=500,
            appName='failingIsMasterTest')
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command('ping')
        address = client.address
        listener.reset()

        fail_ismaster = {
            'configureFailPoint': 'failCommand',
            'mode': {'times': 4},
            'data': {
                'failCommands': ['isMaster'],
                'closeConnection': False,
                'errorCode': 10107,
                'appName': 'failingIsMasterTest',
            },
        }
        with self.fail_point(fail_ismaster):
            def _marked_unknown(event):
                return (event.server_address == address
                        and not event.new_description.is_server_type_known)

            def _discovered_node(event):
                return (event.server_address == address
                        and not event.previous_description.is_server_type_known
                        and event.new_description.is_server_type_known)

            def marked_unknown():
                return len(listener.matching(_marked_unknown)) >= 1

            def rediscovered():
                return len(listener.matching(_discovered_node)) >= 1

            # Topology events are published asynchronously
            wait_until(marked_unknown, 'mark node unknown')
            wait_until(rediscovered, 'rediscover node')

        # Server should be selectable.
        client.admin.command('ping')

    @client_context.require_failCommand_appName
    def test_streaming_rtt(self):
        listener = ServerEventListener()
        hb_listener = HeartbeatEventListener()
        client = rs_or_single_client(
            event_listeners=[listener, hb_listener], heartbeatFrequencyMS=500,
            appName='streamingRttTest')
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command('ping')
        address = client.address

        # Sleep long enough for multiple heartbeats to succeed.
        time.sleep(2)

        def changed_event(event):
            return (event.server_address == address and
                    isinstance(event, monitoring.ServerDescriptionChangedEvent))

        events = listener.matching(changed_event)
        for event in events:
            self.assertTrue(event.new_description.round_trip_time)

        delay_ismaster = {
            'configureFailPoint': 'failCommand',
            'mode': {'times': 1000},
            'data': {
                'failCommands': ['isMaster'],
                'blockConnection': True,
                'blockTimeMS': 500,
                'appName': 'streamingRttTest',
            },
        }
        with self.fail_point(delay_ismaster):
            def rtt_exceeds_250_ms():
                topology = client._topology
                sd = topology.description.server_descriptions()[address]
                return sd.round_trip_time > 0.250
                # TODO: Foiled by SD equality yet again... update prose test.
                # event = listener.matching(changed_event)[-1]
                # return event.new_description.round_trip_time > 0.250

            wait_until(rtt_exceeds_250_ms, 'exceed 250ms RTT')

        # Server should be selectable.
        client.admin.command('ping')

    @client_context.require_failCommand_appName
    def test_monitor_waits_after_server_check_error(self):
        hb_listener = HeartbeatEventListener()
        client = rs_or_single_client(
            event_listeners=[hb_listener], heartbeatFrequencyMS=500,
            appName='waitAfterErrorTest')
        self.addCleanup(client.close)
        # Force a connection.
        client.admin.command('ping')
        address = client.address

        fail_ismaster = {
            'mode': {'times': 50},
            'data': {
                'failCommands': ['isMaster'],
                'closeConnection': False,
                'errorCode': 91,
                'appName': 'waitAfterErrorTest',
            },
        }
        with self.fail_point(fail_ismaster):
            time.sleep(2)

        # Server should be selectable.
        client.admin.command('ping')

        def hb_started(event):
            return (isinstance(event, monitoring.ServerHeartbeatStartedEvent)
                    and event.connection_id == address)

        hb_started_events = hb_listener.matching(hb_started)
        # Time: event
        # 0ms: create MongoClient
        # 1ms: run monitor handshake, 1
        # 2ms: run awaitable isMaster, 2
        # 3ms: run configureFailPoint
        # 502ms: isMaster fails for the first time with command error
        # 1002ms: run monitor handshake, 3
        # 1502ms: run monitor handshake, 4
        # 2002ms: run monitor handshake, 5
        # 2003ms: disable configureFailPoint
        # 2004ms: isMaster succeeds, 6
        # 2004ms: awaitable isMaster, 7
        self.assertGreater(len(hb_started_events), 10)
        self.assertLess(len(hb_started_events), 15)


if __name__ == "__main__":
    unittest.main()
