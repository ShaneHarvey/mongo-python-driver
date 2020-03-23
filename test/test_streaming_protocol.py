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

sys.path[0:0] = [""]

from test import (client_context,
                  IntegrationTest,
                  unittest)
from test.utils import (HeartbeatEventListener,
                        rs_or_single_client,
                        ServerEventListener,
                        wait_until)


class TestStreamingProtocol(IntegrationTest):
    @client_context.require_test_commands
    def test_failCommand_streaming(self):
        listener = ServerEventListener()
        hb_listener = HeartbeatEventListener()
        client = rs_or_single_client(
            event_listeners=[listener, hb_listener], heartbeatFrequencyMS=500)
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


if __name__ == "__main__":
    unittest.main()
