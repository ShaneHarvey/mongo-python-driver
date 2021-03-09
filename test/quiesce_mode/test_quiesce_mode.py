# Copyright 2021-present MongoDB, Inc.
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

"""Test Quiesce Mode."""

import sys
import threading
import time
import unittest

sys.path[0:0] = [""]

from pymongo.errors import AutoReconnect

from test import (client_context,
                  IntegrationTest)
from test.utils import rs_client


class TestQuiesceMode(IntegrationTest):
    @client_context.require_replica_set
    @client_context.require_version_min(4, 9)
    def test_cursor_survives_during_quiesce_mode(self):
        coll = self.db.test
        coll.insert_many([{} for _ in range(10)])
        # Create the cursor before shutdown.
        cursor = coll.find(batch_size=2, limit=10)
        docs = [cursor.next()]
        def shutdown():
            # Shutdown the primary and enter Quiesce Mode for 15 seconds.
            # Use a new client since on success the shutdown command will
            # fail with a network error.
            client = rs_client()
            with self.assertRaises(AutoReconnect):
                client.admin.command('shutdown', 1, timeoutSecs=15)

        thread = threading.Thread(target=shutdown)
        thread.start()
        time.sleep(.5)  # TODO: wait for Quiesce mode event.
        # Finish iterating the cursor after it enters Quiesce Mode.
        docs.extend(list(cursor))
        self.assertEqual(10, len(docs))
        # Assert the pool is not cleared.

        # Wait for the server to actually shutdown.
        thread.join()


if __name__ == '__main__':
    unittest.main()
