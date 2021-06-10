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

"""Test the Load Balancer unified spec tests."""

import os
import socket
import sys

sys.path[0:0] = [""]

from test import unittest, IntegrationTest, client_context
from test.utils import get_pool
from test.unified_format import generate_test_classes

# Location of JSON test specifications.
TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'unified')

# Generate unified tests.
globals().update(generate_test_classes(TEST_PATH, module=__name__))


class TestLB(IntegrationTest):
    @client_context.require_load_balancer
    def test_unpin_committed_transaction(self):
        pool = get_pool(self.client)
        with self.client.start_session() as session:
            with session.start_transaction():
                self.assertEqual(pool.active_sockets, 0)
                self.db.test.insert_one({}, session=session)
                self.assertEqual(pool.active_sockets, 1)  # Pinned.
            self.assertEqual(pool.active_sockets, 1)  # Still pinned.
        self.assertEqual(pool.active_sockets, 0)  # Unpinned.

    def test_client_can_be_reopened(self):
        self.client.close()
        self.db.test.find_one({})

    def test_watch_leak(self):
        import gc
        self.db.test.insert_many([{} for _ in range(20)])
        args = {
            "mode": {
              "times": 1
            },
            "data": {
              "failCommands": [
                "find"
              ],
              "errorCode": 91,
              "closeConnection": True,
            }
        }
        from test.utils import single_client
        from pymongo.event_loggers import CommandLogger, ConnectionPoolLogger
        import logging
        # Enable logs in this format:
        # 2020-06-08 23:49:35,982 DEBUG ocsp_support Peer did not staple an
        # OCSP response
        FORMAT = '%(asctime)s %(levelname)s %(module)s %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.DEBUG)
        client = single_client(
            event_listeners=[ConnectionPoolLogger()]
        )
        with self.fail_point(args):
            print('Running Find')
            # cursor = self.db.watch()
            cursor = client.pymongo_test.test.find(batch_size=5)
            gc.collect()
            try:
                next(cursor)
            except:
                raise
        # print(f'sock: {cursor._cursor._CommandCursor__sock_mgr.sock}')
        import weakref
        # sock = weakref.ref(cursor._Cursor__sock_mgr.sock.sock)
        # sock = cursor._Cursor__sock_mgr.sock
        print(f'sock: {cursor._Cursor__sock_mgr and cursor._Cursor__sock_mgr.sock}')
        pool = get_pool(client)
        print(f'Pool before cursor del: {pool.sockets}')
        # breakpoint()
        # cursor.close()
        # cursor.__del__()
        gc.set_debug(gc.DEBUG_LEAK)
        del cursor
        print(f'Pool after cursor del: {pool.sockets}')
        gc.collect()
        print(f'Pool after gc.collect: {pool.sockets}')
        gc.set_debug(gc.DEBUG_UNCOLLECTABLE)
        print('hello')
        print(client.admin.command('hello')['ok'])
        print('Done')

    def test_exhaust_leak(self):
        self.db.test.insert_many([{} for _ in range(20)])
        args = {
            "mode": {
              "times": 1
            },
            "data": {
              "failCommands": [
                "find"
              ],
              "errorCode": 91
            }
        }
        from pymongo.cursor import CursorType
        with self.fail_point(args):
            print('Running Find')
            # cursor = self.db.watch()
            cursor = self.db.test.find(batch_size=5, cursor_type=CursorType.EXHAUST)
            next(cursor)
        # print(f'sock: {cursor._cursor._CommandCursor__sock_mgr.sock}')
        print(f'sock: {cursor._Cursor__sock_mgr and cursor._Cursor__sock_mgr.sock}')
        pool = get_pool(self.client)
        print(f'Pool before cursor del: {pool.sockets}')
        # breakpoint()
        del cursor
        print(f'Pool after cursor del: {pool.sockets}')
        import gc
        gc.collect()
        print(f'Pool after gc.collect: {pool.sockets}')
        print('hello')
        print(self.client.admin.command('hello')['ok'])
        print('Done')

if __name__ == "__main__":
    unittest.main()
