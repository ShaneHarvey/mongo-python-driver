# Copyright 2017 MongoDB, Inc.
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

"""Test the change_stream module."""

import sys

sys.path[0:0] = ['']

from pymongo.command_cursor import CommandCursor
from pymongo.errors import (InvalidOperation, OperationFailure,
                            ServerSelectionTimeoutError)

from test import client_context, unittest, IntegrationTest
from test.utils import WhiteListEventListener, rs_or_single_client


class TestChangeStream(IntegrationTest):

    @classmethod
    @client_context.require_version_min(3, 5, 11)
    @client_context.require_replica_set
    def setUpClass(cls):
        super(TestChangeStream, cls).setUpClass()
        cls.coll = cls.db.change_stream_test

    def setUp(self):
        # Use a new collection for each test.
        self.coll = self.db[self.id()]

    def tearDown(self):
        self.coll.drop()

    def insert_and_check(self, change_stream, doc):
        self.coll.insert_one(doc)
        change = next(change_stream)
        self.assertEqual(change['operationType'], 'insert')
        self.assertEqual(change['ns'], {'db': self.coll.database.name,
                                        'coll': self.coll.name})
        self.assertEqual(change['fullDocument'], doc)

    def test_constructor(self):
        with self.coll.watch(
                [{'$project': {'foo': 0}}], full_document='updateLookup',
                max_await_time_ms=1000, batch_size=100) as change_stream:
            self.assertIs(self.coll, change_stream._collection)
            self.assertEqual([{'$project': {'foo': 0}}],
                             change_stream._pipeline)
            self.assertEqual('updateLookup', change_stream._full_document)
            self.assertIsNone(change_stream._resume_token)
            self.assertEqual(1000, change_stream._max_await_time_ms)
            self.assertEqual(100, change_stream._batch_size)
            self.assertIsInstance(change_stream._cursor, CommandCursor)
            self.assertEqual(
                1000, change_stream._cursor._CommandCursor__max_await_time_ms)
            self.assertFalse(change_stream._killed)
            self.assertTrue(change_stream.alive)

    def test_full_pipeline(self):
        """$changeStream must be the first stage in a change stream pipeline
        sent to the server.
        """
        listener = WhiteListEventListener("aggregate")
        results = listener.results
        client = rs_or_single_client(event_listeners=[listener])
        self.addCleanup(client.close)
        coll = client[self.db.name][self.coll.name]

        with coll.watch([{'$project': {'foo': 0}}]) as change_stream:
            self.assertEqual([{'$changeStream': {}}, {'$project': {'foo': 0}}],
                             change_stream.full_pipeline)

        self.assertEqual(1, len(results['started']))
        command = results['started'][0]
        self.assertEqual('aggregate', command.command_name)
        self.assertEqual([{'$changeStream': {}}, {'$project': {'foo': 0}}],
                         command.command['pipeline'])

    def test_iteration(self):
        with self.coll.watch(max_await_time_ms=10) as change_stream:
            self.assertTrue(change_stream.alive)
            with self.assertRaises(StopIteration):
                change_stream.next()
            for i in range(5):
                self.insert_and_check(change_stream, {'_id': i, 'a': i})
            self.assertTrue(change_stream.alive)
            with self.assertRaises(StopIteration):
                change_stream.next()
            self.assertTrue(change_stream.alive)
            self.insert_and_check(change_stream, {'_id': 6, 'a': 6})

    def test_update_resume_token(self):
        """ChangeStream must continuously track the last seen resumeToken."""
        with self.coll.watch() as change_stream:
            self.assertIsNone(change_stream._resume_token)
            for i in range(10):
                self.coll.insert_one({})
                change = next(change_stream)
                self.assertEqual(change['_id'], change_stream._resume_token)

    def test_raises_error_on_missing_id(self):
        """ChangeStream will raise an exception if the server response is
        missing the resume token.
        """
        with self.coll.watch([{'$project': {'_id': 0}}]) as change_stream:
            self.coll.insert_one({})
            with self.assertRaises(InvalidOperation):
                next(change_stream)

    def test_resume_on_error(self):
        """ChangeStream will automatically resume one time on a resumable
        error (including not master) with the initial pipeline and options,
        except for the addition/update of a resumeToken.
        """
        with self.coll.watch([]) as change_stream:
            self.insert_and_check(change_stream, {'_id': 1})
            # Cause a cursor not found error on the next getMore.
            cursor = change_stream._cursor
            self.client._close_cursor_now(cursor.cursor_id, cursor.address)
            self.insert_and_check(change_stream, {'_id': 2})

    def test_does_not_resume_on_server_error(self):
        """ChangeStream will not attempt to resume on a server error."""
        def mock_server_error():
            raise OperationFailure('Mock server error')

        with self.coll.watch() as change_stream:
            change_stream._cursor.next = mock_server_error
            with self.assertRaises(OperationFailure):
                next(change_stream)
            with self.assertRaises(StopIteration):
                next(change_stream)

    def test_resume_server_selection_read_preference(self):
        """ChangeStream will perform server selection before attempting to
        resume, using initial readPreference
        """
        # TODO: ChangeStream uses the original collection whose read
        # preference is immutable. I don't think this test is necessary.
        pass

    def test_initial_empty_batch(self):
        """Ensure that a cursor returned from an aggregate command with a
        cursor id, and an initial empty batch, is not closed on the driver
        side.
        """
        with self.coll.watch() as change_stream:
            # The first batch should be empty.
            self.assertEqual(0, change_stream._cursor.retrieved)
            cursor_id = change_stream._cursor.cursor_id
            self.assertTrue(cursor_id)
            self.insert_and_check(change_stream, {})
            # Make sure we're still using the same cursor.
            self.assertEqual(cursor_id, change_stream._cursor.cursor_id)

    def test_kill_cursors(self):
        """The killCursors command sent during the resume process must not be
        allowed to raise an exception.
        """
        def raise_error():
            raise ServerSelectionTimeoutError('mock error')
        with self.coll.watch([]) as change_stream:
            self.insert_and_check(change_stream, {'_id': 1})
            # Cause a cursor not found error on the next getMore.
            cursor = change_stream._cursor
            self.client._close_cursor_now(cursor.cursor_id, cursor.address)
            cursor.close = raise_error
            self.insert_and_check(change_stream, {'_id': 2})

    def test_unknown_full_document(self):
        """Must rely on the server to raise an error on unknown fullDocument.
        """
        try:
            with self.coll.watch(full_document='notValidatedByPyMongo'):
                pass
        except OperationFailure:
            pass


if __name__ == '__main__':
    unittest.main()
