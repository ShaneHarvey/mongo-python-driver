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

from pymongo.errors import InvalidOperation, OperationFailure
from test import client_context, unittest, IntegrationTest


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

    def test_constructor(self):
        pass

    def test_iteration(self):
        with self.coll.watch() as change_stream:
            inserted_doc = {'_id': 1, 'a': 1}
            self.coll.insert_one(inserted_doc)
            change = next(change_stream)
            self.assertEqual(change['operationType'], 'insert')
            self.assertEqual(change['ns'], {'db': self.coll.database.name,
                                            'coll': self.coll.name})
            self.assertEqual(change['fullDocument'], inserted_doc)

    def test_update_resume_token(self):
        with self.coll.watch() as change_stream:
            self.assertIsNone(change_stream._resume_token)
            for i in range(10):
                self.coll.insert_one({})
                change = next(change_stream)
                self.assertEqual(change['_id'], change_stream._resume_token)

    def test_raises_error_on_missing_id(self):
        with self.coll.watch([{'$project': {'_id': 0}}]) as change_stream:
            self.coll.insert_one({})
            with self.assertRaises(InvalidOperation):
                next(change_stream)

    def test_resume_on_error(self):
        with self.coll.watch([]) as change_stream:
            inserted_doc = {'_id': 1}
            self.coll.insert_one(inserted_doc)
            change = next(change_stream)
            self.assertEqual(change['operationType'], 'insert')
            self.assertEqual(change['ns'], {'db': self.coll.database.name,
                                            'coll': self.coll.name})
            self.assertEqual(change['fullDocument'], inserted_doc)
            inserted_doc = {'_id': 2}
            self.coll.insert_one(inserted_doc)
            # Cause a cursor not found error on the next getMore.
            change_stream._cursor.close()
            change = next(change_stream)
            self.assertEqual(change['operationType'], 'insert')
            self.assertEqual(change['ns'], {'db': self.coll.database.name,
                                            'coll': self.coll.name})
            self.assertEqual(change['fullDocument'], inserted_doc)

    def test_does_not_resume_on_server_error(self):
        def mock_server_error():
            raise OperationFailure('Mock server error')

        with self.coll.watch() as change_stream:
            change_stream._cursor.next = mock_server_error
            with self.assertRaises(OperationFailure):
                next(change_stream)
            with self.assertRaises(StopIteration):
                next(change_stream)


if __name__ == '__main__':
    unittest.main()
