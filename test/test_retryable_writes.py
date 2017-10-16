# Copyright 2015 MongoDB, Inc.
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

"""Test retryable writes."""

import json
import os
import re
import sys

sys.path[0:0] = [""]

from bson import SON
from bson.py3compat import iteritems
from pymongo import operations
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.errors import ConnectionFailure
from pymongo.results import _WriteResult, BulkWriteResult

from pymongo.monitoring import _SENSITIVE_COMMANDS

from test import unittest, client_context, IntegrationTest
from test.utils import rs_or_single_client, EventListener
from test.test_crud import run_operation

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'retryable_writes')


class CommandListener(EventListener):
    def started(self, event):
        if event.command_name.lower() in _SENSITIVE_COMMANDS:
            return
        super(CommandListener, self).started(event)

    def succeeded(self, event):
        if event.command_name.lower() in _SENSITIVE_COMMANDS:
            return
        super(CommandListener, self).succeeded(event)

    def failed(self, event):
        if event.command_name.lower() in _SENSITIVE_COMMANDS:
            return
        super(CommandListener, self).failed(event)


class TestAllScenarios(IntegrationTest):

    @classmethod
    @client_context.require_version_min(3, 5)
    def setUpClass(cls):
        super(TestAllScenarios, cls).setUpClass()
        cls.client = rs_or_single_client(retryWrites=True)
        cls.db = cls.client.pymongo_test

    def tearDown(self):
        self.client.admin.command(SON([
            ("configureFailPoint", "onPrimaryTransactionalWrite"),
            ("mode", "off")]))

    def set_fail_point(self, mode):
        self.client.admin.command(SON([
            ("configureFailPoint", "onPrimaryTransactionalWrite"),
            ("mode", mode)]))


def create_test(scenario_def, test):
    def run_scenario(self):
        # Load data.
        assert scenario_def['data'], "tests must have non-empty data"
        self.db.test.drop()
        self.db.test.insert_many(scenario_def['data'])

        # Set the failPoint
        self.set_fail_point(test['failPoint'])

        result = run_operation(self.db.test, test)

        # Assert final state is expected.
        expected_c = test['outcome'].get('collection')
        if expected_c is not None:
            expected_name = expected_c.get('name')
            if expected_name is not None:
                db_coll = self.db[expected_name]
            else:
                db_coll = self.db.test
            self.assertEqual(list(db_coll.find()), expected_c['data'])
        expected_result = test['outcome'].get('result')
        if expected_result is not None:
            self.assertTrue(result, expected_result)

    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        dirname = os.path.split(dirpath)[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)

            test_type = os.path.splitext(filename)[0]

            # Construct test from scenario.
            for test in scenario_def['tests']:
                new_test = create_test(scenario_def, test)
                test_name = 'test_%s_%s_%s' % (
                    dirname,
                    test_type,
                    str(test['description'].replace(" ", "_")))

                new_test.__name__ = test_name
                setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()


class TestRetryableWrites(IntegrationTest):

    @classmethod
    @client_context.require_version_min(3, 5)
    def setUpClass(cls):
        super(TestRetryableWrites, cls).setUpClass()
        cls.listener = CommandListener()
        cls.client = rs_or_single_client(
            retryWrites=True, event_listeners=[cls.listener])
        cls.db = cls.client.pymongo_test

    def setUp(self):
        self.client.admin.command(SON([
            ("configureFailPoint", "onPrimaryTransactionalWrite"),
            ("mode", "alwaysOn")]))

    def tearDown(self):
        self.client.admin.command(SON([
            ("configureFailPoint", "onPrimaryTransactionalWrite"),
            ("mode", "off")]))

    def retryable_single_statement_ops(self, coll):
        return [
            (coll.insert_one, [{}], {}),
            (coll.replace_one, [{}, {}], {}),
            (coll.update_one, [{}, {'$set': {'a': 1}}], {}),
            (coll.delete_one, [{}], {}),
            (coll.insert_one, [{}], {}),  # Insert another document.
            (coll.find_one_and_replace, [{}, {'a': 3}], {}),
            (coll.find_one_and_update, [{}, {'$set': {'a': 1}}], {}),
            (coll.find_one_and_delete, [{}, {}], {})]

    def test_supported_single_statement_no_retry(self):
        listener = CommandListener()
        client = rs_or_single_client(
            retryWrites=False, event_listeners=[listener])
        for method, args, kwargs in self.retryable_single_statement_ops(
                client.db.retryable_write_test):
            listener.results.clear()
            method(*args, **kwargs)
            for event in listener.results['started']:
                self.assertFalse('txnNumber' in event.command,
                                 "%s sent txnNumber with %s" % (
                                     method.__name__, event.command_name))

    def test_supported_single_statement(self):
        for method, args, kwargs in self.retryable_single_statement_ops(
                self.db.retryable_write_test):
            self.listener.results.clear()
            method(*args, **kwargs)
            commands_started = self.listener.results['started']
            self.assertEqual(len(self.listener.results['failed']), 1,
                             method.__name__)
            self.assertEqual(len(self.listener.results['succeeded']), 1,
                             method.__name__)
            self.assertEqual(len(commands_started), 2, method.__name__)
            first_attempt = commands_started[0]
            self.assertTrue(
                'lsid' in first_attempt.command,
                "%s sent no lsid with %s" % (
                    method.__name__, first_attempt.command_name))
            initial_session_id = first_attempt.command['lsid']
            self.assertTrue(
                'txnNumber' in first_attempt.command,
                "%s sent no txnNumber with %s" % (
                    method.__name__, first_attempt.command_name))
            initial_transaction_id = first_attempt.command['txnNumber']
            retry_attempt = commands_started[1]
            self.assertTrue(
                'lsid' in retry_attempt.command,
                "%s sent no lsid with %s" % (
                    method.__name__, first_attempt.command_name))
            self.assertEqual(retry_attempt.command['lsid'], initial_session_id)
            self.assertTrue(
                'txnNumber' in retry_attempt.command,
                "%s sent no txnNumber with %s" % (
                    method.__name__, first_attempt.command_name))
            self.assertEqual(retry_attempt.command['txnNumber'],
                             initial_transaction_id)

    def test_unsupported_single_statement(self):
        coll = self.db.retryable_write_test
        coll.insert_many([{}, {}])
        for method, args, kwargs in [
                (coll.update_many, [{}, {'$set': {'a': 1}}], {}),
                (coll.delete_many, [{}], {})]:
            self.listener.results.clear()
            method(*args, **kwargs)
            started_events = self.listener.results['started']
            self.assertEqual(len(self.listener.results['succeeded']), 1,
                             method.__name__)
            self.assertEqual(len(started_events), 1, method.__name__)
            event = started_events[0]
            self.assertTrue(
                'lsid' in event.command,
                "%s sent no lsid with %s" % (
                    method.__name__, event.command_name))
            self.assertFalse(
                'txnNumber' in event.command,
                "%s sent txnNumber with %s" % (
                    method.__name__, event.command_name))


if __name__ == "__main__":
    unittest.main()
