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
from __future__ import annotations

import unittest
from test import PyMongoTestCase

import pytest

try:
    from mockupdb import MockupDB

    _HAVE_MOCKUPDB = True
except ImportError:
    _HAVE_MOCKUPDB = False

from pymongo import common
from pymongo.errors import ServerSelectionTimeoutError

pytestmark = pytest.mark.mockupdb


class TestAuthRecoveringMember(PyMongoTestCase):
    def test_auth_recovering_member(self):
        # Test that we don't attempt auth against a recovering RS member.
        server = MockupDB()
        server.autoresponds(
            "ismaster",
            {
                "minWireVersion": 2,
                "maxWireVersion": common.MIN_SUPPORTED_WIRE_VERSION,
                "ismaster": False,
                "secondary": False,
                "setName": "rs",
            },
        )

        server.run()
        self.addCleanup(server.stop)

        client = self.simple_client(
            server.uri, replicaSet="rs", serverSelectionTimeoutMS=100, socketTimeoutMS=100
        )

        # Should see there's no primary or secondary and raise selection timeout
        # error. If it raises AutoReconnect we know it actually tried the
        # server, and that's wrong.
        with self.assertRaises(ServerSelectionTimeoutError):
            client.db.command("ping")


if __name__ == "__main__":
    unittest.main()
