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

"""Test experimental asyncio support."""

import sys
import unittest

sys.path[0:0] = [""]

from pymongo.synchro import Synchronizer
from test import IntegrationTest


async def amethod(*args):
    print('Running async code: amethod(%s)' % (args,))
    return 'result'

async def araise():
    raise TypeError('Raising TypeError from coroutine')


class TestAsyncio(IntegrationTest):
    def test_sync_calls_async(self):
        s = Synchronizer()
        self.addCleanup(s.stop)
        s.call_soon(print, 'Runner: Running sync callback 1')
        result = s.run_coroutine(amethod('arg1', 'arg2'))
        self.assertEqual(result, 'result')
        s.call_soon(print, 'Runner: Running sync callback 2')
        with self.assertRaisesRegex(TypeError,
                                    'Raising TypeError from coroutine'):
            s.run_coroutine(araise())
        s.stop()


if __name__ == "__main__":
    unittest.main()
