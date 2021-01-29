import unittest

from bson.code import Code
from pymongo.mongo_client import MongoClient


class TestErrorCodes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        client = MongoClient(directConnection=False)
        client.test.test.insert_one({})

    def test_insert_one_on_secondary(self):
        client = MongoClient(port=27018, directConnection=True)
        self.assertFalse(client.admin.command('isMaster')['ismaster'])
        self.assertTrue(client.test.test.find_one())
        client.test.test.insert_one({})

    def test_insert_one_on_secondary_w0(self):
        client = MongoClient(port=27018, directConnection=True, w=0)
        self.assertFalse(client.admin.command('isMaster')['ismaster'])
        self.assertTrue(client.test.test.find_one())
        client.test.test.insert_one({})

    def test_insert_many_on_secondary(self):
        client = MongoClient(port=27018, directConnection=True)
        self.assertFalse(client.admin.command('isMaster')['ismaster'])
        self.assertTrue(client.test.test.find_one())
        client.test.test.insert_many([{} for _ in range(10000)])

    def test_insert_many_on_secondary_w0(self):
        client = MongoClient(port=27018, directConnection=True, w=0)
        self.assertFalse(client.admin.command('isMaster')['ismaster'])
        self.assertTrue(client.test.test.find_one())
        client.test.test.insert_many([{} for _ in range(10000)])

    def test_map_reduce(self):
        client = MongoClient()
        self.assertTrue(client.admin.command('isMaster')['ismaster'])
        db = client.test
        db.drop_collection("test")

        db.test.insert_one({"id": 1, "tags": ["dog", "cat"]})
        db.test.insert_one({"id": 2, "tags": ["cat"]})
        db.test.insert_one({"id": 3, "tags": ["mouse", "cat", "dog"]})
        db.test.insert_one({"id": 4, "tags": []})

        map = Code("function () {"
                   "  this.tags.forEach(function(z) {"
                   "    emit(z, 1);"
                   "  });"
                   "}")
        reduce = Code("function (key, values) {"
                      "  var total = 0;"
                      "  for (var i = 0; i < values.length; i++) {"
                      "    total += values[i];"
                      "  }"
                      "  return total;"
                      "}")
        result = db.test.map_reduce(map, reduce, out='mrunittests')
        self.assertEqual(3, result.find_one({"_id": "cat"})["value"])
        self.assertEqual(2, result.find_one({"_id": "dog"})["value"])
        self.assertEqual(1, result.find_one({"_id": "mouse"})["value"])

    def test_map_reduce_on_secondary(self):
        client = MongoClient(port=27018, directConnection=True)
        self.assertFalse(client.admin.command('isMaster')['ismaster'])
        db = client.test
        map = Code("function () {"
                   "  this.tags.forEach(function(z) {"
                   "    emit(z, 1);"
                   "  });"
                   "}")
        reduce = Code("function (key, values) {"
                      "  var total = 0;"
                      "  for (var i = 0; i < values.length; i++) {"
                      "    total += values[i];"
                      "  }"
                      "  return total;"
                      "}")
        result = db.test.map_reduce(map, reduce, out='mrunittests')
        self.assertEqual(3, result.find_one({"_id": "cat"})["value"])
        self.assertEqual(2, result.find_one({"_id": "dog"})["value"])
        self.assertEqual(1, result.find_one({"_id": "mouse"})["value"])


if __name__ == "__main__":
    unittest.main()
"""
On 2.6.12, 3.0.14, 3.2.22, and 3.4.24:
Traceback (most recent call last):
  File "/Users/shane/git/mongo-python-driver/research-DRIVERS-1152.py", line 52, in test_map_reduce_on_secondary
    result = db.test.map_reduce(map, reduce, out='mrunittests')
  File "/Users/shane/git/mongo-python-driver/pymongo/collection.py", line 2561, in map_reduce
    response = self._map_reduce(map, reduce, out, session,
  File "/Users/shane/git/mongo-python-driver/pymongo/collection.py", line 2496, in _map_reduce
    return self._command(
  File "/Users/shane/git/mongo-python-driver/pymongo/collection.py", line 231, in _command
    return sock_info.command(
  File "/Users/shane/git/mongo-python-driver/pymongo/pool.py", line 690, in command
    self._raise_connection_failure(error)
  File "/Users/shane/git/mongo-python-driver/pymongo/pool.py", line 674, in command
    return command(self, dbname, spec, slave_ok,
  File "/Users/shane/git/mongo-python-driver/pymongo/network.py", line 157, in command
    helpers._check_command_response(
  File "/Users/shane/git/mongo-python-driver/pymongo/helpers.py", line 154, in _check_command_response
    raise NotMasterError(errmsg, response)
pymongo.errors.NotMasterError: not master, full error: {'ok': 0.0, 'errmsg': 'not master'}

On 3.6.20, 4.0.20, and 4.2.9:
Traceback (most recent call last):
  File "/Users/shane/git/mongo-python-driver/research-DRIVERS-1152.py", line 52, in test_map_reduce_on_secondary
    result = db.test.map_reduce(map, reduce, out='mrunittests')
  File "/Users/shane/git/mongo-python-driver/pymongo/collection.py", line 2561, in map_reduce
    response = self._map_reduce(map, reduce, out, session,
  File "/Users/shane/git/mongo-python-driver/pymongo/collection.py", line 2496, in _map_reduce
    return self._command(
  File "/Users/shane/git/mongo-python-driver/pymongo/collection.py", line 231, in _command
    return sock_info.command(
  File "/Users/shane/git/mongo-python-driver/pymongo/pool.py", line 690, in command
    self._raise_connection_failure(error)
  File "/Users/shane/git/mongo-python-driver/pymongo/pool.py", line 674, in command
    return command(self, dbname, spec, slave_ok,
  File "/Users/shane/git/mongo-python-driver/pymongo/network.py", line 157, in command
    helpers._check_command_response(
  File "/Users/shane/git/mongo-python-driver/pymongo/helpers.py", line 152, in _check_command_response
    raise NotMasterError(errmsg, response)
pymongo.errors.NotMasterError: no longer primary while dropping temporary collection for mapReduce: test.tmp.mr.test_1, full error: {'operationTime': Timestamp(1611876422, 6), 'ok': 0.0, 'errmsg': 'no longer primary while dropping temporary collection for mapReduce: test.tmp.mr.test_1', 'code': 189, 'codeName': 'PrimarySteppedDown', '$clusterTime': {'clusterTime': Timestamp(1611876422, 6), 'signature': {'hash': b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 'keyId': 0}}}

On 4.4.3, it passes (PM-1770 Support $merge and $out executing on secondaries)
"""

"""
Right now the current behavior is to check the error code first and then check the error message when there is not error code or the error code is unexpected. Here's the pseudocode from the SDAM spec:
{code}
recoveringCodes = [11600, 11602, 13436, 189, 91]
notMasterCodes = [10107, 13435]

def isRecovering(message, code):
    if code and code in recoveringCodes:
        return true
    # if no code or an unrecognized code, use the error message.
    return ("not master or secondary" in message
        or "node is recovering" in message)

def isNotMaster(message, code):
    if code and code in notMasterCodes:
        return true
    # if no code or an unrecognized code, use the error message.
    if isRecovering(message, None):
        return false
    return ("not master" in message)
{code}

My proposal is to check error messages if and only if the error does not contain a code. For example:
{code}
recoveringCodes = [11600, 11602, 13436, 189, 91]
notMasterCodes = [10107, 13435]

def isRecovering(message, code):
    if code:
        if code in recoveringCodes:
            return true
    else:  # if no code, use the error message.
        return ("not master or secondary" in message
            or "node is recovering" in message)

def isNotMaster(message, code):
    if code:
        if code in notMasterCodes:
            return true
    else:  # if no code, use the error message.
        if isRecovering(message, None):
            return false
        return ("not master" in message)
{code}

This approach provides a good balance; new servers always return an error code so error messages will never be checked while old servers of backwards compatibility with MongoDB <=3.0. I did some research in git+Jira and found the following:
- The original "not master" string or error code 10107 SDAM behavior was decided by Jesse and Spencer in SERVER-9617 circa MongoDB 3.0.

- Starting in MongoDB 3.2, the server returns error codes for most NotPrimary/Shutdown type errors. This change happened in [this commit|https://github.com/mongodb/mongo/commit/aade72218f4e0dde65d5e64673b42b73baf6dec1] which landed in "r3.1.2-430-gaade72218f" as part of the MongoDB 3.2 OP_CMD work (PM-75). SERVER-18292 confusingly has a fix version 3.5.10 of but ignore that. The bulk of the work was implemented in 3.1. 

- In MongoDB 3.6, the server corrected an issue where mapReduce on a secondary returned an error without a code, see SERVER-27892. I believe this was the absolute final case where the server could respond without an error code.

- In MongoDB 3.6, the server corrected some remaining error code issues in SERVER-14601, for example:
{code}
{ok: 0, code: 28537, errmsg: 'Demoted from primary while removing from test.test'}
{code}
Was changed to use PrimarySteppedDown:
{code}
{ok: 0, code: 189, errmsg: 'Demoted from primary while removing from test.test'}
{code}
Note that none of the errors corrected in SERVER-14601 contain the string "not master".


This only hiccup with this plan is that on MongoDB <=3.2 there exists an error code 10058 that seems to be return with "not master" responses in some cases: https://github.com/mongodb/mongo/blob/v3.2/src/mongo/db/instance.cpp#L1148

However, I'm unable to get the server to actually return the 10058 error code. Maybe it's only returned for legacy wire protocols?

- MyMessageHandler -> process -> assembleResponse -> receivedInsert -> _receivedInsert -> error code 10058
$ git grep assembleResponse
src/mongo/db/db.cpp:186:                assembleResponse(opCtx.get(), m, dbresponse, port->remote());
src/mongo/db/dbdirectclient.cpp:130:    assembleResponse(_txn, toSend, dbResponse, dummyHost);
src/mongo/db/dbdirectclient.cpp:146:    assembleResponse(_txn, toSend, dbResponse, dummyHost);
src/mongo/db/instance.cpp:422:void assembleResponse(OperationContext* txn,
src/mongo/db/instance.h:72:void assembleResponse(OperationContext* txn,
"""