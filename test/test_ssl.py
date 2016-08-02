# Copyright 2011-2015 MongoDB, Inc.
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

"""Tests for SSL support."""

import os
import socket
import sys

sys.path[0:0] = [""]

try:
    from urllib.parse import quote_plus
except ImportError:
    # Python 2
    from urllib import quote_plus

from pymongo import MongoClient, ssl_support
from pymongo.errors import (ConfigurationError,
                            ConnectionFailure,
                            OperationFailure)
from pymongo.ssl_support import HAVE_SSL, get_ssl_context, validate_cert_reqs
from test import (IntegrationTest,
                  client_context,
                  host,
                  pair,
                  port,
                  SkipTest,
                  unittest)
from test.utils import remove_all_users, connected


CERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         'certificates')
CLIENT_PEM = os.path.join(CERT_PATH, 'client.pem')
CLIENT_ENCRYPTED_PEM = os.path.join(CERT_PATH, 'client_encrypted.pem')
CA_PEM = os.path.join(CERT_PATH, 'ca.pem')
CRL_PEM = os.path.join(CERT_PATH, 'crl.pem')
SIMPLE_SSL = False
CERT_SSL = False
SERVER_IS_RESOLVABLE = False
MONGODB_X509_USERNAME = (
    "C=US,ST=California,L=Palo Alto,O=,OU=Drivers,CN=client")

# To fully test this start a mongod instance (built with SSL support) like so:
# mongod --dbpath /path/to/data/directory --sslOnNormalPorts \
# --sslPEMKeyFile /path/to/pymongo/test/certificates/server.pem \
# --sslCAFile /path/to/pymongo/test/certificates/ca.pem \
# --sslWeakCertificateValidation
# Also, make sure you have 'server' as an alias for localhost in /etc/hosts
#
# Note: For all replica set tests to pass, the replica set configuration must
# use 'server' for the hostname of all hosts.

if HAVE_SSL:
    import ssl


class TestClientSSL(IntegrationTest):

    @client_context.require_no_ssl
    def test_no_ssl_module(self):
        # Explicit
        self.assertRaises(ConfigurationError,
                          MongoClient, ssl=True)

        # Implied
        self.assertRaises(ConfigurationError,
                          MongoClient, ssl_certfile=CLIENT_PEM)

    def test_config_ssl(self):
        # Tests various ssl configurations
        self.assertRaises(ValueError, MongoClient, ssl='foo')
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_certfile=CLIENT_PEM)
        self.assertRaises(TypeError, MongoClient, ssl=0)
        self.assertRaises(TypeError, MongoClient, ssl=5.5)
        self.assertRaises(TypeError, MongoClient, ssl=[])

        self.assertRaises(IOError, MongoClient, ssl_certfile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, ssl_certfile=True)
        self.assertRaises(TypeError, MongoClient, ssl_certfile=[])
        self.assertRaises(IOError, MongoClient, ssl_keyfile="NoSuchFile")
        self.assertRaises(TypeError, MongoClient, ssl_keyfile=True)
        self.assertRaises(TypeError, MongoClient, ssl_keyfile=[])

        # Test invalid combinations
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_keyfile=CLIENT_PEM)
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_certfile=CLIENT_PEM)
        self.assertRaises(ConfigurationError,
                          MongoClient,
                          ssl=False,
                          ssl_keyfile=CLIENT_PEM,
                          ssl_certfile=CLIENT_PEM)

        self.assertRaises(
            ValueError, validate_cert_reqs, 'ssl_cert_reqs', 3)
        self.assertRaises(
            ValueError, validate_cert_reqs, 'ssl_cert_reqs', -1)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', None), None)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', ssl.CERT_NONE),
            ssl.CERT_NONE)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', ssl.CERT_OPTIONAL),
            ssl.CERT_OPTIONAL)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', ssl.CERT_REQUIRED),
            ssl.CERT_REQUIRED)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 0), ssl.CERT_NONE)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 1), ssl.CERT_OPTIONAL)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 2), ssl.CERT_REQUIRED)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 'CERT_NONE'), ssl.CERT_NONE)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 'CERT_OPTIONAL'),
            ssl.CERT_OPTIONAL)
        self.assertEqual(
            validate_cert_reqs('ssl_cert_reqs', 'CERT_REQUIRED'),
            ssl.CERT_REQUIRED)


class TestSSL(IntegrationTest):

    @client_context.require_ssl
    def setUp(self):
        super(TestSSL, self).setUp()

    @client_context.require_ssl_cert_none
    def test_simple_ssl(self):
        client = self.client
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoClient(pair,
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl=True,
                                 ssl_cert_reqs=ssl.CERT_NONE)

        db = client.pymongo_ssl_test
        db.test.drop()
        db.test.insert_one({'ssl': True})
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    @client_context.require_ssl_certfile
    def test_ssl_pem_passphrase(self):
        vi = sys.version_info
        if vi[0] == 2 and vi < (2, 7, 9) or vi[0] == 3 and vi < (3, 3):
            self.assertRaises(
                ConfigurationError,
                MongoClient,
                'server',
                ssl=True,
                ssl_certfile=CLIENT_ENCRYPTED_PEM,
                ssl_pem_passphrase="clientpassword",
                ssl_ca_certs=CA_PEM,
                serverSelectionTimeoutMS=100)
        else:
            connected(MongoClient('server',
                                  ssl=True,
                                  ssl_certfile=CLIENT_ENCRYPTED_PEM,
                                  ssl_pem_passphrase="clientpassword",
                                  ssl_ca_certs=CA_PEM,
                                  serverSelectionTimeoutMS=100))

            uri_fmt = ("mongodb://server/?ssl=true"
                       "&ssl_certfile=%s&ssl_pem_passphrase=clientpassword"
                       "&ssl_ca_certs=%s&serverSelectionTimeoutMS=100")
            connected(MongoClient(uri_fmt % (CLIENT_ENCRYPTED_PEM, CA_PEM)))

    @client_context.require_ssl_certfile
    def test_cert_ssl(self):
        client = self.client
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoClient(pair,
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl=True,
                                 ssl_cert_reqs=ssl.CERT_NONE,
                                 ssl_certfile=CLIENT_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        db.test.insert_one({'ssl': True})
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    @client_context.require_ssl_certfile
    def test_cert_ssl_implicitly_set(self):
        client = MongoClient(host, port,
                             ssl_cert_reqs=ssl.CERT_NONE,
                             ssl_certfile=CLIENT_PEM)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            client = MongoClient(pair,
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl_cert_reqs=ssl.CERT_NONE,
                                 ssl_certfile=CLIENT_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        db.test.insert_one({'ssl': True})
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    @client_context.require_ssl_certfile
    @client_context.require_server_resolvable
    def test_cert_ssl_validation(self):
        client = MongoClient('server',
                             ssl=True,
                             ssl_certfile=CLIENT_PEM,
                             ssl_cert_reqs=ssl.CERT_REQUIRED,
                             ssl_ca_certs=CA_PEM)
        response = client.admin.command('ismaster')
        if 'setName' in response:
            if response['primary'].split(":")[0] != 'server':
                raise SkipTest("No hosts in the replicaset for 'server'. "
                               "Cannot validate hostname in the certificate")

            client = MongoClient('server',
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl=True,
                                 ssl_certfile=CLIENT_PEM,
                                 ssl_cert_reqs=ssl.CERT_REQUIRED,
                                 ssl_ca_certs=CA_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        db.test.insert_one({'ssl': True})
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    @client_context.require_ssl_certfile
    @client_context.require_server_resolvable
    def test_cert_ssl_uri_support(self):
        uri_fmt = ("mongodb://server/?ssl=true&ssl_certfile=%s&ssl_cert_reqs"
                   "=%s&ssl_ca_certs=%s&ssl_match_hostname=true")
        client = MongoClient(uri_fmt % (CLIENT_PEM, 'CERT_REQUIRED', CA_PEM))

        db = client.pymongo_ssl_test
        db.test.drop()
        db.test.insert_one({'ssl': True})
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    @client_context.require_ssl_certfile
    @client_context.require_server_resolvable
    def test_cert_ssl_validation_optional(self):
        client = MongoClient('server',
                             ssl=True,
                             ssl_certfile=CLIENT_PEM,
                             ssl_cert_reqs=ssl.CERT_OPTIONAL,
                             ssl_ca_certs=CA_PEM)

        response = client.admin.command('ismaster')
        if 'setName' in response:
            if response['primary'].split(":")[0] != 'server':
                raise SkipTest("No hosts in the replicaset for 'server'. "
                               "Cannot validate hostname in the certificate")

            client = MongoClient('server',
                                 replicaSet=response['setName'],
                                 w=len(response['hosts']),
                                 ssl=True,
                                 ssl_certfile=CLIENT_PEM,
                                 ssl_cert_reqs=ssl.CERT_OPTIONAL,
                                 ssl_ca_certs=CA_PEM)

        db = client.pymongo_ssl_test
        db.test.drop()
        db.test.insert_one({'ssl': True})
        self.assertTrue(db.test.find_one()['ssl'])
        client.drop_database('pymongo_ssl_test')

    @client_context.require_ssl_certfile
    def test_cert_ssl_validation_hostname_matching(self):
        response = self.client.admin.command('ismaster')

        with self.assertRaises(ConnectionFailure):
            connected(MongoClient(pair,
                                  ssl=True,
                                  ssl_certfile=CLIENT_PEM,
                                  ssl_cert_reqs=ssl.CERT_REQUIRED,
                                  ssl_ca_certs=CA_PEM,
                                  serverSelectionTimeoutMS=100))

        connected(MongoClient(pair,
                              ssl=True,
                              ssl_certfile=CLIENT_PEM,
                              ssl_cert_reqs=ssl.CERT_REQUIRED,
                              ssl_ca_certs=CA_PEM,
                              ssl_match_hostname=False,
                              serverSelectionTimeoutMS=100))


        if 'setName' in response:
            with self.assertRaises(ConnectionFailure):
                connected(MongoClient(pair,
                                      replicaSet=response['setName'],
                                      ssl=True,
                                      ssl_certfile=CLIENT_PEM,
                                      ssl_cert_reqs=ssl.CERT_REQUIRED,
                                      ssl_ca_certs=CA_PEM,
                                      serverSelectionTimeoutMS=100))

            connected(MongoClient(pair,
                                  replicaSet=response['setName'],
                                  ssl=True,
                                  ssl_certfile=CLIENT_PEM,
                                  ssl_cert_reqs=ssl.CERT_REQUIRED,
                                  ssl_ca_certs=CA_PEM,
                                  ssl_match_hostname=False,
                                  serverSelectionTimeoutMS=100))

    @client_context.require_ssl_certfile
    def test_ssl_crlfile_support(self):
        if not hasattr(ssl, 'VERIFY_CRL_CHECK_LEAF'):
            self.assertRaises(
                ConfigurationError,
                MongoClient,
                'server',
                ssl=True,
                ssl_ca_certs=CA_PEM,
                ssl_crlfile=CRL_PEM,
                serverSelectionTimeoutMS=100)
        else:
            connected(MongoClient('server',
                                  ssl=True,
                                  ssl_ca_certs=CA_PEM,
                                  serverSelectionTimeoutMS=100))

            with self.assertRaises(ConnectionFailure):
                connected(MongoClient('server',
                                      ssl=True,
                                      ssl_ca_certs=CA_PEM,
                                      ssl_crlfile=CRL_PEM,
                                      serverSelectionTimeoutMS=100))

            uri_fmt = ("mongodb://server/?ssl=true&"
                       "ssl_ca_certs=%s&serverSelectionTimeoutMS=100")
            connected(MongoClient(uri_fmt % (CA_PEM,)))

            uri_fmt = ("mongodb://server/?ssl=true&ssl_crlfile=%s"
                       "&ssl_ca_certs=%s&serverSelectionTimeoutMS=100")
            with self.assertRaises(ConnectionFailure):
                connected(MongoClient(uri_fmt % (CRL_PEM, CA_PEM)))

    @client_context.require_ssl_certfile
    @client_context.require_server_resolvable
    def test_validation_with_system_ca_certs(self):
        if sys.platform == "win32":
            raise SkipTest("Can't test system ca certs on Windows.")

        if sys.version_info < (2, 7, 9):
            raise SkipTest("Can't load system CA certificates.")

        # Tell OpenSSL where CA certificates live.
        os.environ['SSL_CERT_FILE'] = CA_PEM
        try:
            with self.assertRaises(ConnectionFailure):
                # Server cert is verified but hostname matching fails
                connected(MongoClient(pair,
                                      ssl=True,
                                      serverSelectionTimeoutMS=100))

            # Server cert is verified. Disable hostname matching.
            connected(MongoClient(pair,
                                  ssl=True,
                                  ssl_match_hostname=False,
                                  serverSelectionTimeoutMS=100))

            # Server cert and hostname are verified.
            connected(MongoClient('server',
                                  ssl=True,
                                  serverSelectionTimeoutMS=100))

            # Server cert and hostname are verified.
            connected(
                MongoClient(
                    'mongodb://server/?ssl=true&serverSelectionTimeoutMS=100'))
        finally:
            os.environ.pop('SSL_CERT_FILE')

    def test_system_certs_config_error(self):
        ctx = get_ssl_context(None, None, None, None, ssl.CERT_NONE, None)
        if ((sys.platform != "win32"
             and hasattr(ctx, "set_default_verify_paths"))
                or hasattr(ctx, "load_default_certs")):
            raise SkipTest(
                "Can't test when system CA certificates are loadable.")

        have_certifi = ssl_support.HAVE_CERTIFI
        have_wincertstore = ssl_support.HAVE_WINCERTSTORE
        # Force the test regardless of environment.
        ssl_support.HAVE_CERTIFI = False
        ssl_support.HAVE_WINCERTSTORE = False
        try:
            with self.assertRaises(ConfigurationError):
                MongoClient("mongodb://localhost/?ssl=true")
        finally:
            ssl_support.HAVE_CERTIFI = have_certifi
            ssl_support.HAVE_WINCERTSTORE = have_wincertstore

    def test_certifi_support(self):
        if hasattr(ssl, "SSLContext"):
            # SSLSocket doesn't provide ca_certs attribute on pythons
            # with SSLContext and SSLContext provides no information
            # about ca_certs.
            raise SkipTest("Can't test when SSLContext available.")
        if not ssl_support.HAVE_CERTIFI:
            raise SkipTest("Need certifi to test certifi support.")

        have_wincertstore = ssl_support.HAVE_WINCERTSTORE
        # Force the test on Windows, regardless of environment.
        ssl_support.HAVE_WINCERTSTORE = False
        try:
            ctx = get_ssl_context(None, None, None, CA_PEM, ssl.CERT_REQUIRED, None)
            ssl_sock = ctx.wrap_socket(socket.socket())
            self.assertEqual(ssl_sock.ca_certs, CA_PEM)

            ctx = get_ssl_context(None, None, None, None, None, None)
            ssl_sock = ctx.wrap_socket(socket.socket())
            self.assertEqual(ssl_sock.ca_certs, ssl_support.certifi.where())
        finally:
            ssl_support.HAVE_WINCERTSTORE = have_wincertstore

    def test_wincertstore(self):
        if sys.platform != "win32":
            raise SkipTest("Only valid on Windows.")
        if hasattr(ssl, "SSLContext"):
            # SSLSocket doesn't provide ca_certs attribute on pythons
            # with SSLContext and SSLContext provides no information
            # about ca_certs.
            raise SkipTest("Can't test when SSLContext available.")
        if not ssl_support.HAVE_WINCERTSTORE:
            raise SkipTest("Need wincertstore to test wincertstore.")

        ctx = get_ssl_context(None, None, None, CA_PEM, ssl.CERT_REQUIRED, None)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, CA_PEM)

        ctx = get_ssl_context(None, None, None, None, None, None)
        ssl_sock = ctx.wrap_socket(socket.socket())
        self.assertEqual(ssl_sock.ca_certs, ssl_support._WINCERTS.name)

    @client_context.require_version_min(2, 5, 3, -1)
    @client_context.require_auth
    def test_mongodb_x509_auth(self):
        ssl_client = self.client
        self.addCleanup(ssl_client['$external'].logout)
        self.addCleanup(remove_all_users, ssl_client['$external'])
        self.addCleanup(remove_all_users, ssl_client.admin)

        ssl_client.admin.add_user('admin', 'pass')
        ssl_client.admin.authenticate('admin', 'pass')

        # Give admin all necessary privileges.
        ssl_client['$external'].add_user(MONGODB_X509_USERNAME, roles=[
            {'role': 'readWriteAnyDatabase', 'db': 'admin'},
            {'role': 'userAdminAnyDatabase', 'db': 'admin'}])

        ssl_client.admin.logout()

        coll = ssl_client.pymongo_test.test
        self.assertRaises(OperationFailure, coll.count)
        self.assertTrue(ssl_client.admin.authenticate(
            MONGODB_X509_USERNAME, mechanism='MONGODB-X509'))
        coll.drop()
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'MONGODB-X509' % (
                   quote_plus(MONGODB_X509_USERNAME), host, port))
        # SSL options aren't supported in the URI...
        self.assertTrue(MongoClient(uri,
                                    ssl=True, ssl_certfile=CLIENT_PEM))

        # Should require a username
        uri = ('mongodb://%s:%d/?authMechanism=MONGODB-X509' % (host,
                                                                port))
        client_bad = MongoClient(
            uri, ssl=True, ssl_cert_reqs="CERT_NONE", ssl_certfile=CLIENT_PEM)
        self.assertRaises(OperationFailure,
                          client_bad.pymongo_test.test.delete_one, {})

        # Auth should fail if username and certificate do not match
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'MONGODB-X509' % (
                   quote_plus("not the username"), host, port))

        bad_client = MongoClient(
            uri, ssl=True, ssl_cert_reqs="CERT_NONE", ssl_certfile=CLIENT_PEM)
        with self.assertRaises(OperationFailure):
            bad_client.pymongo_test.test.find_one()

        self.assertRaises(OperationFailure, ssl_client.admin.authenticate,
                          "not the username",
                          mechanism="MONGODB-X509")

        # Invalid certificate (using CA certificate as client certificate)
        uri = ('mongodb://%s@%s:%d/?authMechanism='
               'MONGODB-X509' % (
                   quote_plus(MONGODB_X509_USERNAME), host, port))
        try:
            connected(MongoClient(uri,
                                  ssl=True,
                                  ssl_cert_reqs="CERT_NONE",
                                  ssl_certfile=CA_PEM,
                                  serverSelectionTimeoutMS=100))
        except OperationFailure:
            pass
        else:
            self.fail("Invalid certificate accepted.")


if __name__ == "__main__":
    unittest.main()
