# Copyright 2014-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Class to monitor a MongoDB server on a background thread."""

import threading
import weakref

from pymongo import common, periodic_executor
from pymongo.errors import (NotMasterError,
                            OperationFailure,
                            _MonitorCheckCancelled)
from pymongo.ismaster import IsMaster
from pymongo.monotonic import time as _time
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription
from pymongo.srv_resolver import _SrvResolver


class MonitorBase(object):
    def __init__(self, topology, name, interval, min_interval):
        """Base class to do periodic work on a background thread.

        The the background thread is signaled to stop when the Topology or
        this instance is freed.
        """
        # We strongly reference the executor and it weakly references us via
        # this closure. When the monitor is freed, stop the executor soon.
        def target():
            monitor = self_ref()
            if monitor is None:
                return False  # Stop the executor.
            monitor._run()
            return True

        executor = periodic_executor.PeriodicExecutor(
            interval=interval,
            min_interval=min_interval,
            target=target,
            name=name)

        self._executor = executor

        def _on_topology_gc(dummy=None):
            # This prevents GC from waiting 10 seconds for isMaster to complete
            # See test_cleanup_executors_on_client_del.
            monitor = self_ref()
            if monitor:
                monitor.close()

        # Avoid cycles. When self or topology is freed, stop executor soon.
        self_ref = weakref.ref(self, executor.close)
        self._topology = weakref.proxy(topology, _on_topology_gc)

    def open(self):
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        self._executor.open()

    def close(self):
        """Close and stop monitoring.

        open() restarts the monitor after closing.
        """
        self._executor.close()

    def join(self, timeout=None):
        """Wait for the monitor to stop."""
        self._executor.join(timeout)

    def request_check(self):
        """If the monitor is sleeping, wake it soon."""
        self._executor.wake()


class Monitor(MonitorBase):
    def __init__(
            self,
            server_description,
            topology,
            pool,
            topology_settings):
        """Class to monitor a MongoDB server on a background thread.

        Pass an initial ServerDescription, a Topology, a Pool, and
        TopologySettings.

        The Topology is weakly referenced. The Pool must be exclusive to this
        Monitor.
        """
        super(Monitor, self).__init__(
            topology,
            "pymongo_server_monitor_thread",
            topology_settings.heartbeat_frequency,
            common.MIN_HEARTBEAT_INTERVAL)
        self._server_description = server_description
        self._pool = pool
        self._settings = topology_settings
        self._listeners = self._settings._pool_options.event_listeners
        pub = self._listeners is not None
        self._publish = pub and self._listeners.enabled_for_server_heartbeat
        self._connection = None
        self._rtt_monitor = _RttMonitor(
            topology, topology_settings, topology._create_pool_for_monitor(
                server_description.address))
        self.heartbeater = None

    def cancel_check(self):
        """Cancel any concurrent isMaster check.

        Note: this is called from a weakref.proxy callback and MUST NOT take
        any locks.
        """
        conn = self._connection
        if conn:
            # Note: we cannot close the socket because doing so may cause
            # concurrent reads/writes to hang until a timeout occurs
            # (depending on the platform).
            conn.cancel_context.cancel()

    def _start_rtt_monitor(self):
        """Start an _RttMonitor that periodically runs ping."""
        self._rtt_monitor.open()

    def close(self):
        super(Monitor, self).close()
        self._rtt_monitor.close()
        self.cancel_check()

    def _reset_connection(self):
        conn = self._connection
        if conn:
            conn.close_socket(None)
            self._pool.return_socket(conn)
            self._connection = None

    def _run(self):
        try:
            prev_sd = self._server_description
            try:
                self._server_description = self._check_server()
            except _MonitorCheckCancelled:
                # Already closed the connection, wait for the next check.
                return
            self._topology.on_change(self._server_description)

            if (self._server_description.is_server_type_known and
                     self._server_description.topology_version):
                self._start_rtt_monitor()
                # Immediately check for the next streaming response.
                self._executor.skip_sleep()

            if self._server_description.error and prev_sd.is_server_type_known:
                # Immediately retry on network errors.
                self._executor.skip_sleep()
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()

    def _check_server(self):
        """Call isMaster or read the next streaming response.

        Returns a ServerDescription.
        """
        start = _time()
        try:
            try:
                return self._check_once()
            except (OperationFailure, NotMasterError) as exc:
                # Update max cluster time even when isMaster fails.
                self._topology.receive_cluster_time(
                    exc.details.get('$clusterTime'))
                raise
        except ReferenceError:
            raise
        except Exception as error:
            address = self._server_description.address
            duration = _time() - start
            if self._publish:
                self._listeners.publish_server_heartbeat_failed(
                    address, duration, error)
            self._reset_connection()
            if isinstance(error, _MonitorCheckCancelled):
                raise
            self._rtt_monitor.reset()
            # TODO: move to after marking Unknown.
            self._topology.reset_pool(address)
            # Server type defaults to Unknown.
            return ServerDescription(address, error=error)

    def _check_once(self):
        """A single attempt to call ismaster.

        Returns a ServerDescription, or raises an exception.
        """
        address = self._server_description.address
        if self._publish:
            # TODO: Add the "awaited" field. Prose/spec test?
            self._listeners.publish_server_heartbeat_started(address)

        # TODO: add a test for self._connection.closed using 4.2 failpoint?
        # Divjot meeting.
        # TODO: Remove handshakes update the topology.
        if not self._connection or self._connection.cancel_context.cancelled():
            self._reset_connection()
            with self._pool.get_socket({}, checkout=True) as sock_info:
                self._connection = sock_info
        response, round_trip_time = self._check_with_socket(self._connection)
        if not response.awaitable:
            self._rtt_monitor.add_sample(round_trip_time)

        sd = ServerDescription(address, response,
                               self._rtt_monitor.average())
        if self._publish:
            self._listeners.publish_server_heartbeat_succeeded(
                address, round_trip_time, response)
        return sd

    def _check_with_socket(self, conn):
        """Return (IsMaster, round_trip_time).

        Can raise ConnectionFailure or OperationFailure.
        """
        cluster_time = self._topology.max_cluster_time()
        start = _time()
        if conn.more_to_come:
            # Read the next streaming isMaster (MongoDB 4.4+).
            response = IsMaster(conn._next_reply(), awaitable=True)
        elif (conn.performed_handshake and
              self._server_description.topology_version):
            # Initiate streaming isMaster (MongoDB 4.4+).
            response = conn._ismaster(
                cluster_time,
                self._server_description.topology_version,
                self._settings.heartbeat_frequency,
                None)
        else:
            # New connection handshake or polling isMaster (MongoDB <4.4).
            response = conn._ismaster(cluster_time, None, None, None)
        return response, _time() - start


class SrvMonitor(MonitorBase):
    def __init__(self, topology, topology_settings):
        """Class to poll SRV records on a background thread.

        Pass a Topology and a TopologySettings.

        The Topology is weakly referenced.
        """
        super(SrvMonitor, self).__init__(
            topology,
            "pymongo_srv_polling_thread",
            common.MIN_SRV_RESCAN_INTERVAL,
            topology_settings.heartbeat_frequency)
        self._settings = topology_settings
        self._seedlist = self._settings._seeds
        self._fqdn = self._settings.fqdn

    def _run(self):
        seedlist = self._get_seedlist()
        if seedlist:
            self._seedlist = seedlist
            try:
                self._topology.on_srv_update(self._seedlist)
            except ReferenceError:
                # Topology was garbage-collected.
                self.close()

    def _get_seedlist(self):
        """Poll SRV records for a seedlist.

        Returns a list of ServerDescriptions.
        """
        try:
            seedlist, ttl = _SrvResolver(self._fqdn).get_hosts_and_min_ttl()
            if len(seedlist) == 0:
                # As per the spec: this should be treated as a failure.
                raise Exception
        except Exception:
            # As per the spec, upon encountering an error:
            # - An error must not be raised
            # - SRV records must be rescanned every heartbeatFrequencyMS
            # - Topology must be left unchanged
            self.request_check()
            return None
        else:
            self._executor.update_interval(
                max(ttl, common.MIN_SRV_RESCAN_INTERVAL))
            return seedlist


class _RttMonitor(MonitorBase):
    def __init__(self, topology, topology_settings, pool):
        """Maintain round trip times for a server.

        The Topology is weakly referenced.
        """
        super(_RttMonitor, self).__init__(
            topology,
            "pymongo_server_rtt_thread",
            topology_settings.heartbeat_frequency,
            common.MIN_HEARTBEAT_INTERVAL)

        self._pool = pool
        self._moving_average = MovingAverage()
        self._lock = threading.Lock()

    def add_sample(self, sample):
        """Add a RTT sample."""
        with self._lock:
            self._moving_average.add_sample(sample)

    def average(self):
        """Get the calculated average, or None if no samples yet."""
        with self._lock:
            return self._moving_average.get()

    def reset(self):
        """Reset the average RTT."""
        with self._lock:
            return self._moving_average.reset()

    def _run(self):
        try:
            # NOTE: Only run when when using the streaming heartbeat protocol.
            # Optimization: Skip check if the server is unknown?
            rtt = self._ping()
            self.add_sample(rtt)
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()
        except Exception:
            self._pool.reset()

    def _ping(self):
        """Run an "isMaster" command and return the RTT."""
        with self._pool.get_socket({}) as sock_info:
            start = _time()
            sock_info.ismaster()
            return _time() - start
