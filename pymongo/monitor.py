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

import weakref

from pymongo import common, periodic_executor
from pymongo.errors import OperationFailure, _MonitorCheckCancelled
from pymongo.monotonic import time as _time
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription
from pymongo.server_type import SERVER_TYPE
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
                monitor._executor.close()
                monitor.interrupt_check()

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

    def interrupt_check(self):
        """Override this method to interrupt the _run method."""
        pass


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
        self._rtt_executor = None
        self.current_sock = None
        self._avg_round_trip_time = RttMonitor(
            topology, topology_settings, self)

    def interrupt_check(self):
        """Interrupt a concurrent isMaster check by closing the socket.

        Note: this is called from a weakref.proxy callback and MUST NOT take
        any locks.
        """
        sock = self.current_sock
        if sock:
            sock.cancel_context.cancel()
            # sock.close_socket(None)

    def _start_rtt_monitor(self):
        """Start an RttMonitor that periodically runs ping."""
        self._avg_round_trip_time.open()

    def close(self):
        super(Monitor, self).close()
        self._avg_round_trip_time.close()
        self.interrupt_check()

        # Increment the pool_id and maybe close the socket. If the executor
        # thread has the socket checked out, it will be closed when checked in.
        self._pool.reset()

    def _run(self):
        try:
            self._server_description = self._check_with_retry(long_poll=False)
            self._topology.on_change(self._server_description)
            while (not self._executor._stopped and
                   self._server_description.is_server_type_known and
                   self._server_description.topology_version):
                self._start_rtt_monitor()
                self._server_description = self._check_with_retry(long_poll=True)
                self._topology.on_change(self._server_description)
        except _MonitorCheckCancelled:
            pass
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()

    def _check_with_retry(self, long_poll):
        """Call ismaster once or twice. Reset server's pool on error.

        Returns a ServerDescription.
        """
        # According to the spec, if an ismaster call fails we reset the
        # server's pool. If a server was once connected, change its type
        # to Unknown only after retrying once.
        address = self._server_description.address
        retry = True
        if self._server_description.server_type == SERVER_TYPE.Unknown:
            retry = False

        start = _time()
        try:
            return self._check_once(long_poll)
        except (ReferenceError, _MonitorCheckCancelled):
            raise
        except Exception as error:
            error_time = _time() - start
            if self._publish:
                self._listeners.publish_server_heartbeat_failed(
                    address, error_time, error)
            self._topology.reset_pool(address)
            # TODO: is it correct to include both error and topology_version?
            # For command errors I think not.
            # For connection errors, maybe? Consider case study.
            default = ServerDescription(
                address, error=error,
                topology_version=self._server_description.topology_version)
            if not retry:
                self._avg_round_trip_time.reset()
                # Server type defaults to Unknown.
                return default

            # Try a second and final time. If it fails return original error.
            # Always send metadata: this is a new connection.
            start = _time()
            try:
                return self._check_once(long_poll=False)
            except (ReferenceError, _MonitorCheckCancelled):
                raise
            except Exception as error:
                error_time = _time() - start
                if self._publish:
                    self._listeners.publish_server_heartbeat_failed(
                        address, error_time, error)
                self._avg_round_trip_time.reset()
                return default

    def _check_once(self, long_poll):
        """A single attempt to call ismaster.

        Returns a ServerDescription, or raises an exception.
        """
        address = self._server_description.address
        if self._publish:
            self._listeners.publish_server_heartbeat_started(address)
        with self._pool.get_socket({}) as sock_info:
            self.current_sock = sock_info
            response, round_trip_time = self._check_with_socket(
                sock_info, long_poll)
            if not long_poll:
                self._avg_round_trip_time.add_sample(round_trip_time)
            sd = ServerDescription(
                address=address,
                ismaster=response,
                round_trip_time=self._avg_round_trip_time.get())
            if self._publish:
                self._listeners.publish_server_heartbeat_succeeded(
                    address, round_trip_time, response)

            return sd

    def _check_with_socket(self, sock_info, long_poll):
        """Return (IsMaster, round_trip_time).

        Can raise ConnectionFailure or OperationFailure.
        """
        if long_poll:
            topology_version = self._server_description.topology_version
        else:
            topology_version = None
        start = _time()
        try:
            return (sock_info.ismaster(self._pool.opts.metadata,
                                       self._topology.max_cluster_time(),
                                       topology_version,
                                       self._settings.heartbeat_frequency),
                    _time() - start)
        except OperationFailure as exc:
            # Update max cluster time even when isMaster fails.
            self._topology.receive_cluster_time(
                exc.details.get('$clusterTime'))
            raise


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


class RttMonitor(MonitorBase):
    def __init__(self, topology, topology_settings, monitor):
        """Maintain round trip times for a server.

        The Topology is weakly referenced.
        """
        super(RttMonitor, self).__init__(
            topology,
            "pymongo_server_rtt_thread",
            topology_settings.heartbeat_frequency,
            common.MIN_HEARTBEAT_INTERVAL)

        # We weakly reference the monitor and it strongly references us.
        self._monitor = weakref.proxy(monitor)
        self._avg_round_trip_time = MovingAverage()

    def add_sample(self, sample):
        """Add a RTT sample."""
        self._avg_round_trip_time.add_sample(sample)

    def get(self):
        """Get the calculated average, or None if no samples yet."""
        return self._avg_round_trip_time.get()

    def reset(self):
        """Reset the average RTT."""
        # TODO: Call this method when a server is reset from an app error.
        return self._avg_round_trip_time.reset()

    def _run(self):
        try:
            # NOTE: Only run when when using the streaming heartbeat protocol.
            # TODO: skip check if the server is unknown.
            # TODO: shutdown if the server is downgraded.
            rtt = self._ping()
            self.add_sample(rtt)
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()
        except Exception as error:
            # TODO: handle errors with _MongoClientErrorHandler
            pass

    def _ping(self):
        """Run a "ping" command.

        Returns the RTT of the ping.
        """
        with self._monitor._pool.get_socket({}) as sock_info:
            start = _time()
            self._ping_with_socket(sock_info)
            return _time() - start

    def _ping_with_socket(self, sock_info):
        """Return (IsMaster, round_trip_time).

        Can raise ConnectionFailure or OperationFailure.
        """
        # TODO: ping?
        return sock_info.ismaster(self._monitor._pool.opts.metadata,
                                  None,  # TODO: $clusterTime?
                                  None, None),
