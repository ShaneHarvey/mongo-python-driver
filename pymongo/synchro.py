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

import threading
import weakref

try:
    import asyncio
    ASYNCIO = True
except ImportError:
    ASYNCIO = False


async def noop():
    await asyncio.sleep(0.0000000000000000001)
    return


class Synchronizer:
    """Create and run a new event loop on a background thread."""
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._client_ref = None
        self._stopped = False
        self._lock = threading.Lock()
        # TODO: when embedded in MongoClient this must be a daemon?
        # Not a daemon, will be signaled to shutdown.
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the loop and wait for the runner thread to exit."""
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
        # Stop the loop.
        # https://docs.python.org/3/library/asyncio-dev.html#concurrency-and-multithreading
        self.call_soon(self.loop.stop)
        self._thread.join()

    def run(self):
        asyncio.set_event_loop(self.loop)
        # Run the event loop until stop is called.
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    def call_soon(self, callback, *args):
        """Schedule a synchronous callback on the event loop.

        Returns a handle which can cancel the pending callback.
        """
        handle = self.loop.call_soon_threadsafe(callback, *args)
        return handle

    # TODO: rename synchronize
    def run_coroutine(self, coro, timeout=None):
        """Run a coroutine on the event loop and wait for the result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    def sleep(self):
        self.run_coroutine(noop())

    def stop_del(self, _):
        """Stop the loop (GC safe)."""
        if self._stopped:
            return
        # Stop the loop.
        # https://docs.python.org/3/library/asyncio-dev.html#concurrency-and-multithreading
        self.call_soon(self.loop.stop)

    def register_client(self, client):
        # We strongly reference the Synchronizer and it weakly
        # references us via this closure. When the client is freed,
        # stop the Synchronizer soon.
        self._client_ref = weakref.ref(client, self.stop_del)
