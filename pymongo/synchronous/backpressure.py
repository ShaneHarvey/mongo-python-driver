# Copyright 2025-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal backpressure utilities for PyMongo."""

from __future__ import annotations

import random
import time

from pymongo.lock import _create_lock

_IS_SYNC = True

DEFAULT_MAX_RETRY_ATTEMPTS = 3
DEFAULT_BACKOFF_DELAY = 0.1
DEFAULT_BACKOFF_MAX = 10
# TODO: How will we choose a default value for capacity?
#  Complexity arrises from the fact that client applications
#  may be architected to have 1000s of clients running few
#  operations or 1 client running many operations and everything in
#  between.
DEFAULT_RETRY_TOKEN_CAPACITY = 1000.0
DEFAULT_RETRY_TOKEN_RETURN = 0.1


class TokenBucket:
    """A simple token bucket implementation for rate limiting."""

    def __init__(
        self,
        capacity: float = DEFAULT_RETRY_TOKEN_CAPACITY,
        return_rate: float = DEFAULT_RETRY_TOKEN_RETURN,
    ):
        self.lock = _create_lock()
        self.capacity = capacity
        # TODO: Should the bucket start full?
        self.tokens = capacity
        self.last_refill = time.monotonic()
        self.return_rate = return_rate

    def consume(self) -> bool:
        """Consume a token from the bucket if available."""
        with self.lock:
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    def deposit(self):
        """Deposit a token back into the bucket."""
        with self.lock:
            self.tokens = min(self.capacity, self.tokens + 1 + self.return_rate)


class RetryStrategy:
    """A retry limiter that performs exponential backoff with jitter.

    Retry attempts are limited by a token bucket to prevent overwhelming the server during
    a prolonged outage or high load.
    """

    def __init__(
        self,
        token_bucket: TokenBucket,
        attempts: int = DEFAULT_MAX_RETRY_ATTEMPTS,
        delay: float = DEFAULT_BACKOFF_DELAY,
    ):
        self.token_bucket = token_bucket
        self.attempts = attempts
        self.delay = delay

    def record_success(self):
        """Record a successful operation."""
        self.token_bucket.deposit()

    def backoff(self, attempt: int) -> float:
        """Return the backoff duration for the given ."""
        jitter = random.random()  # noqa: S311
        return jitter * min(self.delay * (2**attempt), DEFAULT_BACKOFF_MAX)

    def should_retry(self, attempt: int) -> bool:
        """Return if we have budget to retry and how long to backoff."""
        if attempt >= self.attempts:
            return False
        # Check token bucket last since we only want to consume a token if we actually retry.
        if not self.token_bucket.consume():
            # TODO: Improve diagnostics when this case happens.
            #  We could add info to the exception and log.
            return False
        return True
