# Copyright 2017 MongoDB, Inc.
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

"""Change Stream cursor."""

import copy


from pymongo.errors import (ConnectionFailure, CursorNotFound,
                            InvalidOperation, PyMongoError)


class ChangeStream(object):

    def __init__(self, collection, pipeline, full_document=None,
                 resume_after=None, max_await_time_ms=None, **kwargs):
        """A change stream cursor.

        :Parameters:
          - `collection`: The watched :class:`~pymongo.collection.Collection`.
          - `pipeline`: A list of aggregation pipeline stages to append to an
            initial `$changeStream` aggregation stage.
          - `options`: The :class:`~pymongo.change_stream.ChangeStreamOptions`
            to provide to the `$changeStream` aggregation.
        """
        self._collection = collection
        self._pipeline = copy.deepcopy(pipeline)
        self._full_document = full_document
        self._resume_token = copy.deepcopy(resume_after)
        self._max_await_time_ms = max_await_time_ms
        self._aggregate_kwargs = copy.deepcopy(kwargs)
        self._killed = False
        self._cursor = self._create_cursor()

    @property
    def full_pipeline(self):
        """Return the full aggregation pipeline for this ChangeStream."""
        options = {}
        if self._full_document is not None:
            options['fullDocument'] = self._full_document
        if self._resume_token is not None:
            options['resumeAfter'] = self._resume_token
        full_pipeline = [{'$changeStream': options}]
        full_pipeline.extend(self._pipeline)
        return full_pipeline

    def _create_cursor(self):
        """Initialize the cursor or raise a fatal error"""
        try:
            cursor = self._collection.aggregate(self.full_pipeline,
                                                **self._aggregate_kwargs)
            if self._max_await_time_ms is not None:
                cursor.max_await_time_ms(self._max_await_time_ms)
            return cursor
        except:
            self._killed = True
            raise

    def close(self):
        """Close this ChangeStream."""
        self._cursor.close()
        self._killed = True

    def __iter__(self):
        return self

    def next(self):
        """Advance the cursor.

        Raises StopIteration if this ChangeStream is closed, or there are no
        current changes.

        """
        if self._killed:
            raise StopIteration
        try:
            next_change = self._cursor.next()
        except (ConnectionFailure, CursorNotFound):
            try:
                self._cursor.close()
            except PyMongoError:
                pass
            self._cursor = self._create_cursor()
            if not self._cursor.retrieved:
                raise StopIteration
            next_change = self._cursor.next()
        except StopIteration:
            raise
        except:
            self._killed = True
            raise
        try:
            self._resume_token = next_change['_id']
        except KeyError:
            raise InvalidOperation(
                "Cannot provide resume functionality when the resume "
                "token is missing.")
        return next_change

    __next__ = next

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?

        Even if :attr:`alive` is ``True``, :meth:`next` can raise
        :exc:`StopIteration`. Best to use a for loop::

            with collection.watch(pipeline) as change_stream:
                while change_stream.alive:
                    for change in change_stream:
                        print(change)
        """
        return self._cursor.alive or (not self._killed)
