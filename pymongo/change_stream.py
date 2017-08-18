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

"""ChangeStream cursor to iterate over changes on a collection."""

import copy


from pymongo.errors import (ConnectionFailure, CursorNotFound,
                            InvalidOperation, PyMongoError)


class ChangeStream(object):

    def __init__(self, collection, pipeline, full_document=None,
                 resume_after=None, max_await_time_ms=None, batch_size=None,
                 collation=None):
        """A change stream cursor.

        :Parameters:
          - `collection`: The watched :class:`~pymongo.collection.Collection`.
          - `pipeline`: A list of aggregation pipeline stages to append to an
            initial `$changeStream` aggregation stage.
          - `full_document` (optional): The fullDocument to pass as an option
            to the $changeStream pipeline stage. Allowed values: 'default',
            'updateLookup'.  Defaults to 'default'.
            When set to 'updateLookup', the change notification for partial
            updates will include both a delta describing the changes to the
            document, as well as a copy of the entire document that was
            changed from some time after the change occurred.
          - `resume_after` (optional): The logical starting point for this
            change stream.
          - `max_await_time_ms` (optional): The maximum time in milliseconds
            for the server to wait for changes before responding to a getMore
            operation.
          - `batch_size` (optional): The maximum number of documents to return
            per batch.
          - `collation` (optional): The :class:`~pymongo.collation.Collation`
            to use for the aggregation.

        ..versionadded: 3.6
        """
        self._collection = collection
        self._pipeline = copy.deepcopy(pipeline)
        self._full_document = full_document
        self._resume_token = copy.deepcopy(resume_after)
        self._max_await_time_ms = max_await_time_ms
        self._batch_size = batch_size
        self._collation = collation
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
            cursor = self._collection.aggregate(
                self.full_pipeline, batchSize=self._batch_size,
                collation=self._collation)
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
