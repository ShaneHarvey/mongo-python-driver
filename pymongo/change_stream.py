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

"""ChangeStream cursor to iterate over changes on a collection.

MongoDB 3.6 introduces a new `$changeStream aggregation stage`_ to watch
changes to documents in a collection.

To improve the usability of this stage, :class:`~pymongo.collection.Collection`
offers the :meth:`~pymongo.collection.Collection.watch` helper method which
returns a :class:`~pymongo.change_stream.ChangeStream`.

Change Stream Documents
~~~~~~~~~~~~~~~~~~~~~~~

To see the structure of documents delivered by a change stream:

.. code-block:: python

    with collection.watch() as change_stream:
        collection.insert_one({'_id': 1, 'foo': 'bar'})
        print('Insert change document: %s' % change_stream.next())
        collection.update_one({'_id': 1}, {'$set': {'new': 1}, '$unset': {'foo': 1}})
        print('Update change document: %s' % change_stream.next())
        collection.replace_one({'new': 1}, {'foo': 'bar'})
        print('Replace change document: %s' % change_stream.next())
        collection.delete_one({'_id': 1})
        print('Delete change document: %s' % change_stream.next())
        collection.drop()
        print('Invalidate change document: %s' % change_stream.next())


Resume Process
~~~~~~~~~~~~~~

A ChangeStream cursor automatically resumes when it encounters a recoverable
error during iteration. For example, when a getMore command fails with a
network error the cursor is recreated with the proper resume token. The resume
process is transparent to the application and ensures no change documents are
lost; the call to :meth:`~pymongo.change_stream.ChangeStream.next` blocks until
the next change document is returned or an unrecoverable error is raised.

.. code-block:: python

    try:
        for change in db.collection.watch():
            print(change)
    except pymongo.errors.PyMongoError:
        # We know for sure it's unrecoverable:
        log.error("...")

For a precise description of the resume process see the
`Change Streams specification`_.

Change streams are valid until the underlying collection is dropped at which
point the change stream cursor receives one final change document with an
operationType of "invalidate" and the cursor is closed.

.. _$changeStream aggregation stage:
    https://docs.mongodb.com/manual/reference/operator/aggregation/changeStream/

.. _Change Streams specification:
    https://github.com/mongodb/specifications/blob/master/source/change-streams.rst
"""

import copy


from pymongo.errors import (ConnectionFailure, CursorNotFound,
                            InvalidOperation, PyMongoError)


class ChangeStream(object):

    def __init__(self, collection, pipeline, full_document,
                 resume_after=None, max_await_time_ms=None, batch_size=None,
                 collation=None):
        """A change stream cursor.

        Should not be called directly by application developers. Use
        :meth:`~pymongo.collection.Collection.watch` instead.

        :Parameters:
          - `collection`: The watched :class:`~pymongo.collection.Collection`.
          - `pipeline`: A list of aggregation pipeline stages to append to an
            initial `$changeStream` aggregation stage.
          - `full_document` (string): The fullDocument to pass as an option
            to the $changeStream pipeline stage. Allowed values: 'default',
            'updateLookup'. When set to 'updateLookup', the change notification
            for partial updates will include both a delta describing the
            changes to the document, as well as a copy of the entire document
            that was changed from some time after the change occurred.
          - `resume_after` (optional): The logical starting point for this
            change stream.
          - `max_await_time_ms` (optional): The maximum time in milliseconds
            for the server to wait for changes before responding to a getMore
            operation.
          - `batch_size` (optional): The maximum number of documents to return
            per batch.
          - `collation` (optional): The :class:`~pymongo.collation.Collation`
            to use for the aggregation.

        .. versionadded: 3.6
        """
        self._collection = collection
        self._pipeline = copy.deepcopy(pipeline)
        self._full_document = full_document
        self._resume_token = copy.deepcopy(resume_after)
        self._max_await_time_ms = max_await_time_ms
        self._batch_size = batch_size
        self._collation = collation
        self._cursor = self._create_cursor()

    def _full_pipeline(self):
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
        cursor = self._collection.aggregate(
            self._full_pipeline(), batchSize=self._batch_size,
            collation=self._collation)
        if self._max_await_time_ms is not None:
            cursor.max_await_time_ms(self._max_await_time_ms)
        return cursor

    def close(self):
        """Close this ChangeStream."""
        self._cursor.close()

    def __iter__(self):
        return self

    def next(self):
        """Advance the cursor.

        This method blocks until the next change document is returned or an
        unrecoverable error is raised.

        Raises :exc:`StopIteration` if this ChangeStream is closed.
        """
        while True:
            try:
                next_change = self._cursor.next()
            except (ConnectionFailure, CursorNotFound):
                try:
                    self._cursor.close()
                except PyMongoError:
                    pass
                self._cursor = self._create_cursor()
                continue
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
