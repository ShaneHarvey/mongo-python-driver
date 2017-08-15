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


from pymongo.errors import (ConnectionFailure, CursorNotFound,
                            InvalidOperation, PyMongoError)


class ChangeStreamOptions(object):

    def __init__(self, full_document=None, resume_after=None,
                 max_await_time_ms=None, **kwargs):
        if full_document is None:
            self.full_document = 'default'
        else:
            self.full_document = full_document
        self.resume_after = resume_after
        self.max_await_time_ms = max_await_time_ms
        self.aggregate_kwargs = kwargs


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
        self._pipeline = pipeline
        self._full_document = full_document
        self._resume_token = resume_after
        self._max_await_time_ms = max_await_time_ms
        self._aggregate_kwargs = kwargs
        self._read_preference = collection.read_preference
        self._killed = False
        self._cursor = self._create_cursor()

    def _create_cursor(self):
        """Initialize the cursor or raise a fatal error"""
        options = {}
        if self._full_document is not None:
            options['fullDocument'] = self._full_document
        if self._resume_token is not None:
            options['resumeAfter'] = self._resume_token
        full_pipeline = [{'$changeStream': options}]
        full_pipeline.extend(self._pipeline)
        try:
            cursor = self._collection.aggregate(full_pipeline,
                                                **self._aggregate_kwargs)
            if self._max_await_time_ms is not None:
                cursor.max_await_time_ms(self._max_await_time_ms)
            return cursor
        except:
            self._killed = True
            raise

    def close(self):
        """Close this ChangeStream."""
        if self._cursor:
            self._cursor.close()
        self._killed = True

    def __iter__(self):
        return self

    def next(self):
        """Advance the cursor."""
        if self._killed:
            raise StopIteration
        if not self._cursor.alive:
            self._cursor = self._create_cursor()
        while True:
            try:
                next_change = self._cursor.next()
            except (ConnectionFailure, CursorNotFound):
                try:
                    self._cursor.close()
                except PyMongoError:
                    pass
                self._cursor = self._create_cursor()
            except Exception:
                self._killed = True
                raise
            else:
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

            for doc in collection.aggregate(pipeline):
                print(doc)

        .. note:: :attr:`alive` can be True while iterating a cursor from
          a failed server. In this case :attr:`alive` will return False after
          :meth:`next` fails to retrieve the next batch of results from the
          server.
        """
        return self._cursor.alive or (not self._killed)
