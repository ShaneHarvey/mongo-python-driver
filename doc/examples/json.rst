JSON Examples
=============

This example shows how to use :mod:`bson.json_util` to serialize BSON objects
to JSON.

:mod:`bson.json_util` provides two helper methods `dumps` and `loads` that
wrap the native :mod:`json` methods and provide explicit BSON conversion to
and from MongoDB `Extended JSON`_.

There are various use cases for expressing BSON documents in a text rather
that binary format.  They broadly fall into two categories:

* JSON-like: for things like a web API, where one is sending a document (or a
  projection of a document) that only uses ordinary JSON type primitives, it's
  desirable to represent numbers in the native JSON format.  This output is
  also the most human readable and is useful for debugging and documentation.
  This output in produced with :const:`~bson.json_util.RELAXED_JSON_OPTIONS`.

* Type preserving: for things like testing, where one has to describe the
  expected form of a BSON document, it's helpful to be able to precisely
  specify expected types.  In particular, numeric types need to differentiate
  between Int32, Int64 and Double forms. This output in produced with
  :const:`~bson.json_util.CANONICAL_JSON_OPTIONS`.


Example usage (deserialization)
-------------------------------

.. doctest::

   >>> from bson.json_util import loads
   >>> loads('[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$scope": {}, "$code": "function x() { return 1; }"}}, {"bin": {"$type": "00", "$binary": "AQIDBA=="}}]')
   [{u'foo': [1, 2]}, {u'bar': {u'hello': u'world'}}, {u'code': Code('function x() { return 1; }', {})}, {u'bin': Binary('...', 0)}]


Example usage (with :const:`~bson.json_util.RELAXED_JSON_OPTIONS`)
------------------------------------------------------------------

.. doctest::

   >>> import datetime
   >>> from bson import Binary, Code, Int64, Regex
   >>> from bson.json_util import dumps, RELAXED_JSON_OPTIONS
   >>> def dumps_relaxed(obj):
   ...    return dumps(obj, json_options=RELAXED_JSON_OPTIONS)
   >>> dumps_relaxed({'foo': [1, 2.5, Int64(3)]})
   '{"foo": [1, 2.5, 3]}'
   >>> dumps_relaxed({'bar': {'hello': 'world'}})
   '{"bar": {"hello": "world"}}'
   >>> dumps_relaxed({'datePostEpoch': datetime.datetime(1970, 1, 1)})
   '{"datePostEpoch": {"$date": "1970-01-01T00:00:00Z"}}'
   >>> dumps_relaxed({'datePreEpoch': datetime.datetime(1969, 12, 31)})
   '{"datePreEpoch": {"$date": {"$numberLong": "-86400000"}}}'
   >>> dumps_relaxed({'bin': Binary(b"\x01\x02\x03\x04")})
   '{"bin": {"$binary": {"base64": "AQIDBA==", "subType": "00"}}}'


Example usage (with :const:`~bson.json_util.CANONICAL_JSON_OPTIONS`)
--------------------------------------------------------------------

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps, CANONICAL_JSON_OPTIONS
   >>> dumps([{'foo': [1, 2]},
   ...        {'bar': {'hello': 'world'}},
   ...        {'code': Code("function x() { return 1; }")},
   ...        {'bin': Binary(b"\x01\x02\x03\x04")}],
   ...       json_options=CANONICAL_JSON_OPTIONS)
   '[{"foo": [{"$numberInt": "1"}, {"$numberInt": "2"}]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }"}}, {"bin": {"$binary": {"base64": "AQIDBA==", "subType": "00"}}}]'

Example usage (with :const:`~bson.json_util.LEGACY_JSON_OPTIONS`)
-----------------------------------------------------------------

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps, LEGACY_JSON_OPTIONS
   >>> dumps([{'foo': [1, 2]},
   ...        {'bar': {'hello': 'world'}},
   ...        {'code': Code("function x() { return 1; }")},
   ...        {'bin': Binary(b"\x01\x02\x03\x04")}],
   ...       json_options=LEGACY_JSON_OPTIONS)
   '[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }"}}, {"bin": {"$binary": "AQIDBA==", "$type": "00"}}]'


:class:`~bson.json_util.JSONOptions` provides a way to control how JSON
is emitted and parsed, with the default being the legacy PyMongo format.

:const:`~bson.json_util.RELAXED_JSON_OPTIONS` is recommended is when your
application needs to generate JSON that is not aware of MongoDB
`Extended JSON`_. For example, an application that serves BSON documents
to a web application might not want the "$numberInt", "$numberLong", and
"$numberDouble" type wrappers. RELAXED_JSON_OPTIONS


:const:`~bson.json_util.CANONICAL_JSON_OPTIONS` is recommended when your
application needs to preserve BSON types when round-tripped through JSON.


:const:`~bson.json_util.LEGACY_JSON_OPTIONS` is recommended when your
application needs to preserve BSON types when round-tripped through JSON.


:mod:`~bson.json_util` can also generate Canonical or Relaxed `Extended JSON`_
when :const:`~bson.json_util.CANONICAL_JSON_OPTIONS` or
:const:`~bson.json_util.RELAXED_JSON_OPTIONS` is provided, respectively.

.. _Extended JSON: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst


Conversion table
----------------

+--------------------+----------------------------------------------------------+------------------------------------------------+
|**Python Type**     |**Canonical Extended JSON Format**                        |**Relaxed Extended JSON Format**                |
+====================+==========================================================+================================================+
|ObjectId            |{"$oid": <ObjectId bytes as 24-character, big-endian *hex | <Same as Canonical>                            |
|                    |string*>}                                                 |                                                |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|str                 |*string*                                                  | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|int                 |{"$numberInt": <32-bit signed integer as a *string*>}     | *integer*                                      |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Int64               |{"$numberLong": <64-bit signed integer as a *string*>}    | *integer*                                      |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|float \[finite\]    |{"$numberDouble": <64-bit signed floating point as a      | *non-integer*                                  |
|                    |decimal *string*>}                                        |                                                |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|float               |{"$numberDouble": <One of the *strings*: "Infinity",      | <Same as Canonical>                            |
|\[non-finite\]      |"-Infinity", or "NaN">}                                   |                                                |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Decimal128          |{"$numberDecimal": <decimal as a *string*>} [#]_          | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Binary              |{"$binary": {"base64": <base64-encoded (with padding as   | <Same as Canonical>                            |
|                    |``=``) payload as a *string*>, "subType": <BSON binary    |                                                |
|                    |type as a one- or two-character *hex string*>}}           |                                                |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Code                |{"$code": *string*}                                       | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Code (with scope)   |{"$code": *string*, "$scope": *Document*}                 | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Document            |*object* (with Extended JSON extensions)                  | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Timestamp           |{"$timestamp": {"t": *pos-integer*, "i": *pos-integer*}}  | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Regex               |{"$regularExpression": {pattern: *string*,                | <Same as Canonical>                            |
|                    |"options": <BSON regular expression options as a *string* |                                                |
|                    |or "" [#]_>}}                                             |                                                |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|datetime            |{"$date": {"$numberLong": <64-bit signed integer          | {"$date": <ISO-8601 Internet Date/Time Format  |
|\[year from 1970    |giving millisecs relative to the epoch, as a *string*>}}  | as decribed in RFC-3339 [#]_ with maximum time |
|to 9999 inclusive\] |                                                          | precision of milliseconds [#]_ as a *string*>} |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|datetime            |{"$date": {"$numberLong": <64-bit signed integer          | <Same as Canonical>                            |
|\[year before 1970  |giving millisecs relative to the epoch, as a *string*>}}  |                                                |
|or after 9999\]     |                                                          |                                                |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|DBRef [#]_          |{"$ref": <collection name as a *string*>, "$id":          | <Same as Canonical>                            |
|                    |<Extended JSON for the id>}                               |                                                |
|                    |                                                          |                                                |
|                    |If the generator supports DBRefs with a database          |                                                |
|                    |component, and the database component is nonempty:        |                                                |
|                    |                                                          |                                                |
|                    |{"$ref": <collection name as a *string*>,                 |                                                |
|                    | "$id": <Extended JSON for the id>,                       |                                                |
|                    | "$db": <database name as a *string*>}                    |                                                |
|                    |                                                          |                                                |
|                    |DBRefs may also have other fields that do not begin with  |                                                |
|                    |``$``, which MUST appear after ``$id`` and ``$db`` (if    |                                                |
|                    |supported).                                               |                                                |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|MinKey              |{"$minKey": 1}                                            | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|MaxKey              |{"$maxKey": 1}                                            | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|Array               |*array*                                                   | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|bool                |*true* or *false*                                         | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+
|None                |*null*                                                    | <Same as Canonical>                            |
+--------------------+----------------------------------------------------------+------------------------------------------------+

.. [#] This MUST conform to the `Decimal128 specification`_

.. [#] BSON Regular Expression options MUST be in alphabetical order.

.. [#] See https://docs.mongodb.com/manual/reference/glossary/#term-namespace

.. [#] See https://tools.ietf.org/html/rfc3339#section-5.6

.. [#] Fractional seconds SHOULD have exactly 3 decimal places if the fractional part
   is non-zero.  Otherwise, fractional seconds SHOULD be omitted if zero.

.. [#] See https://docs.mongodb.com/manual/reference/database-references/#dbrefs

.. _Decimal128 specification: https://github.com/mongodb/specifications/blob/master/source/bson-decimal128/decimal128.rst#writing-to-extended-json

