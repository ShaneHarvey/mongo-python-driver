import sys
import timeit

from functools import partial

import bson
from pymongo import MongoClient


mongo_client = MongoClient()
db = mongo_client.test
coll = db.test


def iterate_cursor(limit, batch_size):
    cursor = coll.find(limit=limit, batch_size=batch_size)
    rowCount = 0
    for _ in cursor:
        rowCount += 1


def load_data():
    fields = (
        "aaa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|ab|ac|ad|ae|af|ag|ah|ai"
        "|aj|ak|al|am|an|ao|ap|aq|ar|as|at|au|av|aw|ax|ay|az|ba|bb|bc|bd|be|bf"
        "|bg|bh|bi|bj|bk|bl|bm|bn|bo|bp|bq|br|bs|bt|bu|bv|bw|bx|by|bz|ca|cb|cc"
        "|cd|ce|cf|cg|ch|ci|cj|ck|cl|cm|cn|co|cp|cq|cr").split('|')
    vals = (
        "xxxxxxxx|x|xxxxxxxx|xxxxxx|xxxx|xxxxxxxx|x||||xx|xxxxxxxxxx|||xxxx"
        "|xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|xxxx|xxxxxxxx|xxxxxxxx||x|xx|xx"
        "|xxxxxxxxxx|||xxxxxx|xxxx|xxxx|xxxx|xxxxxxxxxxxxxxx||xx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxxxx|xxxxx|xxxxxxxx|||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxxxxxxxxxxx|xxxxxx|||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxx|xxxxxxxx||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxxx|xxxxxxxx||xxxxxxxx|xxxx"
        "|xxxxxxxxxxxxxxxxxxxx|xxxxxx|xxxxxxxx|xxxxxxx||||||xxxx|xx|xxxxx"
        "|xxxxxxxxxxxxxx|xxxxxxxxxxxxxx||xx|xxxxx|xxxxxxxxx|xxxxxxxxxxxxx"
        "||xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|||||||||xx|xx").split('|')

    coll.drop()
    for i in range(3):
        docs = [bson.SON(zip(fields, vals)) for _ in range(10000)]
        coll.insert_many(docs)


def time(fn):
    """Measure how long it takes to call fn 1 times, take best of 3 trials."""
    return min(timeit.Timer(fn).repeat(3, number=1))


if __name__ == "__main__":
    print('bson.has_c(): %s' % (bson.has_c(),))
    if 'load_data' in sys.argv[1:]:
        print('loading data')
        load_data()

    print("%10s: %7s" % ("batch_size", "cursor iteration time"))
    for batch_size in (0, 10, 100, 1000, 5000, 10000, 100000, 1000000):
        t = time(partial(iterate_cursor, 30000, batch_size))
        print("%10s: %7.2f" % (batch_size, t))
