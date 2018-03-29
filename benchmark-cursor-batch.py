import sys
# import flamegraph
import bson
import timeit
from pymongo import MongoClient

mongo_client = MongoClient()
db = mongo_client.test
coll = db.test

fields = (
    "aaa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|ab|ac|ad|ae|af|ag|ah|ai|aj"
    "|ak|al|am|an|ao|ap|aq|ar|as|at|au|av|aw|ax|ay|az|ba|bb|bc|bd|be|bf|bg"
    "|bh|bi|bj|bk|bl|bm|bn|bo|bp|bq|br|bs|bt|bu|bv|bw|bx|by|bz|ca|cb|cc|cd"
    "|ce|cf|cg|ch|ci|cj|ck|cl|cm|cn|co|cp|cq|cr").split('|')
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


def benchmark():
    def iterate_cursor():
        cursor = coll.find().limit(30000).batch_size(100)
        rowCount = 0

        for _ in cursor:
            rowCount += 1
        print("Finished after {} entries".format(rowCount))

    time(iterate_cursor)


def time(fn):
    """Measure how long it takes to call fn 1 times, take best of 3 trials."""
    print('%s %7.2f' % (fn.__name__, min(timeit.Timer(fn).repeat(3, number=1))))


def benchmark_decoding(limit, batch_size):
    global fields, vals
    batches = []
    items = [('_id', bson.ObjectId())]
    items.extend(zip(fields, vals))

    def add_batch(num_docs):
        items[0] = '_id', bson.ObjectId()
        docs = [bson.SON(items) for _ in range(num_docs)]
        batch = {
            'ok': 1,
            'cursor': {
                'id': bson.Int64(1),
                'ns': 'test.test',
                'nextBatch': docs,
            }
        }
        batches.append(bson.BSON.encode(batch))

    first_batch_size = min(batch_size, 101)
    limit -= first_batch_size
    add_batch(first_batch_size)
    for _ in range(int(limit/batch_size)):
        add_batch(batch_size)
    remaining_docs = limit % batch_size
    if remaining_docs:
        add_batch(remaining_docs)

    # benchmark
    def decode_batches():
        for batch in batches:
            command_response = bson.decode_all(batch)[0]
            # print(len(command_response['cursor']['nextBatch']))

    print('bench limit: %s, batch_size: %s' % (limit, batch_size))
    time(decode_batches)


def load_data():
    global fields, vals
    coll.drop()
    for i in range(3):
        docs = [bson.SON(zip(fields, vals)) for _ in range(10000)]
        coll.insert_many(docs)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        func = sys.argv[1]
        if func == "benchmark_decoding":
            benchmark_decoding(30000, 100)
            benchmark_decoding(30000, 10000)
            benchmark_decoding(30000, 100000)
        elif func == "load_data":
            load_data()
        else:
            print('unknown usage: ', sys.argv[1:])
    else:
        benchmark()
