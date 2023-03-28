import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor

from bson import Int64
from pymongo import MongoClient
from pymongo.results import InsertOneResult

WORKERS = 100
BATCH = 300

LOCK = threading.Lock()
BUFFER = {}


def insert_auto(coll):
    """HELP-43739/DRIVERS-2582 Quick implementation of insert coalescing.

    Note doesn't handle sessions, writeConcern, comment, bypassDocumentValidation, etc...
    """
    global BUFFER
    f = Future()
    sleep = True
    follower = False
    with LOCK:
        BUFFER[f] = gen_doc()
        if len(BUFFER) >= 50:
            sleep = False
            # Leader fast path
            batch = BUFFER
            BUFFER = {}
    if sleep:
        # Briefly sleep to allow other threads to append to the buffer.
        time.sleep(0.001)
        with LOCK:
            if f in BUFFER:
                # Leader
                batch = BUFFER
                BUFFER = {}
            else:
                follower = True
    if follower:
        return f.result()
    futures = list(batch.keys())
    documents = list(batch.values())
    try:
        res = coll.insert_many(documents, ordered=False)
    except BaseException as exc:
        for i, f in enumerate(futures):
            f.set_exception(exc)
        raise
    for i, f in enumerate(futures):
        f.set_result(InsertOneResult(res.inserted_ids[i], res.acknowledged))
    return f.result()


def gen_doc():
    return {
        "_id": uuid.uuid4(),
        "zoneKey": 4,
        "occurred_on": Int64("4000266376"),
        "playhead": 133,
        "runtime": 938,
        "alid": uuid.uuid4(),
        "Is_cleared": True,
    }


def insert_one(coll):
    return coll.insert_one(gen_doc())


def insert_many(coll):
    return coll.insert_many([gen_doc() for _ in range(1000)])


def bench(w, method):
    with MongoClient(
        "mongodb://user:password@localhost:27017",
        w=w,
        uuidRepresentation="standard",
        serverSelectionTimeoutMS=3000,
        tls=True,
        tlscertificatekeyfile="/Users/shane/git/mongo-python-driver/test/certificates/client.pem",
        tlscafile="/Users/shane/git/mongo-python-driver/test/certificates/ca.pem",
    ) as client:
        coll = client.test.test
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            for f in [executor.submit(method, coll) for _ in range(BATCH)]:
                f.result()
            init_count = coll.estimated_document_count()
            start = time.time()
            while time.time() - start < 5:
                for f in [executor.submit(method, coll) for _ in range(BATCH)]:
                    f.result()
        duration = time.time() - start
        end_count = coll.estimated_document_count()
        ninserts = end_count - init_count
    print(
        f"Bench {method.__name__} inserts/sec: {int(ninserts/duration): > 6}, w={w}, workers={WORKERS}"
    )


def main():
    for method in [insert_one, insert_auto, insert_many]:
        for w in [1, "majority"]:
            bench(w=w, method=method)


if __name__ == "__main__":
    main()

"""
# Running local 3 member replica set auth+tls MongoDB 6.0.4
(pymongo-py311) ➜  mongo-python-driver git:(HELP-43739-coalesce-insert_one) ✗ python bench-insert.py
Bench insert_one inserts/sec:   5375, w=1, workers=100
Bench insert_one inserts/sec:   1510, w=majority, workers=100
Bench insert_auto inserts/sec:  14126, w=1, workers=100
Bench insert_auto inserts/sec:   2091, w=majority, workers=100
Bench insert_many inserts/sec:  48673, w=1, workers=100
Bench insert_many inserts/sec:  48854, w=majority, workers=100
"""
