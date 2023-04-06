import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure

client = MongoClient()
print(f"Server version: {client.server_info()['version']}")
coll = client.test.snapshot


def run_bench():
    coll.drop()
    coll.insert_one({"x": 1})
    start = time.time()
    while not snapshot_contains({"x": 1}):
        print("waiting")
        pass
    duration = time.time() - start
    print(f"waiting for snapshot took: {duration} seconds")
    return duration


def snapshot_contains(filter):
    """Wait for a document to be present in the latest snapshot."""
    with coll.database.client.start_session(snapshot=True) as s:
        try:
            if coll.find_one(filter, session=s):
                return True
            return False
        except OperationFailure as e:
            # Retry them as the server demands...
            if e.code == 246:  # SnapshotUnavailable
                return False
            raise


def main():
    durations = [run_bench() for _ in range(50)]
    print(f"Average: {(sum(durations)*1000)/len(durations)} milliseconds")


if __name__ == "__main__":
    main()
