from __future__ import annotations

import os
import sys
import time

import pymongo
from pymongo import network_layer

client = pymongo.MongoClient(os.getenv("URI_SRV"), w=1, heartbeatFrequencyMS=100000)
start = time.monotonic()
client.admin.command("ping")
print(f"Initialization RTT: {time.monotonic()-start} secs")
print(f"Python: {sys.version}")
print(f"MongoDB: {client.server_info()}")
SIZES = (1024 * 100, 1024 * 1024, (1024 * 1024 * 16) - 100)
coll = client.test.bench_recv
for size in SIZES:
    if coll.find_one({"_id": size}):
        continue
    coll.insert_one({"_id": size, "data": "x" * size})

start = time.monotonic()
client.admin.command("ping")
print(f"Ping RTT: {time.monotonic()-start} secs")

network_layer.DEBUG = True
for size in SIZES:
    print(f"\nTesting document size: {size} bytes")
    for i in range(3):
        print(f"find_one {i+1}")
        coll.find_one({"_id": size})
network_layer.DEBUG = False
client.close()
