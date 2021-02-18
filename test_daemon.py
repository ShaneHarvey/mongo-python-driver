from pymongo.daemon import _spawn_daemon
import sys


with open('_test_daemon.log', mode='w') as f:
    d = _spawn_daemon(['sleep', '5'])
    print('Spawned: ', d)
import time
time.sleep(10)
