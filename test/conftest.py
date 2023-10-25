from __future__ import annotations

import os
import threading
from test import setup, teardown

import pytest


class ProfileSystemThread(threading.Thread):
    def __init__(self):
        super().__init__(name="profile_system_with_top")
        self.daemon = True
        self.stopped = threading.Event()

    def stop(self):
        self.stopped.set()

    def run(self):
        while not self.stopped.is_set():
            ret = os.system("top -n15 -o cpu -l 2 | tail -n 27 >> top-output.txt")
            if ret:
                return
            ret = os.system("top -n15 -o mem -l 1 >> top-output.txt")
            if ret:
                return
            self.stopped.wait(30)


@pytest.fixture(scope="session", autouse=True)
def test_setup_and_teardown():
    thread = ProfileSystemThread()
    thread.start()
    setup()
    yield
    thread.stop()
    thread.join(10)
    teardown()
