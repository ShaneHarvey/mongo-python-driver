from __future__ import annotations

from test.asynchronous import async_setup, async_teardown

import pytest_asyncio

_IS_SYNC = False


@pytest_asyncio.fixture(scope="session", autouse=True)
async def test_setup_and_teardown():
    await async_setup()
    yield
    await async_teardown()


def pytest_collection_modifyitems(items, config):
    for item in items:
        if not any(item.iter_markers()):
            item.add_marker("default")
