import datetime
from typing import Any, Union

UPPERBOUND: int

class Timestamp:
    def __init__(self, time: Union[datetime.datetime, int], inc: int) -> None: ...
    @property
    def time(self) -> int: ...
    @property
    def inc(self) -> int: ...
    def __eq__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    def __ne__(self, other: Any) -> bool: ...
    def __lt__(self, other: Any) -> bool: ...
    def __le__(self, other: Any) -> bool: ...
    def __gt__(self, other: Any) -> bool: ...
    def __ge__(self, other: Any) -> bool: ...
    def as_datetime(self) -> datetime.datetime: ...