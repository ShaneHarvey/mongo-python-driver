from typing import Any

class MaxKey:
    def __eq__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    def __ne__(self, other: Any) -> bool: ...
    def __le__(self, other: Any) -> bool: ...
    def __lt__(self, dummy: Any) -> bool: ...
    def __ge__(self, dummy: Any) -> bool: ...
    def __gt__(self, other: Any) -> bool: ...