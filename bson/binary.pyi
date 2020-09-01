from typing import Any, Dict, Iterable, Tuple
from uuid import UUID

BINARY_SUBTYPE: int
FUNCTION_SUBTYPE: int
OLD_BINARY_SUBTYPE: int
OLD_UUID_SUBTYPE: int
UUID_SUBTYPE: int
STANDARD: int
PYTHON_LEGACY: int
JAVA_LEGACY: int
CSHARP_LEGACY: int
ALL_UUID_SUBTYPES: Iterable[int]
ALL_UUID_REPRESENTATIONS: Iterable[int]
UUID_REPRESENTATION_NAMES: Dict[int, str]
MD5_SUBTYPE: int
USER_DEFINED_SUBTYPE: int

class Binary(bytes):
    def __new__(cls: Any, data: bytes, subtype: int=...) -> Binary: ...
    @property
    def subtype(self) -> int: ...
    def __getnewargs__(self) -> Tuple[bytes, int]: ...  # type: ignore
    def __eq__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    def __ne__(self, other: Any) -> bool: ...

class UUIDLegacy(Binary):
    def __new__(cls: Any, obj: UUID) -> UUIDLegacy: ...
    def __getnewargs__(self) -> Tuple[UUID]: ...  # type: ignore
    @property
    def uuid(self) -> UUID: ...
