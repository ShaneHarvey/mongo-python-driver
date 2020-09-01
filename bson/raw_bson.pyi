from bson.codec_options import CodecOptions
from typing import Dict, Optional, TypeVar

_Key = TypeVar('_Key')
_Value = TypeVar('_Value')

class RawBSONDocument(Dict[_Key, _Value]):
    def __init__(self, bson_bytes: bytes, codec_options: Optional[CodecOptions]=...) -> None: ...
    @property
    def raw(self) -> bytes: ...

DEFAULT_RAW_BSON_OPTIONS: CodecOptions
