import datetime
from typing import Any, Callable, Iterable, NamedTuple, Optional, Union

class TypeEncoder:
    @property
    def python_type(self) -> Any: ...
    def transform_python(self, value: Any) -> Any: ...

class TypeDecoder:
    @property
    def bson_type(self) -> Any: ...
    def transform_bson(self, value: Any) -> Any: ...

class TypeCodec(TypeEncoder, TypeDecoder): ...
Codec = Union[TypeEncoder, TypeDecoder, TypeCodec]
Fallback = Callable[[Any], Any]

class TypeRegistry:
    def __init__(self, type_codecs: Optional[Iterable[Codec]]=..., fallback_encoder: Optional[Fallback]=...) -> None: ...

_options_base = NamedTuple('CodecOptions', [
    ('document_class', type),
    ('tz_aware', bool),
    ('uuid_representation', int),
    ('unicode_decode_error_handler', str),
    ('tzinfo', Optional[datetime.tzinfo]),
    ('type_registry', TypeRegistry)
])

class CodecOptions(_options_base):
    def __new__(cls: Any, document_class: type=..., tz_aware: bool=..., uuid_representation: Optional[int]=..., unicode_decode_error_handler: str=..., tzinfo: Optional[datetime.tzinfo]=..., type_registry: Optional[TypeRegistry]=...) -> CodecOptions: ...
    def with_options(self, **kwargs: Any) -> CodecOptions: ...

DEFAULT_CODEC_OPTIONS: CodecOptions
