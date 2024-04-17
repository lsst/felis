import dataclasses
from enum import Enum, StrEnum, auto


class LengthSupport(Enum):
    """Enum for length support in a type system."""

    UNSUPPORTED = auto()
    OPTIONAL = auto()
    REQUIRED = auto()


class TypeCategory(StrEnum):
    """Enum for the category of a type in a type system."""

    INTEGER = auto()
    FLOATING_POINT = auto()
    STRING = auto()
    BOOLEAN = auto()
    DATETIME = auto()
    BINARY = auto()
    TIMESTAMP = auto()


@dataclasses.dataclass
class TypeAttributes:
    """Attributes of a type in a type system.

    These are intended to be immutable characteristics. Attributes which may
    change, such as length, should be defined in the containing type object.
    """

    category: TypeCategory
    length_support: LengthSupport = LengthSupport.UNSUPPORTED
    properties: dict[str, type[bool | str | float | int]] = dataclasses.field(default_factory=dict)

    @property
    def is_length_supported(self) -> bool:
        """Check if length is supported by the type."""
        return self.length_support in (LengthSupport.OPTIONAL, LengthSupport.REQUIRED)

    @property
    def is_length_required(self) -> bool:
        """Check if length is required by the type."""
        return self.length_support is LengthSupport.REQUIRED


@dataclasses.dataclass
class BooleanAttributes(TypeAttributes):
    """Attributes of a boolean type in a type system."""

    def __init__(self) -> None:
        super().__init__(category=TypeCategory.BOOLEAN)


@dataclasses.dataclass
class FloatingPointAttributes(TypeAttributes):
    """Attributes of a boolean type in a type system."""

    def __init__(self) -> None:
        super().__init__(category=TypeCategory.FLOATING_POINT, properties={"precision": int, "scale": int})


@dataclasses.dataclass
class IntegerAttributes(TypeAttributes):
    """Attributes of a boolean type in a type system."""

    def __init__(self) -> None:
        super().__init__(category=TypeCategory.INTEGER)


@dataclasses.dataclass
class BinaryAttributes(TypeAttributes):
    """Attributes of a boolean type in a type system."""

    def __init__(self) -> None:
        super().__init__(category=TypeCategory.BINARY)


@dataclasses.dataclass
class StringAttributes(TypeAttributes):
    """Attributes of a string type in a type system."""

    def __init__(self) -> None:
        super().__init__(category=TypeCategory.STRING, length_support=LengthSupport.OPTIONAL)


@dataclasses.dataclass
class TimestampAttributes(TypeAttributes):
    """Attributes of a timestamp type in a type system."""

    def __init__(self) -> None:
        super().__init__(category=TypeCategory.TIMESTAMP, properties={"precision": int, "timezone": bool})


# Mapping of the default type names to their natural attributes.
DEFAULT_TYPES: dict[str, type[TypeAttributes]] = {
    "binary": BinaryAttributes,
    "boolean": BooleanAttributes,
    "byte": IntegerAttributes,
    "char": StringAttributes,
    "datetime": TimestampAttributes,
    "double": FloatingPointAttributes,
    "float": FloatingPointAttributes,
    "int": IntegerAttributes,
    "long": IntegerAttributes,
    "short": IntegerAttributes,
    "string": StringAttributes,
    "text": StringAttributes,
    "timestamp": TimestampAttributes,
    "unicode": StringAttributes,
}
