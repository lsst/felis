import dataclasses
from enum import Enum, auto


class LengthSupport(Enum):
    """Enum for length support in a type system."""

    UNSUPPORTED = auto()
    OPTIONAL = auto()
    REQUIRED = auto()


# TODO: Add support for type parameters such as 'precision' and 'scale' on
# numeric types
@dataclasses.dataclass
class TypeProperties:
    """Properties of a type in a type system."""

    length_support: LengthSupport = LengthSupport.UNSUPPORTED
    is_numeric: bool = False
    is_string: bool = False
    is_boolean: bool = False
    is_datetime: bool = False
    is_binary: bool = False

    @property
    def is_length_supported(self) -> bool:
        """Check if length is supported by the type."""
        return self.length_support in (LengthSupport.OPTIONAL, LengthSupport.REQUIRED)

    @property
    def is_length_required(self) -> bool:
        """Check if length is required by the type."""
        return self.length_support == LengthSupport.REQUIRED


class CharProperties(TypeProperties):
    """Properties of a char type in a type system."""

    def __init__(self) -> None:
        super().__init__(length_support=LengthSupport.REQUIRED, is_string=True)


@dataclasses.dataclass
class StringProperties(TypeProperties):
    """Properties of a string type in a type system."""

    def __init__(self) -> None:
        super().__init__(length_support=LengthSupport.OPTIONAL, is_string=True)


@dataclasses.dataclass
class NumericProperties(TypeProperties):
    """Properties of a numeric type in a type system."""

    def __init__(self) -> None:
        super().__init__(is_numeric=True)


@dataclasses.dataclass
class BooleanProperties(TypeProperties):
    """Properties of a boolean type in a type system."""

    def __init__(self) -> None:
        super().__init__(is_boolean=True)


@dataclasses.dataclass
class TimestampProperties(TypeProperties):
    """Properties of a timestamp type in a type system."""

    def __init__(self) -> None:
        super().__init__(length_support=LengthSupport.OPTIONAL, is_datetime=True)


@dataclasses.dataclass
class DatetimeProperties(TypeProperties):
    """Properties of a datetime type in a type system."""

    def __init__(self) -> None:
        super().__init__(length_support=LengthSupport.OPTIONAL, is_datetime=True)


@dataclasses.dataclass
class BinaryProperties(TypeProperties):
    """Properties of a binary type in a type system."""

    def __init__(self) -> None:
        super().__init__(is_binary=True)


@dataclasses.dataclass
class FloatingPointProperties(NumericProperties):
    """Properties of a floating point type in a type system."""

    def __init__(self) -> None:
        super().__init__()


class IntegerProperties(NumericProperties):
    """Properties of an integer type in a type system."""

    def __init__(self) -> None:
        super().__init__()


DEFAULT_TYPES: dict[str, TypeProperties] = {
    "binary": BinaryProperties(),
    "boolean": BooleanProperties(),
    "byte": IntegerProperties(),
    "char": CharProperties(),
    "datetime": TimestampProperties(),
    "double": FloatingPointProperties(),
    "float": FloatingPointProperties(),
    "int": IntegerProperties(),
    "long": IntegerProperties(),
    "short": IntegerProperties(),
    "string": StringProperties(),
    "text": StringProperties(),
    "timestamp": TimestampProperties(),
    "unicode": StringProperties(),
}
