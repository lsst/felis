from typing import Any

from .abstract import AbstractType, AbstractTypes, TypeFactory
from .properties import (
    BinaryProperties,
    BooleanProperties,
    NumericProperties,
    StringProperties,
    TimestampProperties,
)

__all__ = ["SimpleType", "SimpleTypes"]


class SimpleType(AbstractType):
    """Abstract type in the simple type system."""

    _type_string: str
    """Type string for the type, which can be defined statically for simple
    types"""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the simple type."""
        super().__init__(**kwargs)

    @property
    def type_string(self) -> str:
        return self.__class__._type_string


class SimpleTypeFactory(TypeFactory):
    """Abstract factory for creating types."""

    def __init__(self) -> None:
        super().__init__(SimpleType)


_TF = SimpleTypeFactory()


class SimpleTypes(AbstractTypes):
    """Implementation of a simple type system."""

    type_classes = {
        "binary": _TF.create(BinaryProperties, _type_string="binary"),
        "boolean": _TF.create(BooleanProperties, _type_string="bool"),
        "byte": _TF.create(NumericProperties, _type_string="byte"),
        "char": _TF.create(StringProperties, _type_string="char"),
        "datetime": _TF.create(TimestampProperties, _type_string="datetime"),
        "double": _TF.create(NumericProperties, _type_string="double"),
        "float": _TF.create(NumericProperties, _type_string="float"),
        "int": _TF.create(NumericProperties, _type_string="int"),
        "long": _TF.create(NumericProperties, _type_string="long"),
        "short": _TF.create(NumericProperties, _type_string="short"),
        "string": _TF.create(StringProperties, _type_string="string"),
        "text": _TF.create(StringProperties, _type_string="text"),
        "timestamp": _TF.create(TimestampProperties, _type_string="timestamp"),
        "unicode": _TF.create(StringProperties, _type_string="unicode"),
    }
