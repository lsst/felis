from abc import ABC
from typing import Any

from .properties import TypeProperties

__all__ = ["AbstractType", "AbstractTypes"]


class AbstractType(ABC):
    """Abstract class for a type in the type system."""

    type_properties: TypeProperties

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the type."""
        length = kwargs.pop("length", None)
        self._length: int | None
        if length is not None and not isinstance(length, int):
            raise TypeError("Length must be an integer.")
        if length is None and self.type_properties.is_length_required:
            raise ValueError("Length is required for this type.")
        if length is not None and self.type_properties.is_length_supported:
            self._length = length
        else:
            self._length = None
        self._type_properties = self.type_properties
        self._type_parameters = kwargs

    @property
    def length(self) -> int | None:
        """Return the length of the type."""
        return self._length

    @property
    def type_object(self) -> Any:
        """Return a specific type object for the type system."""
        return None

    @property
    def type_string(self) -> str:
        """Return a string representation of the type."""
        return ""

    @property
    def type_parameters(self) -> dict[str, Any]:
        """Return the type parameters of the type."""
        return self._type_parameters

    def __str__(self) -> str:
        """Return a string representation of the type."""
        return self.type_string


class TypeFactory:
    """Abstract factory for creating type classes."""

    def __init__(self, base_class: type[AbstractType]):
        self.base_class = base_class

    def create(self, type_properties: type[TypeProperties], **kwargs: Any) -> type[AbstractType]:
        """Create a type class with the given properties."""

        class _Type(self.base_class):  # type: ignore[name-defined]
            """Type class with the given properties."""

            pass

        _Type.type_properties = type_properties()
        for key, value in kwargs.items():
            setattr(_Type, key, value)
        return _Type


class AbstractTypes(ABC):
    """Abstract base class for a type system."""

    type_classes: dict[str, type[AbstractType]] = {}
    """Map of type names to type classes.

    Notes
    -----
    Subclasses need to redefine this variable to include mappings for all their
    valid types. These should ideally match those types defined in
    the `~felis.types.simple` module for consistency, though it is possible
    some type systems would not provide bindings for all available types.
    """

    def create(self, type_name: str, **kwargs: dict[str, Any]) -> AbstractType:
        if type_name not in self.type_classes:
            raise ValueError(f"Unknown type name: {type_name}")
        return self.type_classes[type_name.lower()](**kwargs)

    def available_types(self) -> list[str]:
        """Return a list of available type names."""
        return list(self.type_classes.keys())
