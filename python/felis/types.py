"""Define the supported Felis datatypes."""

# This file is part of felis.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from typing import Any

__all__ = [
    "FelisType",
    "Boolean",
    "Byte",
    "Short",
    "Int",
    "Long",
    "Float",
    "Double",
    "Char",
    "String",
    "Unicode",
    "Text",
    "Binary",
    "Timestamp",
]


class FelisType:
    """Base class for a representation of Felis column types.

    Notes
    -----
    This class plays a role of a metaclass without being an actual metaclass.
    It provides a method to retrieve a class (type) given Felis type name.
    There should be no instances of this class (or sub-classes), the utility
    of the class hierarchy is in the type system itself.
    """

    felis_name: str
    """Name of the type as defined in the Felis schema."""

    votable_name: str
    """Name of the type as defined in VOTable."""

    is_numeric: bool
    """Flag indicating if the type is numeric."""

    is_sized: bool
    """Flag indicating if the type is sized, meaning it requires a length."""

    is_timestamp: bool
    """Flag indicating if the type is a timestamp."""

    _types: dict[str, type[FelisType]] = {}
    """Dictionary of all known Felis types."""

    @classmethod
    def __init_subclass__(
        cls,
        /,
        felis_name: str,
        votable_name: str,
        is_numeric: bool = False,
        is_sized: bool = False,
        is_timestamp: bool = False,
        **kwargs: Any,
    ):
        """Register a new Felis type.

        Parameters
        ----------
        felis_name
            Name of the type.
        votable_name
            Name of the type as defined in VOTable.
        is_numeric
            Flag indicating if the type is numeric.
        is_sized
            Flag indicating if the type is sized.
        is_timestamp
            Flag indicating if the type is a timestamp.
        kwargs
            Additional keyword arguments.
        """
        super().__init_subclass__(**kwargs)
        cls.felis_name = felis_name
        cls.votable_name = votable_name
        cls.is_numeric = is_numeric
        cls.is_sized = is_sized
        cls.is_timestamp = is_timestamp
        cls._types[felis_name] = cls

    @classmethod
    def felis_type(cls, felis_name: str) -> type[FelisType]:
        """Return specific Felis type for a given name.

        Parameters
        ----------
        felis_name
            Name of the felis type as defined in felis schema.

        Returns
        -------
        `type` [ `FelisType` ]
            A specific Felis type class.

        Raises
        ------
        TypeError
            Raised if ``felis_name`` does not correspond to a known type.
        """
        try:
            return cls._types[felis_name]
        except KeyError:
            raise TypeError(f"Unknown felis type {felis_name!r}") from None


class Boolean(FelisType, felis_name="boolean", votable_name="boolean", is_numeric=False):
    """Felis definition of boolean type."""


class Byte(FelisType, felis_name="byte", votable_name="unsignedByte", is_numeric=True):
    """Felis definition of byte type."""


class Short(FelisType, felis_name="short", votable_name="short", is_numeric=True):
    """Felis definition of short integer type."""


class Int(FelisType, felis_name="int", votable_name="int", is_numeric=True):
    """Felis definition of integer type."""


class Long(FelisType, felis_name="long", votable_name="long", is_numeric=True):
    """Felis definition of long integer type."""


class Float(FelisType, felis_name="float", votable_name="float", is_numeric=True):
    """Felis definition of single precision floating point type."""


class Double(FelisType, felis_name="double", votable_name="double", is_numeric=True):
    """Felis definition of double precision floating point type."""


class Char(FelisType, felis_name="char", votable_name="char", is_sized=True):
    """Felis definition of character type."""


class String(FelisType, felis_name="string", votable_name="char", is_sized=True):
    """Felis definition of string type."""


class Unicode(FelisType, felis_name="unicode", votable_name="unicodeChar", is_sized=True):
    """Felis definition of unicode string type."""


class Text(FelisType, felis_name="text", votable_name="char"):
    """Felis definition of text type."""


class Binary(FelisType, felis_name="binary", votable_name="unsignedByte", is_sized=True):
    """Felis definition of binary type."""


class Timestamp(FelisType, felis_name="timestamp", votable_name="char", is_timestamp=True):
    """Felis definition of timestamp type."""
