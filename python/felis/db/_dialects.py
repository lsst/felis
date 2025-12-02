"""Utilities for accessing SQLAlchemy dialects and their type modules."""

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

import re
from collections.abc import Mapping
from types import MappingProxyType, ModuleType

from sqlalchemy import dialects, types
from sqlalchemy.engine import Dialect
from sqlalchemy.engine.mock import create_mock_engine
from sqlalchemy.types import TypeEngine

from ._sqltypes import MYSQL, POSTGRES, SQLITE

__all__ = ["get_dialect_module", "get_supported_dialects", "string_to_typeengine"]

_DIALECT_NAMES = (MYSQL, POSTGRES, SQLITE)
"""List of supported dialect names.

This list is used to create the dialect and module dictionaries.
"""

_DATATYPE_REGEXP = re.compile(r"(\w+)(\((.*)\))?")
"""Regular expression to match data types with parameters in parentheses."""


def _dialect(dialect_name: str) -> Dialect:
    """Create the SQLAlchemy dialect for the given name using a mock engine.

    Parameters
    ----------
    dialect_name
        The name of the dialect to create.

    Returns
    -------
    `~sqlalchemy.engine.Dialect`
        The SQLAlchemy dialect.
    """
    return create_mock_engine(f"{dialect_name}://", executor=None).dialect


_DIALECTS = MappingProxyType({name: _dialect(name) for name in _DIALECT_NAMES})
"""Dictionary of dialect names to SQLAlchemy dialects."""


def get_supported_dialects() -> Mapping[str, Dialect]:
    """Get a dictionary of the supported SQLAlchemy dialects.

    Returns
    -------
    `dict` [ `str`, `~sqlalchemy.engine.Dialect`]
        A dictionary of the supported SQLAlchemy dialects.

    Notes
    -----
    The dictionary is keyed by the dialect name and the value is the SQLAlchemy
    dialect object. This function is intended as the primary interface for
    getting the supported dialects.
    """
    return _DIALECTS


def _dialect_module(dialect_name: str) -> ModuleType:
    """Get the SQLAlchemy dialect module for the given name.

    Parameters
    ----------
    dialect_name
        The name of the dialect module to get from the SQLAlchemy package.
    """
    return getattr(dialects, dialect_name)


_DIALECT_MODULES = MappingProxyType({name: _dialect_module(name) for name in _DIALECT_NAMES})
"""Dictionary of dialect names to SQLAlchemy modules."""


def get_dialect_module(dialect_name: str) -> ModuleType:
    """Get the SQLAlchemy dialect module for the given name.

    Parameters
    ----------
    dialect_name
        The name of the dialect module to get from the SQLAlchemy package.

    Returns
    -------
    `~types.ModuleType`
        The SQLAlchemy dialect module.

    Raises
    ------
    ValueError
        Raised if the dialect name is not supported.
    """
    if dialect_name not in _DIALECT_MODULES:
        raise ValueError(f"Unsupported dialect: {dialect_name}")
    return _DIALECT_MODULES[dialect_name]


def string_to_typeengine(
    type_string: str, dialect: Dialect | None = None, length: int | None = None
) -> TypeEngine:
    """Convert a string representation of a datatype to a SQLAlchemy type.

    Parameters
    ----------
    type_string
        The string representation of the data type.
    dialect
        The SQLAlchemy dialect to use. If None, the default dialect will be
        used.
    length
        The length of the data type. If the data type does not have a length
        attribute, this parameter will be ignored.

    Returns
    -------
    `sqlalchemy.types.TypeEngine`
        The SQLAlchemy type engine object.

    Raises
    ------
    ValueError
        Raised if the type string is invalid or the type is not supported.

    Notes
    -----
    This function is used when converting type override strings defined in
    fields such as ``mysql:datatype`` in the schema data.
    """
    match = _DATATYPE_REGEXP.search(type_string)
    if not match:
        raise ValueError(f"Invalid type string: {type_string}")

    type_name, _, params = match.groups()
    if dialect is None:
        type_class = getattr(types, type_name.upper(), None)
    else:
        try:
            dialect_module = get_dialect_module(dialect.name)
        except KeyError:
            raise ValueError(f"Unsupported dialect: {dialect}")
        type_class = getattr(dialect_module, type_name.upper(), None)

    if not type_class:
        raise ValueError(f"Unsupported type: {type_name.upper()}")

    if params:
        params = [int(param) if param.isdigit() else param for param in params.split(",")]
        type_obj = type_class(*params)
    else:
        type_obj = type_class()

    if hasattr(type_obj, "length") and getattr(type_obj, "length") is None and length is not None:
        type_obj.length = length

    return type_obj
