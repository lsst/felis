"""Get SQLAlchemy dialects and their type modules."""

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

from collections.abc import Mapping
from types import MappingProxyType, ModuleType

from sqlalchemy import dialects
from sqlalchemy.engine import Dialect
from sqlalchemy.engine.mock import create_mock_engine

from .sqltypes import MYSQL, POSTGRES, SQLITE

__all__ = ["get_supported_dialects", "get_dialect_module"]

_DIALECT_NAMES = (MYSQL, POSTGRES, SQLITE)
"""List of supported dialect names.

This list is used to create the dialect and module dictionaries.
"""


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
