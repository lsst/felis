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

import logging
from types import ModuleType

from sqlalchemy import dialects
from sqlalchemy.engine import Dialect
from sqlalchemy.engine.mock import create_mock_engine

from .sqltypes import MYSQL, ORACLE, POSTGRES, SQLITE

logger = logging.getLogger(__name__)

_DIALECT_NAMES = [MYSQL, POSTGRES, SQLITE, ORACLE]


def _dialect(dialect_name: str) -> Dialect:
    """Create the SQLAlchemy dialect for the given name."""
    return create_mock_engine(f"{dialect_name}://", executor=None).dialect


_DIALECTS = {name: _dialect(name) for name in _DIALECT_NAMES}
"""Dictionary of dialect names to SQLAlchemy dialects."""


def get_supported_dialects() -> dict[str, Dialect]:
    """Get a dictionary of the supported SQLAlchemy dialects."""
    return _DIALECTS


def _dialect_module(dialect_name: str) -> ModuleType:
    """Get the SQLAlchemy dialect module for the given name."""
    return getattr(dialects, dialect_name)


_DIALECT_MODULES = {name: _dialect_module(name) for name in _DIALECT_NAMES}
"""Dictionary of dialect names to SQLAlchemy modules for type instantiation."""


def get_dialect_module(dialect_name: str) -> ModuleType:
    """Get the SQLAlchemy dialect module for the given name."""
    if dialect_name not in _DIALECT_MODULES:
        raise ValueError(f"Unsupported dialect: {dialect_name}")
    return _DIALECT_MODULES[dialect_name]
