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

import builtins
from collections.abc import Mapping
from typing import Any, Callable

from sqlalchemy import SmallInteger, types
from sqlalchemy.dialects import mysql, oracle, postgresql
from sqlalchemy.ext.compiler import compiles

MYSQL = "mysql"
ORACLE = "oracle"
POSTGRES = "postgresql"
SQLITE = "sqlite"


class TINYINT(SmallInteger):
    """The non-standard TINYINT type."""

    __visit_name__ = "TINYINT"


@compiles(TINYINT)
def compile_tinyint(type_: Any, compiler: Any, **kw: Any) -> str:
    """Return type name for TINYINT."""
    return "TINYINT"


_TypeMap = Mapping[str, types.TypeEngine | type[types.TypeEngine]]

boolean_map: _TypeMap = {MYSQL: mysql.BIT(1), ORACLE: oracle.NUMBER(1), POSTGRES: postgresql.BOOLEAN()}

byte_map: _TypeMap = {
    MYSQL: mysql.TINYINT(),
    ORACLE: oracle.NUMBER(3),
    POSTGRES: postgresql.SMALLINT(),
}

short_map: _TypeMap = {
    MYSQL: mysql.SMALLINT(),
    ORACLE: oracle.NUMBER(5),
    POSTGRES: postgresql.SMALLINT(),
}

# Skip Oracle
int_map: _TypeMap = {
    MYSQL: mysql.INTEGER(),
    POSTGRES: postgresql.INTEGER(),
}

long_map: _TypeMap = {
    MYSQL: mysql.BIGINT(),
    ORACLE: oracle.NUMBER(38, 0),
    POSTGRES: postgresql.BIGINT(),
}

float_map: _TypeMap = {
    MYSQL: mysql.FLOAT(),
    ORACLE: oracle.BINARY_FLOAT(),
    POSTGRES: postgresql.FLOAT(),
}

double_map: _TypeMap = {
    MYSQL: mysql.DOUBLE(),
    ORACLE: oracle.BINARY_DOUBLE(),
    POSTGRES: postgresql.DOUBLE_PRECISION(),
}

char_map: _TypeMap = {
    MYSQL: mysql.CHAR,
    ORACLE: oracle.CHAR,
    POSTGRES: postgresql.CHAR,
}

string_map: _TypeMap = {
    MYSQL: mysql.VARCHAR,
    ORACLE: oracle.VARCHAR2,
    POSTGRES: postgresql.VARCHAR,
}

unicode_map: _TypeMap = {
    MYSQL: mysql.NVARCHAR,
    ORACLE: oracle.NVARCHAR2,
    POSTGRES: postgresql.VARCHAR,
}

text_map: _TypeMap = {
    MYSQL: mysql.LONGTEXT,
    ORACLE: oracle.CLOB,
    POSTGRES: postgresql.TEXT,
}

binary_map: _TypeMap = {
    MYSQL: mysql.LONGBLOB,
    ORACLE: oracle.BLOB,
    POSTGRES: postgresql.BYTEA,
}


def boolean(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for boolean."""
    return _vary(types.BOOLEAN(), boolean_map, kwargs)


def byte(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for byte."""
    return _vary(TINYINT(), byte_map, kwargs)


def short(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for short integer."""
    return _vary(types.SMALLINT(), short_map, kwargs)


def int(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for integer."""
    return _vary(types.INTEGER(), int_map, kwargs)


def long(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for long integer."""
    return _vary(types.BIGINT(), long_map, kwargs)


def float(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for single precision float."""
    return _vary(types.FLOAT(), float_map, kwargs)


def double(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for double precision float."""
    return _vary(types.DOUBLE(), double_map, kwargs)


def char(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for character."""
    return _vary(types.CHAR(length), char_map, kwargs, length)


def string(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for string."""
    return _vary(types.VARCHAR(length), string_map, kwargs, length)


def unicode(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for unicode string."""
    return _vary(types.NVARCHAR(length), unicode_map, kwargs, length)


def text(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for text."""
    return _vary(types.TEXT(), text_map, kwargs)


def binary(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for binary."""
    return _vary(types.BLOB(length), binary_map, kwargs, length)


def timestamp(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for timestamp."""
    return types.TIMESTAMP()


def get_type_func(type_name: str) -> Callable:
    """Return the function for the type with the given name."""
    if type_name not in globals():
        raise ValueError(f"Unknown type: {type_name}")
    return globals()[type_name]


def _vary(
    type_: types.TypeEngine,
    variant_map: _TypeMap,
    overrides: _TypeMap,
    *args: Any,
) -> types.TypeEngine:
    variants: dict[str, types.TypeEngine | type[types.TypeEngine]] = dict(variant_map)
    variants.update(overrides)
    for dialect, variant in variants.items():
        # If this is a class and not an instance, instantiate
        if callable(variant):
            variant = variant(*args)
        type_ = type_.with_variant(variant, dialect)
    return type_
