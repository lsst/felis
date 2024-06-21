"""Map Felis types to SQLAlchemy types."""

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

import builtins
from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy import SmallInteger, types
from sqlalchemy.dialects import mysql, oracle, postgresql
from sqlalchemy.ext.compiler import compiles

__all__ = [
    "boolean",
    "byte",
    "short",
    "int",
    "long",
    "float",
    "double",
    "char",
    "string",
    "unicode",
    "text",
    "binary",
    "timestamp",
    "get_type_func",
]

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

boolean_map: _TypeMap = {MYSQL: mysql.BOOLEAN, ORACLE: oracle.NUMBER(1), POSTGRES: postgresql.BOOLEAN()}

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
    """Return SQLAlchemy type for boolean.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.BOOLEAN(), boolean_map, kwargs)


def byte(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for byte.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(TINYINT(), byte_map, kwargs)


def short(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for short integer.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.SMALLINT(), short_map, kwargs)


def int(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for integer.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.INTEGER(), int_map, kwargs)


def long(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for long integer.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.BIGINT(), long_map, kwargs)


def float(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for single precision float.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.FLOAT(), float_map, kwargs)


def double(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for double precision float.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.DOUBLE(), double_map, kwargs)


def char(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for character.

    Parameters
    ----------
    length
        The length of the character field.
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.CHAR(length), char_map, kwargs, length)


def string(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for string.

    Parameters
    ----------
    length
        The length of the string field.
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.VARCHAR(length), string_map, kwargs, length)


def unicode(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for unicode string.

    Parameters
    ----------
    length
        The length of the unicode string field.
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.NVARCHAR(length), unicode_map, kwargs, length)


def text(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for text.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.TEXT(), text_map, kwargs)


def binary(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for binary.

    Parameters
    ----------
    length
        The length of the binary field.
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return _vary(types.BLOB(length), binary_map, kwargs, length)


def timestamp(**kwargs: Any) -> types.TypeEngine:
    """Return SQLAlchemy type for timestamp.

    Parameters
    ----------
    kwargs
        Additional keyword arguments to pass to the type object.
    """
    return types.TIMESTAMP()


def get_type_func(type_name: str) -> Callable:
    """Return the function for the type with the given name.

    Parameters
    ----------
    type_name
        The name of the type function to get.

    Raises
    ------
    ValueError
        If the type name is not recognized.

    Notes
    -----
    This maps the type name to the function that creates the SQL type. This is
    the main way to get the type functions from the type names.
    """
    if type_name not in globals():
        raise ValueError(f"Unknown type: {type_name}")
    return globals()[type_name]


def _vary(
    type_: types.TypeEngine,
    variant_map: _TypeMap,
    overrides: _TypeMap,
    *args: Any,
) -> types.TypeEngine:
    """Build a SQLAlchemy type object including the datatype variant and
    override definitions from Felis.

    Parameters
    ----------
    type_
        The base SQLAlchemy type object. This is essentially a default
        SQLAlchemy ``TypeEngine`` object, which will apply if there is no
        variant or type override from the schema.
    variant_map
        The dictionary of dialects to types. Each key is a string representing
        a dialect name, and each value is either an instance of
        ``TypeEngine`` representing the variant type object or a callable
        reference to its class type that will be instantiated later.
    overrides
        The dictionary of dialects to types to override the defaults. Each key
        is a string representing a dialect name and type with a similar
        structure as the `variant_map`.
    args
        The extra arguments to pass to the type object.

    Notes
    -----
    This function is intended for internal use only. It builds a SQLAlchemy
    ``TypeEngine`` that includes variants and overrides defined by Felis.
    """
    variants: dict[str, types.TypeEngine | type[types.TypeEngine]] = dict(variant_map)
    variants.update(overrides)
    for dialect, variant in variants.items():
        # If this is a class and not an instance, instantiate
        if callable(variant):
            variant = variant(*args)
        type_ = type_.with_variant(variant, dialect)
    return type_
