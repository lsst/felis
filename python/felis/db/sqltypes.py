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
from sqlalchemy.dialects import mysql, postgresql
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
POSTGRES = "postgresql"
SQLITE = "sqlite"


class TINYINT(SmallInteger):
    """The non-standard TINYINT type."""

    __visit_name__ = "TINYINT"


@compiles(TINYINT)
def compile_tinyint(type_: Any, compiler: Any, **kwargs: Any) -> str:
    """Compile the non-standard ``TINYINT`` type to SQL.

    Parameters
    ----------
    type_
        The type object.
    compiler
        The compiler object.
    **kwargs
        Additional keyword arguments.

    Returns
    -------
    `str`
        The compiled SQL for TINYINT.

    Notes
    -----
    This function returns the SQL for the the TINYINT type. The function
    signature and parameters are defined by SQLAlchemy.
    """
    return "TINYINT"


_TypeMap = Mapping[str, types.TypeEngine | type[types.TypeEngine]]

boolean_map: _TypeMap = {MYSQL: mysql.BOOLEAN, POSTGRES: postgresql.BOOLEAN()}

byte_map: _TypeMap = {
    MYSQL: mysql.TINYINT(),
    POSTGRES: postgresql.SMALLINT(),
}

short_map: _TypeMap = {
    MYSQL: mysql.SMALLINT(),
    POSTGRES: postgresql.SMALLINT(),
}

int_map: _TypeMap = {
    MYSQL: mysql.INTEGER(),
    POSTGRES: postgresql.INTEGER(),
}

long_map: _TypeMap = {
    MYSQL: mysql.BIGINT(),
    POSTGRES: postgresql.BIGINT(),
}

float_map: _TypeMap = {
    MYSQL: mysql.FLOAT(),
    POSTGRES: postgresql.FLOAT(),
}

double_map: _TypeMap = {
    MYSQL: mysql.DOUBLE(),
    POSTGRES: postgresql.DOUBLE_PRECISION(),
}

char_map: _TypeMap = {
    MYSQL: mysql.CHAR,
    POSTGRES: postgresql.CHAR,
}

string_map: _TypeMap = {
    MYSQL: mysql.VARCHAR,
    POSTGRES: postgresql.VARCHAR,
}

unicode_map: _TypeMap = {
    MYSQL: mysql.NVARCHAR,
    POSTGRES: postgresql.VARCHAR,
}

text_map: _TypeMap = {
    MYSQL: mysql.LONGTEXT,
    POSTGRES: postgresql.TEXT,
}

binary_map: _TypeMap = {
    MYSQL: mysql.LONGBLOB,
    POSTGRES: postgresql.BYTEA,
}

timestamp_map: _TypeMap = {
    MYSQL: mysql.DATETIME(timezone=False),
    POSTGRES: postgresql.TIMESTAMP(timezone=False),
}


def boolean(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Boolean` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis boolean.
    """
    return _vary(types.BOOLEAN(), boolean_map, kwargs)


def byte(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Byte` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis byte.
    """
    return _vary(TINYINT(), byte_map, kwargs)


def short(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Short` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis short.
    """
    return _vary(types.SMALLINT(), short_map, kwargs)


def int(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Int` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis int.
    """
    return _vary(types.INTEGER(), int_map, kwargs)


def long(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Long` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis long.
    """
    return _vary(types.BIGINT(), long_map, kwargs)


def float(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Float` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis float.
    """
    return _vary(types.FLOAT(), float_map, kwargs)


def double(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Double` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis double.
    """
    return _vary(types.DOUBLE(), double_map, kwargs)


def char(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Char` with variants.

    Parameters
    ----------
    length
        The length of the character field.
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis char.
    """
    return _vary(types.CHAR(length), char_map, kwargs, length)


def string(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.String` with variants.

    Parameters
    ----------
    length
        The length of the string field.
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis string.
    """
    return _vary(types.VARCHAR(length), string_map, kwargs, length)


def unicode(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Unicode` with variants.

    Parameters
    ----------
    length
        The length of the unicode string field.
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis unicode string.
    """
    return _vary(types.NVARCHAR(length), unicode_map, kwargs, length)


def text(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Text` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for Felis text.
    """
    return _vary(types.TEXT(), text_map, kwargs)


def binary(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Binary` with variants.

    Parameters
    ----------
    length
        The length of the binary field.
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for Felis binary.
    """
    return _vary(types.BLOB(length), binary_map, kwargs, length)


def timestamp(**kwargs: Any) -> types.TypeEngine:
    """Get the SQL type for Felis `~felis.types.Timestamp` with variants.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to the type object.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQL type for a Felis timestamp.
    """
    return _vary(types.TIMESTAMP(timezone=False), timestamp_map, kwargs)


def get_type_func(type_name: str) -> Callable:
    """Find the function which creates a specific SQL type by its Felis type
    name.

    Parameters
    ----------
    type_name
        The name of the type function to get.

    Returns
    -------
    `Callable`
        The function for the type.

    Raises
    ------
    ValueError
        Raised if the type name is not recognized.

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
    """Add datatype variants and overrides to a SQLAlchemy type.

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
