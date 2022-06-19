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
from collections.abc import Mapping, MutableMapping
from typing import Any

from sqlalchemy import Float, SmallInteger, types
from sqlalchemy.dialects import mysql, oracle, postgresql
from sqlalchemy.ext.compiler import compiles

MYSQL = "mysql"
ORACLE = "oracle"
POSTGRES = "postgresql"
SQLITE = "sqlite"


class TINYINT(SmallInteger):
    """The non-standard TINYINT type."""

    __visit_name__ = "TINYINT"


class DOUBLE(Float):
    """The non-standard DOUBLE type."""

    __visit_name__ = "DOUBLE"


@compiles(TINYINT)
def compile_tinyint(type_: Any, compiler: Any, **kw: Any) -> str:
    return "TINYINT"


@compiles(DOUBLE)
def compile_double(type_: Any, compiler: Any, **kw: Any) -> str:
    return "DOUBLE"


boolean_map = {MYSQL: mysql.BIT(1), ORACLE: oracle.NUMBER(1), POSTGRES: postgresql.BOOLEAN()}

byte_map = {
    MYSQL: mysql.TINYINT(),
    ORACLE: oracle.NUMBER(3),
    POSTGRES: postgresql.SMALLINT(),
}

short_map = {
    MYSQL: mysql.SMALLINT(),
    ORACLE: oracle.NUMBER(5),
    POSTGRES: postgresql.SMALLINT(),
}

# Skip Oracle
int_map = {
    MYSQL: mysql.INTEGER(),
    POSTGRES: postgresql.INTEGER(),
}

long_map = {
    MYSQL: mysql.BIGINT(),
    ORACLE: oracle.NUMBER(38, 0),
    POSTGRES: postgresql.BIGINT(),
}

float_map = {
    MYSQL: mysql.FLOAT(),
    ORACLE: oracle.BINARY_FLOAT(),
    POSTGRES: postgresql.FLOAT(),
}

double_map = {
    MYSQL: mysql.DOUBLE(),
    ORACLE: oracle.BINARY_DOUBLE(),
    POSTGRES: postgresql.DOUBLE_PRECISION(),
}

char_map = {
    MYSQL: mysql.CHAR,
    ORACLE: oracle.CHAR,
    POSTGRES: postgresql.CHAR,
}

string_map = {
    MYSQL: mysql.VARCHAR,
    ORACLE: oracle.VARCHAR2,
    POSTGRES: postgresql.VARCHAR,
}

unicode_map = {
    MYSQL: mysql.NVARCHAR,
    ORACLE: oracle.NVARCHAR2,
    POSTGRES: postgresql.VARCHAR,
}

text_map = {
    MYSQL: mysql.LONGTEXT,
    ORACLE: oracle.CLOB,
    POSTGRES: postgresql.TEXT,
}

binary_map = {
    MYSQL: mysql.LONGBLOB,
    ORACLE: oracle.BLOB,
    POSTGRES: postgresql.BYTEA,
}


def boolean(**kwargs: Any) -> types.TypeEngine:
    return _vary(types.BOOLEAN(), boolean_map.copy(), kwargs)


def byte(**kwargs: Any) -> types.TypeEngine:
    return _vary(TINYINT(), byte_map.copy(), kwargs)


def short(**kwargs: Any) -> types.TypeEngine:
    return _vary(types.SMALLINT(), short_map.copy(), kwargs)


def int(**kwargs: Any) -> types.TypeEngine:
    return _vary(types.INTEGER(), int_map.copy(), kwargs)


def long(**kwargs: Any) -> types.TypeEngine:
    return _vary(types.BIGINT(), long_map.copy(), kwargs)


def float(**kwargs: Any) -> types.TypeEngine:
    return _vary(types.FLOAT(), float_map.copy(), kwargs)


def double(**kwargs: Any) -> types.TypeEngine:
    return _vary(DOUBLE(), double_map.copy(), kwargs)


def char(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    return _vary(types.CHAR(length), char_map.copy(), kwargs, length)


def string(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    return _vary(types.VARCHAR(length), string_map.copy(), kwargs, length)


def unicode(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    return _vary(types.NVARCHAR(length), unicode_map.copy(), kwargs, length)


def text(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    return _vary(types.CLOB(length), text_map.copy(), kwargs, length)


def binary(length: builtins.int, **kwargs: Any) -> types.TypeEngine:
    return _vary(types.BLOB(length), binary_map.copy(), kwargs, length)


def timestamp(**kwargs: Any) -> types.TypeEngine:
    return types.TIMESTAMP()


def _vary(
    type_: types.TypeEngine,
    variant_map: MutableMapping[str, types.TypeEngine],
    overrides: Mapping[str, types.TypeEngine],
    *args: Any,
) -> types.TypeEngine:
    for dialect, variant in overrides.items():
        variant_map[dialect] = variant
    for dialect, variant in variant_map.items():
        # If this is a class and not an instance, instantiate
        if isinstance(variant, type):
            variant = variant(*args)
        type_ = type_.with_variant(variant, dialect)
    return type_
