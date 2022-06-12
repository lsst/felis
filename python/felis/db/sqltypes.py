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
# along with this program. If not, see <https://www.gnu.org/licenses/>.

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
def compile_tinyint(type_, compiler, **kw):
    return "TINYINT"


@compiles(DOUBLE)
def compile_double(type_, compiler, **kw):
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


def boolean(**kwargs):
    return _vary(types.BOOLEAN(), boolean_map.copy(), kwargs)


def byte(**kwargs):
    return _vary(TINYINT(), byte_map.copy(), kwargs)


def short(**kwargs):
    return _vary(types.SMALLINT(), short_map.copy(), kwargs)


def int(**kwargs):
    return _vary(types.INTEGER(), int_map.copy(), kwargs)


def long(**kwargs):
    return _vary(types.BIGINT(), long_map.copy(), kwargs)


def float(**kwargs):
    return _vary(types.FLOAT(), float_map.copy(), kwargs)


def double(**kwargs):
    return _vary(DOUBLE(), double_map.copy(), kwargs)


def char(length, **kwargs):
    return _vary(types.CHAR(length), char_map.copy(), kwargs, length)


def string(length, **kwargs):
    return _vary(types.VARCHAR(length), string_map.copy(), kwargs, length)


def unicode(length, **kwargs):
    return _vary(types.NVARCHAR(length), unicode_map.copy(), kwargs, length)


def text(length, **kwargs):
    return _vary(types.CLOB(length), text_map.copy(), kwargs, length)


def binary(length, **kwargs):
    return _vary(types.BLOB(length), binary_map.copy(), kwargs, length)


def timestamp(**kwargs):
    return types.TIMESTAMP()


def _vary(type_, variant_map, overrides, *args):
    for dialect, variant in overrides.items():
        variant_map[dialect] = variant
    for dialect, variant in variant_map.items():
        # If this is a class and not an instance, instantiate
        if isinstance(variant, type):
            variant = variant(*args)
        type_ = type_.with_variant(variant, dialect)
    return type_
