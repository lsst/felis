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

import re
from typing import Any

from sqlalchemy import types
from sqlalchemy.dialects import mysql, oracle, postgresql, sqlite
from sqlalchemy.types import TypeEngine

from ..datamodel import Column

MYSQL = "mysql"
ORACLE = "oracle"
POSTGRES = "postgresql"
SQLITE = "sqlite"

TABLE_OPTS = {
    "mysql:engine": "mysql_engine",
    "mysql:charset": "mysql_charset",
    "oracle:compress": "oracle_compress",
}

COLUMN_VARIANT_OVERRIDE = {
    "mysql_datatype": "mysql",
    "oracle_datatype": "oracle",
    "postgresql_datatype": "postgresql",
    "sqlite_datatype": "sqlite",
}

DIALECT_MODULES = {MYSQL: mysql, ORACLE: oracle, SQLITE: sqlite, POSTGRES: postgresql}

_length_regex = re.compile(r"\((\d+)\)")
"""A regular expression that is looking for numbers within parentheses."""


def process_variant_override(dialect_name: str, variant_override_str: str) -> types.TypeEngine:
    """Return variant type for given dialect."""
    dialect = DIALECT_MODULES[dialect_name]
    variant_type_name = variant_override_str.split("(")[0]

    # Process Variant Type
    if variant_type_name not in dir(dialect):
        raise ValueError(f"Type {variant_type_name} not found in dialect {dialect_name}")
    variant_type = getattr(dialect, variant_type_name)
    length_params = []
    if match := _length_regex.search(variant_override_str):
        length_params.extend([int(i) for i in match.group(1).split(",")])
    return variant_type(*length_params)


def make_variant_dict(column_obj: Column) -> dict[str, TypeEngine[Any]]:
    """Handle variant overrides for a `felis.datamodel.Column`.

    This function will return a dictionary of `str` to
    `sqlalchemy.types.TypeEngine` containing variant datatype information
    (e.g., for mysql, postgresql, etc).

    Parameters
    ----------
    column_obj : `felis.datamodel.Column`
        The column object from which to build the variant dictionary.

    Returns
    -------
    variant_dict : `dict`
        The dictionary of `str` to `sqlalchemy.types.TypeEngine` containing
        variant datatype information (e.g., for mysql, postgresql, etc).
    """
    variant_dict = {}
    for field_name, value in iter(column_obj):
        if field_name in COLUMN_VARIANT_OVERRIDE and value is not None:
            dialect = COLUMN_VARIANT_OVERRIDE[field_name]
            variant: TypeEngine = process_variant_override(dialect, value)
            variant_dict[dialect] = variant
    return variant_dict
