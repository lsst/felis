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

import os
import sys
import unittest
from typing import Any

from sqlalchemy import create_engine

from felis.datamodel import Schema
from felis.db.ddl import DDLGenerator
from felis.metadata import MetaDataBuilder

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test_ddl.yaml")


class DDLUtils:
    """Utility class for DDL generation."""

    def __init__(self, engine_url: str) -> None:
        self.schema = Schema.from_uri(TEST_YAML, context={"id_generation": True})
        self.metadata = MetaDataBuilder(self.schema).build()
        self.engine = create_engine(engine_url)
        self.ddl = self._create_ddl_generator()

    def _create_ddl_generator(self) -> DDLGenerator:
        return DDLGenerator(
            dialect=self.engine.dialect,
            metadata=self.metadata,
            output=sys.__stdout__,
            single_line_format=True,
        )


class BaseDDLTestCase(unittest.TestCase):
    """Base test case class for DDL generation."""

    def __init__(self, engine_url: str, *args: Any, **kwargs: Any) -> None:
        if self.__class__ is BaseDDLTestCase:
            raise unittest.SkipTest(
                "BaseDDLTestCase is a base class and should not be instantiated directly."
            )
        super().__init__(*args, **kwargs)
        self.utils = self._create_utils(engine_url)

    def _create_utils(self, engine_url: str) -> DDLUtils:
        return DDLUtils(engine_url)

    def _check_column(self, column_type: str, expected_type: str) -> None:
        self.assertEqual(
            self.utils.ddl.create_column("test_types", f"{column_type}_col"),
            f"{column_type}_col {expected_type}",
        )

    def _check_table(self, table_name: str, expected_sql: str) -> None:
        self.assertEqual(self.utils.ddl.create_table(table_name), expected_sql)


class SQLiteDDLTestCase(BaseDDLTestCase):
    """Test case for generation of DDL in SQLite."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__("sqlite:///:memory:", *args, **kwargs)

    def test_create_column(self) -> None:
        self._check_column("binary", "BLOB")
        self._check_column("boolean", "BOOLEAN")
        self._check_column("byte", "TINYINT")
        self._check_column("char", "CHAR(64)")
        self._check_column("double", "DOUBLE")
        self._check_column("float", "FLOAT")
        self._check_column("int", "INTEGER")
        self._check_column("long", "BIGINT")
        self._check_column("short", "SMALLINT")
        self._check_column("text", "TEXT")
        self._check_column("timestamp", "TIMESTAMP")
        self._check_column("unicode", "NVARCHAR(256)")

    def test_create_table(self) -> None:
        self._check_table(
            "test_table_creation",
            "CREATE TABLE test_ddl.test_table_creation ( id INTEGER, PRIMARY KEY (id) );",
        )

    def test_create_index(self) -> None:
        self.assertEqual(
            self.utils.ddl.create_index("test_index_creation", "test_index"),
            "CREATE INDEX test_ddl.test_index ON test_index_creation (indexed_col);",
        )

    def test_create_constraint(self) -> None:
        self.assertEqual(
            self.utils.ddl.add_constraint("test_constraint_creation", "test_unique_constraint"),
            "ALTER TABLE test_ddl.test_constraint_creation ADD CONSTRAINT test_unique_constraint UNIQUE "
            "(unique_col);",
        )

    def test_create_schema(self) -> None:
        self.assertEqual(self.utils.ddl.create_schema(), "CREATE SCHEMA IF NOT EXISTS test_ddl;")
