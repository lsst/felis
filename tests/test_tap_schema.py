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
import unittest

import yaml
from sqlalchemy import create_engine

from felis import tap_schema
from felis.datamodel import Schema
from felis.tap_schema import DataLoader, TableManager

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "sales.yaml")


class TableManagerTestCase(unittest.TestCase):
    """Test the TAP loading visitor."""

    def setUp(self) -> None:
        """Set up the test case."""
        data = yaml.safe_load(open(TEST_YAML))
        self.schema = Schema.model_validate(data)

    def test_create_metadata(self) -> None:
        """Test the TAP table manager class."""
        mgr = TableManager()

        tap_schema_name = mgr.tap_schema_name

        # Check the created metadata and tables.
        self.assertNotEqual(len(mgr.tables), 0)
        self.assertEqual(mgr.metadata.schema, tap_schema_name)
        tables = mgr.tables
        table_names = [f"{tap_schema_name}.{table_name}" for table_name in tap_schema._COLUMNS.keys()]
        self.assertSetEqual(set(table_names), set(tables.keys()))

        # Check that the metadata contains the required columns for each table.
        for table_name, columns in tap_schema._COLUMNS.items():
            table = tables[f"{tap_schema_name}.{table_name}"]
            self.assertSetEqual(set(columns.keys()), set(table.columns.keys()))

        # Make sure that creating a new table manager works when one has
        # already been created.
        mgr = TableManager()

    def test_table_name_postfix(self) -> None:
        """Test the TAP table manager class."""
        postfix = "11"
        mgr = TableManager(tap_schema_name=None, table_name_postfix=postfix)

        for table_name in mgr.standard_table_names:
            table = mgr[table_name]
            self.assertEqual(table.name, f"{table_name}{postfix}")


class DataLoaderTestCase(unittest.TestCase):
    """Test the TAP data loader class."""

    def setUp(self) -> None:
        """Set up the test case."""
        data = yaml.safe_load(open(TEST_YAML))
        self.schema = Schema.model_validate(data)

    def test_sqlite(self) -> None:
        """Test the TAP data loader class using an in-memory SQLite
        database.
        """
        engine = create_engine("sqlite:///:memory:")

        mgr = TableManager(tap_schema_name=None)
        mgr.initialize_database(engine)

        loader = DataLoader(self.schema, mgr, engine, 1)
        loader.load()
