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
from sqlalchemy import MetaData, create_engine

from felis.datamodel import Schema
from felis.metadata import SchemaMetaData

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class DumpTestCase(unittest.TestCase):
    """Dump the generated DDL for MySQL to an output file."""

    def setUp(self) -> None:
        os.makedirs(os.path.join(TESTDIR, ".tests"), exist_ok=True)
        with open(TEST_YAML) as test_yaml:
            self.yaml_data = yaml.safe_load(test_yaml)

    def test_dump(self) -> None:
        """Load test file and validate it using the data model.

        Dump the generated DDL for MySQL to an output file.
        """
        schema_obj = Schema.model_validate(self.yaml_data)
        md = SchemaMetaData(schema_obj)
        with open(os.path.join(TESTDIR, ".tests", "schema_metadata_dump_test.txt"), "w") as dumpfile:
            md.dump(connection_string="mysql://", file=dumpfile)


class SchemaMetaDataTestCase(unittest.TestCase):
    """Tests for the `SchemaMetaData` class."""

    def setUp(self) -> None:
        """Create an in-memory SQLite database and load the test data."""
        self.engine = create_engine("sqlite://")
        with open(os.path.join(TESTDIR, "data", "sales.yaml")) as data:
            self.yaml_data = yaml.safe_load(data)

    def connection(self):
        """Return a connection to the database."""
        return self.engine.connect()

    def test_create_all(self):
        """Create all tables in the schema using the metadata object.

        Check that the reflected `MetaData` from the database matches the
        `MetaData` created from the schema.
        """
        with self.connection() as connection:
            schema = Schema.model_validate(self.yaml_data)
            md = SchemaMetaData(schema, no_metadata_schema=True)
            md.create_all(connection)

            md_db = MetaData()
            md_db.reflect(connection)

            self.assertEqual(md_db.tables.keys(), md.tables.keys())

            for md_table_name in md.tables.keys():
                md_table = md.tables[md_table_name]
                md_db_table = md_db.tables[md_table_name]
                self.assertEqual(md_table.columns.keys(), md_db_table.columns.keys())
                for md_column_name in md_table.columns.keys():
                    md_column = md_table.columns[md_column_name]
                    md_db_column = md_db_table.columns[md_column_name]
                    self.assertEqual(type(md_column.type), type(md_db_column.type))
                    self.assertEqual(md_column.nullable, md_db_column.nullable)
                    self.assertEqual(md_column.primary_key, md_db_column.primary_key)


if __name__ == "__main__":
    unittest.main()
