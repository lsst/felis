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
from felis.metadata import DatabaseContext, MetaDataBuilder

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "sales.yaml")


class MetaDataTestCase(unittest.TestCase):
    """Test creationg of SQLAlchemy `MetaData` from a `Schema`."""

    def setUp(self) -> None:
        """Create an in-memory SQLite database and load the test data."""
        self.engine = create_engine("sqlite://")
        with open(TEST_YAML) as data:
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
            schema.name = "main"
            builder = MetaDataBuilder(schema)
            builder.build()
            md = builder.metadata

            ctx = DatabaseContext(md, connection)

            ctx.create_all()

            md_db = MetaData()
            md_db.reflect(connection, schema=schema.name)

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

            # TODO: Check constraints and indexes


if __name__ == "__main__":
    unittest.main()
