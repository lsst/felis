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
from sqlalchemy import text

from felis.datamodel import Schema
from felis.db.database_context import PostgreSQLContext
from felis.metadata import MetaDataBuilder
from felis.tests.postgresql import TemporaryPostgresInstance, setup_postgres_test_db  # type: ignore

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "sales.yaml")


class TestPostgresql(unittest.TestCase):
    """Test PostgreSQL database setup."""

    postgresql: TemporaryPostgresInstance

    @classmethod
    def setUpClass(cls) -> None:
        # Create the postgres test server.
        cls.postgresql = cls.enterClassContext(setup_postgres_test_db())
        super().setUpClass()

    def test_initialize_create_and_drop(self) -> None:
        """Test database initialization, creation, and deletion in
        PostgreSQL.
        """
        # Create the schema and metadata
        with open(TEST_YAML) as test_file:
            yaml_data = yaml.safe_load(test_file)
        schema = Schema.model_validate(yaml_data)
        md = MetaDataBuilder(schema).build()

        # Initialize the database
        ctx = PostgreSQLContext(self.postgresql.engine.url, md)
        ctx.initialize()
        ctx.create_all()

        # Get the names of the tables without the schema prepended
        table_names = [name.split(".")[-1] for name in md.tables.keys()]

        # Check that the tables and columns are created
        with self.postgresql.begin() as conn:
            res = conn.execute(text("SELECT table_name FROM information_schema.tables"))
            tables = [row[0] for row in res.fetchall()]
            for table_name in table_names:
                self.assertIn(table_name, tables)
                # Check that all columns are created
                expected_columns = [col.name for col in md.tables[f"sales.{table_name}"].columns]
                res = conn.execute(
                    text("SELECT column_name FROM information_schema.columns WHERE table_name = :table_name"),
                    {"table_name": table_name},
                )
                actual_columns = [row[0] for row in res.fetchall()]
                self.assertSetEqual(set(expected_columns), set(actual_columns))

        # Drop the schema
        ctx.drop()

        # Check that the "sales" schema was dropped
        with self.postgresql.begin() as conn:
            res = conn.execute(
                text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sales'")
            )
            schemas = [row[0] for row in res.fetchall()]
            self.assertNotIn("sales", schemas)

    def test_create_and_drop_indexes(self) -> None:
        """Test creating and dropping indexes separately from table
        creation.
        """
        # Load the schema
        with open(TEST_YAML) as test_file:
            yaml_data = yaml.safe_load(test_file)
        schema = Schema.model_validate(yaml_data)

        # Create metadata without indexes
        md_no_indexes = MetaDataBuilder(schema, skip_indexes=True).build()

        # Initialize the database and create tables (without indexes)
        ctx = PostgreSQLContext(self.postgresql.engine.url, md_no_indexes)
        ctx.initialize()
        ctx.create_all()

        # Create metadata with indexes to get the index definitions
        md_with_indexes = MetaDataBuilder(schema, skip_indexes=False).build()
        ctx_with_indexes = PostgreSQLContext(self.postgresql.engine.url, md_with_indexes)

        def check_indexes_exist(should_exist: bool, message: str) -> None:
            """Check if indexes exist or don't exist in the database."""
            with self.postgresql.begin() as conn:
                from sqlalchemy import inspect

                inspector = inspect(conn)
                for table in md_with_indexes.tables.values():
                    # Get existing indexes for this table
                    existing_indexes = {
                        ix["name"]
                        for ix in inspector.get_indexes(table.name, schema=table.schema)
                        if "name" in ix and ix["name"] is not None
                    }
                    for index in table.indexes:
                        if index.name is not None:
                            exists = index.name in existing_indexes
                            if should_exist:
                                self.assertTrue(
                                    exists,
                                    f"Index '{index.name}' {message}",
                                )
                            else:
                                self.assertFalse(
                                    exists,
                                    f"Index '{index.name}' {message}",
                                )

        # Check that indexes don't exist yet
        check_indexes_exist(False, "should not exist yet")

        # Create the indexes
        ctx_with_indexes.create_indexes()

        # Check that indexes now exist
        check_indexes_exist(True, "should exist after creation")

        # Create the indexes again; should not raise an error
        ctx_with_indexes.create_indexes()

        # Drop the indexes
        ctx_with_indexes.drop_indexes()

        # Check that indexes were dropped
        check_indexes_exist(False, "should not exist after dropping")

        # Cleanup: drop the schema
        ctx.drop()

        # Cleanup
        ctx.close()
