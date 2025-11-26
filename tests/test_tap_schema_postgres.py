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

import gc
import os
import unittest

from sqlalchemy import MetaData
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema

from felis.datamodel import Schema
from felis.db.database_context import create_database_context
from felis.metadata import MetaDataBuilder
from felis.tap_schema import DataLoader, TableManager

try:
    from testing.postgresql import Postgresql
except ImportError:
    Postgresql = None

TEST_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_SALES = os.path.join(TEST_DIR, "data", "sales.yaml")
TEST_TAP_SCHEMA_NONSTD = os.path.join(TEST_DIR, "data", "test_tap_schema_nonstandard.yaml")


class TestTapSchemaPostgresql(unittest.TestCase):
    """Test TAP_SCHEMA for PostgreSQL"""

    def setUp(self) -> None:
        """Set up a local PostgreSQL database and a test schema."""
        # Skip the test if the testing.postgresql package is not installed.
        if not Postgresql:
            self.skipTest("testing.postgresql not installed")

        # Start a PostgreSQL database for testing.
        self.postgresql = Postgresql()
        url = self.postgresql.url()
        self.engine = create_engine(url)

        # Setup a test schema.
        self.test_schema = Schema.from_uri(TEST_SALES)

    def test_create_metadata(self) -> None:
        """Test loading of data into a PostgreSQL TAP_SCHEMA database created
        by the `~felis.tap_schema.TableManager`.
        """
        db_ctx = None
        try:
            # Create the TAP_SCHEMA database.
            mgr = TableManager(engine_url=str(self.engine.url))
            db_ctx = create_database_context(str(self.engine.url), mgr.metadata)
            mgr.initialize_database(db_ctx)

            # Load the test data into the database.
            loader = DataLoader(self.test_schema, mgr, db_ctx, tap_schema_index=1)
            loader.load()
        finally:
            # Drop the schema.
            if db_ctx is not None:
                db_ctx.drop()

    def test_reflect_database(self) -> None:
        """Test reflecting an existing PostgreSQL TAP_SCHEMA database into a
        `~felis.tap_schema.TableManager`.
        """
        mgr = None
        db_ctx = None
        try:
            # Build the TAP_SCHEMA database independently of the TableManager.
            schema = TableManager.load_schema_resource()
            md = MetaDataBuilder(schema).build()
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    print(f"Creating schema '{schema.name}'")
                    conn.execute(CreateSchema(schema.name, if_not_exists=False))
                    trans.commit()
                except SQLAlchemyError as e:
                    trans.rollback()
                    self.fail(f"Failed to create schema: {e}")
            try:
                print(f"Creating tables in schema: {md.schema}")
                md.create_all(self.engine)
            except SQLAlchemyError as e:
                self.fail(f"Failed to create database: {e}")

            # Reflect the existing database into a TableManager.
            db_ctx = create_database_context(str(self.engine.url), md)
            mgr = TableManager(engine_url=str(self.engine.url), db_context=db_ctx)
            self.assertIsNotNone(mgr.metadata)
            self.assertGreater(len(mgr.metadata.tables), 0)
            table_names = set(
                [table_name.replace(f"{schema.name}.", "") for table_name in mgr.metadata.tables.keys()]
            )
            self.assertEqual(table_names, set(TableManager.get_table_names_std()))

            # See if test data can be loaded successfully using the existing
            # database.
            loader = DataLoader(self.test_schema, mgr, db_ctx, tap_schema_index=1)
            loader.load()
        finally:
            # Drop the schema.
            if db_ctx is not None:
                db_ctx.drop()

    def test_nonstandard_names(self) -> None:
        """Test the TAP table manager class with non-standard names for the
        schema and columns, which are present in the test YAML file used
        to create the TAP_SCHEMA database.
        """
        ctx = None
        try:
            with open(TEST_TAP_SCHEMA_NONSTD) as file:
                sch = Schema.from_stream(file, context={"id_generation": True})
            md = MetaDataBuilder(sch).build()
            ctx = create_database_context(str(self.engine.url), md)
            ctx.initialize()
            ctx.create_all()

            postfix = "11"
            # Create a context for reflection with the existing database
            reflect_ctx = create_database_context(str(self.engine.url), md)
            mgr = TableManager(
                engine_url=str(self.engine.url),
                db_context=reflect_ctx,
                table_name_postfix=postfix,
                schema_name=sch.name,
            )
            for table_name in mgr.get_table_names_std():
                table = mgr[table_name]
                self.assertEqual(table.name, f"{table_name}{postfix}".replace(f"{sch.name}", ""))
        finally:
            if ctx:
                ctx.drop()

    def test_bad_engine(self) -> None:
        """Test the TableManager class with an invalid engine."""
        bad_url = "postgresql+psycopg2://fake_user:fake_password@fake_host:5555"
        # Create metadata for reflection attempt
        md = MetaData(schema="TAP_SCHEMA")
        db_ctx = create_database_context(bad_url, md)
        # Reflection will fail when trying to connect
        with self.assertRaises(SQLAlchemyError):
            TableManager(db_context=db_ctx)

    def tearDown(self) -> None:
        """Tear down the test case."""
        gc.collect()
        self.engine.dispose()


if __name__ == "__main__":
    unittest.main()
