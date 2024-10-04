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

from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema

from felis.datamodel import Schema
from felis.db.utils import DatabaseContext
from felis.metadata import MetaDataBuilder
from felis.tap_schema import DataLoader, TableManager
from felis.tests.utils import open_test_file

try:
    from testing.postgresql import Postgresql
except ImportError:
    Postgresql = None

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "sales.yaml")


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
        self.test_schema = Schema.from_uri(TEST_YAML)

    def test_create_metadata(self) -> None:
        """Test loading of data into a PostgreSQL TAP_SCHEMA database created
        by the `~felis.tap_schema.TableManager`.
        """
        try:
            # Create the TAP_SCHEMA database.
            mgr = TableManager()
            mgr.initialize_database(self.engine)

            # Load the test data into the database.
            loader = DataLoader(self.test_schema, mgr, self.engine, 1)
            loader.load()
        finally:
            # Drop the schema.
            DatabaseContext(metadata=mgr.metadata, engine=self.engine).drop()

    def test_reflect_database(self) -> None:
        """Test reflecting an existing PostgreSQL TAP_SCHEMA database into a
        `~felis.tap_schema.TableManager`.
        """
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
            mgr = TableManager(engine=self.engine)
            self.assertIsNotNone(mgr.metadata)
            self.assertGreater(len(mgr.metadata.tables), 0)
            table_names = set(
                [table_name.replace(f"{schema.name}.", "") for table_name in mgr.metadata.tables.keys()]
            )
            self.assertEqual(table_names, set(TableManager.get_table_names_std()))

            # See if test data can be loaded successfully using the existing
            # database.
            loader = DataLoader(self.test_schema, mgr, self.engine, 1)
            loader.load()
        finally:
            # Drop the schema.
            DatabaseContext(metadata=mgr.metadata, engine=self.engine).drop()

    def test_nonstandard_names(self) -> None:
        """Test the TAP table manager class with non-standard names for the
        schema and columns, which are present in the test YAML file used
        to create the TAP_SCHEMA database.
        """
        try:
            with open_test_file("test_tap_schema_nonstandard.yaml") as file:
                sch = Schema.from_stream(file, context={"id_generation": True})
            md = MetaDataBuilder(sch).build()
            ctx = DatabaseContext(md, self.engine)
            ctx.initialize()
            ctx.create_all()

            postfix = "11"
            mgr = TableManager(engine=self.engine, table_name_postfix=postfix, schema_name=sch.name)
            for table_name in mgr.get_table_names_std():
                table = mgr[table_name]
                self.assertEqual(table.name, f"{table_name}{postfix}".replace(f"{sch.name}", ""))
        finally:
            if ctx:
                ctx.drop()

    def test_bad_engine(self) -> None:
        """Test the TableManager class with an invalid engine."""
        bad_engine = create_engine("postgresql+psycopg2://fake_user:fake_password@fake_host:5555")
        with self.assertRaises(SQLAlchemyError):
            TableManager(engine=bad_engine)

    def tearDown(self) -> None:
        """Tear down the test case."""
        gc.collect()
        self.engine.dispose()
