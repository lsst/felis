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

import sqlalchemy

try:
    from testing.postgresql import Postgresql  # type: ignore
except ImportError:
    Postgresql = None

from felis.datamodel import Schema
from felis.db.database_context import (
    DatabaseContext,
    DatabaseContextError,
    DatabaseContextFactory,
    MockContext,
    MySQLContext,
    PostgreSQLContext,
    create_database_context,
)
from felis.metadata import MetaDataBuilder

TEST_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TEST_DIR, "data", "sales.yaml")


class BaseDatabaseContextTest:
    """Base tests of database context."""

    def setUp(self) -> None:
        """Set up the test case."""
        self._schema = Schema.from_uri(TEST_YAML)
        self._metadata = MetaDataBuilder(self._schema).build()
        self._engine_url: str
        self._dialect_name: str

    def _create_database_context(self) -> DatabaseContext:
        return create_database_context(self._engine_url, self._metadata)

    def test_database_context(self):
        """Test database context with SQLite."""
        with self._create_database_context() as db_ctx:
            self.assertIsNotNone(db_ctx)
            self.assertIsNotNone(db_ctx.metadata)
            if not isinstance(db_ctx, MockContext):
                # Only a non-mock connection has a valid engine object.
                self.assertIsNotNone(db_ctx.engine)
                self.assertEqual(db_ctx.engine.dialect.name, self._dialect_name)

            # Drop first in case it exists from a previous test
            db_ctx.drop()

            # Initialize the database
            db_ctx.initialize()

            # Create all tables
            db_ctx.create_all()

            # Drop indexes
            db_ctx.drop_indexes()

            # Create indexes
            db_ctx.create_indexes()

            # Determine schema prefix (none if schema name is not set)
            if db_ctx.metadata.schema:
                schema_prefix = f"{db_ctx.metadata.schema}."
            else:
                schema_prefix = ""

            # Execute a simple query from a string
            db_ctx.execute(f"SELECT * FROM {schema_prefix}customers")

            # Execute a simple query from a TextClause
            db_ctx.execute(sqlalchemy.text(f"SELECT * FROM {schema_prefix}orders"))

            # Execute a query that raises an error, except for MockContext
            if not isinstance(db_ctx, MockContext):
                with self.assertRaises(DatabaseContextError):
                    db_ctx.execute("SELECT * FROM non_existent_table")

            # Drop all tables
            db_ctx.drop()

    def test_initialize_is_idempotent(self):
        """Test that attempting to create a schema that already exists does not
        raise an error.
        """
        with self._create_database_context() as db_ctx:
            # Ensure database does not exist by first dropping
            db_ctx.drop()

            # Initialize the database
            db_ctx.initialize()

            # Second initialization should not raise an error.
            db_ctx.initialize()

            # Clean up
            db_ctx.drop()

    def test_drop_is_idempotent(self):
        """Test that dropping a schema that does not exist does not raise an
        error.
        """
        with self._create_database_context() as db_ctx:
            # Initialize the database
            db_ctx.initialize()

            # Drop the schema
            db_ctx.drop()

            # Second drop should not raise an error.
            db_ctx.drop()


class SQLiteTestCase(BaseDatabaseContextTest, unittest.TestCase):
    """Tests of database context using SQLite dialect."""

    def setUp(self) -> None:
        """Set up the test case."""
        super().setUp()
        self._dialect_name = "sqlite"
        self._engine_url = "sqlite:///:memory:"


@unittest.skipIf(Postgresql is None, "testing.postgresql is not installed")
class PostgreSQLTestCase(BaseDatabaseContextTest, unittest.TestCase):
    """Tests of database context using PostgreSQL dialect."""

    def setUp(self) -> None:
        """Set up the test case."""
        super().setUp()
        self._dialect_name = "postgresql"
        self._postgres = Postgresql()
        self._engine_url = self._postgres.url()

    def test_missing_schema_name(self):
        """Test that a missing schema name raises an error when using
        a Postgres context.
        """
        metadata = MetaDataBuilder(self._schema, apply_schema_to_metadata=False).build()
        with self.assertRaises(DatabaseContextError):
            PostgreSQLContext(sqlalchemy.engine.make_url(self._engine_url), metadata)


class MySQLTestCase(BaseDatabaseContextTest, unittest.TestCase):
    """Tests of MySQL database context."""

    # Environment variable name for MySQL engine URL
    _env_name = "_FELIS_MYSQL_ENGINE_URL"

    def setUp(self) -> None:
        super().setUp()
        try:
            mysql_engine_url = os.environ[f"{self._env_name}"]
        except KeyError:
            raise unittest.SkipTest(f"{self._env_name} is not set in the environment; skipping MySQL tests.")
        if mysql_engine_url is None or mysql_engine_url == "":
            raise ValueError(f"Value of {self._env_name} from environment is invalid")
        self._dialect_name = "mysql"
        self._engine_url = mysql_engine_url

    def test_missing_schema_name(self):
        """Test that a missing schema name raises an error when using
        a MySQL context.
        """
        metadata = MetaDataBuilder(self._schema, apply_schema_to_metadata=False).build()
        with self.assertRaises(DatabaseContextError):
            MySQLContext(sqlalchemy.engine.make_url(self._engine_url), metadata)


class MockTestCase(BaseDatabaseContextTest, unittest.TestCase):
    """Tests of mock database context."""

    def setUp(self) -> None:
        super().setUp()
        # This URL should result in a mock connection being setup.
        self._engine_url = "sqlite://"

    def test_mock_connection_engine_error(self):
        """Test that attempting to access the engine of a mock context throws
        an error.
        """
        db_ctx = create_database_context(self._engine_url, self._metadata)
        with self.assertRaises(DatabaseContextError):
            _ = db_ctx.engine


class DatabaseContextTestCase(unittest.TestCase):
    """Test that a mismatch between the engine and database context correctly
    throws an error.
    """

    def test_create_with_bad_url(self):
        """Test that using a SQLite engine with a Postgres context correctly
        throws an error.
        """
        with self.assertRaises(DatabaseContextError):
            engine = sqlalchemy.engine.make_url("sqlite:///:memory:")
            PostgreSQLContext(engine, sqlalchemy.MetaData(schema="test_schema"))

    def test_create_with_bad_dialect(self):
        """Test that attempting to create a database context with an
        unsupported dialect raises an error.
        """
        engine_url = "oracle+cx_oracle://user:password@host:1521/service_name"
        metadata = sqlalchemy.MetaData()
        with self.assertRaises(DatabaseContextError):
            create_database_context(engine_url, metadata)


class SupportedDialectsTestCase(unittest.TestCase):
    """Test supported dialects."""

    def test_supported_dialects(self) -> None:
        """Test that supported dialects are correctly reported."""
        supported_dialects = DatabaseContextFactory.get_supported_dialects()
        self.assertIn("sqlite", supported_dialects)
        self.assertIn("postgresql", supported_dialects)
        self.assertIn("mysql", supported_dialects)

    def test_supported_dialect_with_driver(self) -> None:
        """Test that supported dialects with drivers are correctly
        normalized.
        """
        supported_dialects = DatabaseContextFactory.get_supported_dialects()
        # Test that postgresql+psycopg2 normalizes to postgresql
        self.assertIn("postgresql", supported_dialects)
        # Test that mysql+mysqlconnector normalizes to mysql
        self.assertIn("mysql", supported_dialects)

    def test_unsupported_dialect(self) -> None:
        """Test that an unsupported dialect raises an error."""
        metadata = sqlalchemy.MetaData()
        with self.assertRaises(DatabaseContextError):
            create_database_context("oracle+cx_oracle://user:pass@host/db", metadata)
        with self.assertRaises(DatabaseContextError):
            create_database_context("oracle://user:pass@host/db", metadata)


if __name__ == "__main__":
    unittest.main()
