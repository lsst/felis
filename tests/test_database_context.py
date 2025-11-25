import os
import unittest

from sqlalchemy import (
    MetaData,
    create_engine,
    text,
)

try:
    from testing.postgresql import Postgresql  # type: ignore
except ImportError:
    Postgresql = None

from felis.datamodel import Schema
from felis.db.database_context import (
    DatabaseContextError,
    DatabaseContextFactory,
    MockContext,
    MySQLContext,
    PostgreSQLContext,
    SupportedDialect,
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

    def test_database_context(self):
        """Test database context with SQLite."""
        # Initialize the database context

        db_ctx = create_database_context(self._engine_url, self._metadata)
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
        db_ctx.execute(text(f"SELECT * FROM {schema_prefix}orders"))

        # Execute a query that raises an error
        if not isinstance(db_ctx, MockContext):
            with self.assertRaises(DatabaseContextError):
                db_ctx.execute("SELECT * FROM non_existent_table")

        # Drop all tables
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
        engine = create_engine(self._engine_url)
        metadata = MetaDataBuilder(self._schema, apply_schema_to_metadata=False).build()
        with self.assertRaises(DatabaseContextError):
            PostgreSQLContext(engine, metadata)

    def test_schema_already_exists_error(self):
        """Test that attempting to create a schema that already exists
        raises an error.
        """
        engine = create_engine(self._engine_url)
        db_ctx = PostgreSQLContext(engine, self._metadata)
        db_ctx.drop()  # Ensure database does not exist
        db_ctx.initialize()
        # Attempt to initialize again, which should raise an error
        with self.assertRaises(DatabaseContextError):
            db_ctx.initialize()
        db_ctx.drop()  # Clean up


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

    def test_database_already_exists_error(self):
        """Test that attempting to create a MySQL database that already exists
        raises an error.
        """
        engine = create_engine(self._engine_url)
        db_ctx = MySQLContext(engine, self._metadata)
        db_ctx.drop()  # Ensure database does not exist
        db_ctx.initialize()
        # Attempt to initialize again, which should raise an error
        with self.assertRaises(DatabaseContextError):
            db_ctx.initialize()
        db_ctx.drop()  # Clean up


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

    def test_bad_engine(self):
        """Test that using a SQLite engine with a Postgres context correctly
        throws an error.
        """
        with self.assertRaises(DatabaseContextError):
            engine = create_engine("sqlite:///:memory:")
            PostgreSQLContext(engine, MetaData())

    def test_create_database_context_bad_dialect(self):
        """Test that attempting to create a database context with an
        unsupported dialect raises an error.
        """
        engine_url = "oracle+cx_oracle://user:password@host:1521/service_name"
        metadata = MetaData()
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
        """Test that supported dialects with drivers are correctly reported."""
        supported_dialects = DatabaseContextFactory.get_supported_dialects()
        self.assertIn(SupportedDialect.from_string("postgresql+psycopg2").value, supported_dialects)
        self.assertIn(SupportedDialect.from_string("mysql+mysqlconnector").value, supported_dialects)

    def test_unsupported_dialect(self) -> None:
        """Test that an unsupported dialect raises an error."""
        with self.assertRaises(ValueError):
            SupportedDialect.from_string("oracle+cx_oracle")
        with self.assertRaises(ValueError):
            SupportedDialect.from_string("oracle")


if __name__ == "__main__":
    unittest.main()
