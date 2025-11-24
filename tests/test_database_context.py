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
    MockContext,
    PostgreSQLContext,
    create_database_context,
)
from felis.metadata import MetaDataBuilder

TEST_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TEST_DIR, "data", "sales.yaml")


class DatabaseContextTestCase:
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

        # Drop all tables
        db_ctx.drop()


class SQLiteTestCase(DatabaseContextTestCase, unittest.TestCase):
    """Tests of database context using SQLite dialect."""

    def setUp(self) -> None:
        """Set up the test case."""
        super().setUp()
        self._dialect_name = "sqlite"
        self._engine_url = "sqlite:///:memory:"


@unittest.skipIf(Postgresql is None, "testing.postgresql is not installed")
class PostgreSQLTestCase(DatabaseContextTestCase, unittest.TestCase):
    """Tests of database context using PostgreSQL dialect."""

    def setUp(self) -> None:
        """Set up the test case."""
        super().setUp()
        self._dialect_name = "postgresql"
        self._postgres = Postgresql()
        self._engine_url = self._postgres.url()


class MySQLTestCase(DatabaseContextTestCase, unittest.TestCase):
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


class MockTestCase(DatabaseContextTestCase, unittest.TestCase):
    """Tests of mock database context."""

    def setUp(self) -> None:
        super().setUp()
        # This URL should result in a mock connection being setup.
        self._engine_url = "sqlite://"


class BadEngineTestCase(unittest.TestCase):
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
