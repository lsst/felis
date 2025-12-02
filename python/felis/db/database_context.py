"""API for managing database operations across different dialects."""

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

from __future__ import annotations

import logging
from abc import abstractmethod
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import IO, Any, Literal, TypeAlias

from sqlalchemy import (
    Engine,
    MetaData,
    create_engine,
    inspect,
    make_url,
    quoted_name,
)
from sqlalchemy.engine import (
    Connection,
    Dialect,
    Result,
)
from sqlalchemy.engine.mock import MockConnection, create_mock_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import (
    CreateSchema,
    DropSchema,
)
from sqlalchemy.sql import (
    Executable,
    text,
)
from sqlalchemy.sql.elements import TextClause

__all__ = [
    "DatabaseContext",
    "DatabaseContextError",
    "MockContext",
    "MySQLContext",
    "PostgreSQLContext",
    "SQLiteContext",
    "create_database_context",
]

logger = logging.getLogger("felis")

SQLStatement = str | Executable | TextClause


def _normalize_statement(statement: SQLStatement) -> Executable | TextClause:
    if isinstance(statement, str):
        return text(statement)
    return statement


def _create_mock_connection(engine_url: str | URL, output_file: IO[str] | None = None) -> MockConnection:
    writer = _SQLWriter(output_file)
    engine = create_mock_engine(engine_url, executor=writer.write, paramstyle="pyformat")
    writer.dialect = engine.dialect
    return engine


def _dialect_name(url: URL) -> str:
    dialect_name = url.drivername
    # Normalize dialect name (e.g., "postgresql+psycopg2" -> "postgresql")
    if "+" in dialect_name:
        dialect_name = dialect_name.split("+")[0]
    return dialect_name


def _clear_schema(metadata: MetaData) -> None:
    if metadata.schema:
        metadata.schema = None
        for table in metadata.tables.values():
            table.schema = None


def _get_existing_indexes(inspector: Any, table_name: str, schema: str | None) -> set[str]:
    return {
        ix["name"]
        for ix in inspector.get_indexes(table_name, schema=schema)
        if "name" in ix and ix["name"] is not None
    }


def is_mock_url(url: URL) -> bool:
    """Check if the engine URL points to a mock connection.

    Parameters
    ----------
    url
        The SQLAlchemy engine URL.

    Returns
    -------
    bool
        True if the URL is a mock URL, False otherwise.
    """
    return (url.drivername == "sqlite" and url.database is None) or (
        url.drivername != "sqlite" and url.host is None
    )


def is_sqlite_url(url: URL | str) -> bool:
    """Check if the engine URL points to a SQLite database.

    Parameters
    ----------
    url
        The SQLAlchemy engine URL or string.

    Returns
    -------
    bool
        True if the URL is a SQLite URL, False otherwise.
    """
    if isinstance(url, str):
        url = make_url(url)
    return url.drivername.startswith("sqlite")


class DatabaseContextError(Exception):
    """Exception raised for errors in the DatabaseContext operations."""


class DatabaseContext(AbstractContextManager):
    """Interface for managing database operations across different
    SQL dialects.
    """

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Exit the context manager and clean up resources."""
        try:
            self.close()
        except Exception:
            logger.exception("Error during cleanup of database context")
        return False

    @abstractmethod
    def close(self) -> None:
        """Close and clean up database resources."""
        ...

    @property
    @abstractmethod
    def metadata(self) -> MetaData:
        """The SQLAlchemy metadata representing the database for the context
        (`~sqlalchemy.sql.schema.MetaData`).
        """
        ...

    @property
    @abstractmethod
    def engine(self) -> Engine:
        """The SQAlchemy engine for the context
        (`~sqlalchemy.engine.Engine`).
        """
        ...

    @property
    @abstractmethod
    def dialect(self) -> Dialect:
        """The SQLAlchemy dialect for the context
        (`~sqlalchemy.engine.Dialect`).
        """
        ...

    @property
    @abstractmethod
    def dialect_name(self) -> str:
        """Get the dialect name for this database context (``str``)."""
        ...

    @abstractmethod
    def initialize(self) -> None:
        """Create the target schema in the database if it does not exist
        already.

        Sub-classes should implement idempotent behavior so that calling this
        method multiple times has no adverse effects. If the schema already
        exists, the method should simply return without raising an error. (A
        warning message may be logged in this case.)

        Raises
        ------
        DatabaseContextError
            If there is an error instantiating the schema.
        """
        ...

    @abstractmethod
    def drop(self) -> None:
        """Drop the schema in the database if it exists.

        Implementations should use ``IF EXISTS`` semantics to avoid raising
        an error if the schema does not exist.

        Raises
        ------
        DatabaseContextError
            If there is an error dropping the schema.
        """
        ...

    @abstractmethod
    def create_all(self) -> None:
        """Create all database objects in the schema using the metadata
        object.

        Raises
        ------
        DatabaseContextError
            If there is an error creating the schema objects in the database.
        """
        ...

    @abstractmethod
    def create_indexes(self) -> None:
        """Create all indexes in the schema using the metadata object.

        Raises
        ------
        DatabaseContextError
            If there is an error creating the indexes in the database.
        """
        ...

    @abstractmethod
    def drop_indexes(self) -> None:
        """Drop all indexes in the schema using the metadata object.

        Raises
        ------
        DatabaseContextError
            If there is an error dropping the indexes in the database.
        """
        ...

    @abstractmethod
    def execute(self, statement: SQLStatement, parameters: dict[str, Any] | None = None) -> Result:
        """Execute a SQL statement and return the result.

        Parameters
        ----------
        statement
            The SQL statement to execute.

        Returns
        -------
        `~sqlalchemy.engine.Result`
            The result of the statement execution.

        Raises
        ------
        DatabaseContextError
            If there is an error executing the SQL statement.
        """
        ...


class _BaseContext(DatabaseContext):
    """Base database context providing common behavior.

    Parameters
    ----------
    engine_url
        The SQLAlchemy engine for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    require_schema
        True if a valid schema name is required on the MetaData, False if not.
    """

    # Subclasses should set this to the dialect name.
    DIALECT: str

    def __init__(self, engine_url: URL, metadata: MetaData, require_schema: bool = False) -> None:
        self._engine_url = engine_url
        self._metadata = metadata
        self._schema_name: str | None = metadata.schema
        self._engine: Engine | None = None
        self._echo: bool = False

        # Check that the URL dialect matches this context's expected dialect
        self._validate_dialect(engine_url)

        # Ensure the schema name is set for dialects that require it
        if require_schema and self._schema_name is None:
            raise DatabaseContextError(f"Schema name must be set for context: {self.dialect_name}")

    @property
    def echo(self) -> bool:
        """Whether to log all SQL statements executed by the engine
        (``bool``).
        """
        return self._echo

    @echo.setter
    def echo(self, value: bool) -> None:
        self._echo = value
        if self.engine is not None:
            self.engine.echo = value

    @classmethod
    def _validate_dialect(cls, engine_url: URL) -> None:
        """Validate that the engine dialect matches this context's expected
        dialect.

        Parameters
        ----------
        engine_url
            The SQLAlchemy database URL to validate.

        Raises
        ------
        DatabaseContextError
            If the engine dialect doesn't match the context's expected dialect.
        """
        # Normalize both the engine dialect and expected dialect for comparison
        engine_dialect = _dialect_name(engine_url)
        expected_dialect = cls.DIALECT.lower()

        if engine_dialect != expected_dialect:
            raise DatabaseContextError(
                f"Engine dialect '{engine_dialect}' does not match the context's expected dialect: "
                f"{expected_dialect}"
            )

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self._engine_url)
        return self._engine

    @property
    def metadata(self) -> MetaData:
        return self._metadata

    @property
    def dialect(self) -> Dialect:
        return self.engine.dialect

    @property
    def dialect_name(self) -> str:
        """Get the dialect name for this database context.

        Returns
        -------
        str
            The normalized dialect name.
        """
        return self.DIALECT

    @property
    def schema_name(self) -> str | None:
        """Effective schema name for this context (may be None).

        Returns
        -------
        str | None
            The schema name, or None if no schema is set.
        """
        return self._schema_name

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        """Context manager for database connection."""
        with self.engine.connect() as connection:
            yield connection

    def execute(self, statement: SQLStatement, parameters: dict[str, Any] | None = None) -> Result:
        statement = _normalize_statement(statement)
        try:
            with self.connect() as conn:
                with conn.begin():
                    if parameters:
                        result = conn.execute(statement, parameters)
                    else:
                        result = conn.execute(statement)
                    return result
        except SQLAlchemyError as e:
            raise DatabaseContextError(f"Error executing statement: {e}") from e

    def create_all(self) -> None:
        with self.connect() as conn:
            with conn.begin():
                try:
                    self.metadata.create_all(bind=conn)
                except SQLAlchemyError as e:
                    raise DatabaseContextError(f"Error creating database: {e}") from e

    def _manage_indexes(self, action: str) -> None:
        """Manage indexes by creating or dropping them.

        Parameters
        ----------
        action
            The action to perform, either "create" or "drop".

        Raises
        ------
        DatabaseContextError
            If there is an error managing the indexes in the database.
        """
        with self.connect() as conn:
            with conn.begin():
                try:
                    inspector = inspect(conn)
                    for table in self.metadata.tables.values():
                        # Fetch all existing indexes for this table once
                        existing_indexes = _get_existing_indexes(inspector, table.name, self.schema_name)

                        for index in table.indexes:
                            if index.name is None:
                                # Anonymous indexes can't be checked by name
                                logger.warning(f"Skipping anonymous index on table '{table.name}'")
                                continue

                            if action == "create":
                                if index.name in existing_indexes:
                                    logger.warning(
                                        f"Skipping creation of index '{index.name}' which already exists"
                                    )
                                    continue
                                index.create(bind=conn, checkfirst=False)  # We already checked
                                logger.info(f"Created index '{index.name}'")
                            elif action == "drop":
                                if index.name not in existing_indexes:
                                    logger.warning(f"Skipping index '{index.name}' which does not exist")
                                    continue
                                index.drop(bind=conn, checkfirst=False)  # We already checked
                                logger.info(f"Dropped index '{index.name}'")
                            else:
                                raise ValueError(f"Invalid action '{action}'. Must be 'create' or 'drop'.")
                except SQLAlchemyError as e:
                    raise DatabaseContextError(f"Error {action}ing indexes: {e}") from e

    def create_indexes(self) -> None:
        """Create all indexes in the schema using the metadata object.

        Raises
        ------
        DatabaseContextError
            If there is an error creating the indexes in the database.
        """
        self._manage_indexes("create")

    def drop_indexes(self) -> None:
        """Drop all indexes in the schema using the metadata object.

        Raises
        ------
        DatabaseContextError
            If there is an error dropping the indexes in the database.
        """
        self._manage_indexes("drop")

    def _required_schema_name(self) -> str:
        """Return the schema name, ensuring that it is set.

        This is mainly here for typing purposes, because the schema_name
        property may be None, and mypy doesn't understand that we already
        checked it during initialization.
        """
        if self.schema_name is None:
            raise DatabaseContextError("Schema name is required but not set.")
        return self.schema_name

    def close(self) -> None:
        """Close and dispose of the database engine."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


_ContextClass: TypeAlias = type[_BaseContext]
_ContextDecorator: TypeAlias = Callable[[_ContextClass], _ContextClass]


class DatabaseContextFactory:
    """Factory for creating DatabaseContext instances based on dialect type."""

    _registry: dict[str, _ContextClass] = {}

    @classmethod
    def register(cls) -> _ContextDecorator:
        """Register a context class for its dialect.

        The dialect is determined by reading the DIALECT attribute from the
        decorated class.

        Returns
        -------
        Callable
            The decorator function that registers the context class.

        Examples
        --------
        >>> @DatabaseContextFactory.register()
        ... class PostgreSQLContext(_BaseContext):
        ...     DIALECT = "postgresql"
        ...     pass

        Notes
        -----
        The registry is populated at module import time and afterwards should
        be treated as read-only.
        """

        def decorator(context_class: type[_BaseContext]) -> type[_BaseContext]:
            # Get the dialect from the class's DIALECT attribute
            if not hasattr(context_class, "DIALECT"):
                raise ValueError(f"Context class {context_class.__name__} must define a DIALECT attribute")
            cls._registry[context_class.DIALECT] = context_class
            return context_class

        return decorator

    @classmethod
    def register_class(cls, dialect: str, context_class: type[_BaseContext]) -> None:
        """Register a context class for a specific dialect programmatically.

        Parameters
        ----------
        dialect
            The dialect name to register.
        context_class
            The context class to use for this dialect.
        """
        dialect_name = dialect.lower()
        if "+" in dialect_name:
            dialect_name = dialect_name.split("+")[0]
        cls._registry[dialect_name] = context_class

    @classmethod
    def create_context(cls, dialect: str, engine_url: URL, metadata: MetaData) -> DatabaseContext:
        """Create a context instance for the given dialect.

        Parameters
        ----------
        dialect
            The database dialect name.
        engine_url
            The SQLAlchemy database URL.
        metadata
            The SQLAlchemy metadata.

        Returns
        -------
        DatabaseContext
            The appropriate context instance.

        Raises
        ------
        ValueError
            If no context class is registered for the dialect.
        """
        dialect_name = dialect.lower()
        if "+" in dialect_name:
            dialect_name = dialect_name.split("+")[0]

        if dialect_name not in cls._registry:
            supported = cls.get_supported_dialects()
            raise ValueError(
                f"No context class registered for dialect: {dialect_name}. "
                f"Supported dialects: {', '.join(supported)}"
            )

        context_class = cls._registry[dialect_name]
        return context_class(engine_url, metadata)

    @classmethod
    def get_supported_dialects(cls) -> list[str]:
        """Get a list of supported dialect names.

        Returns
        -------
        list[str]
            List of supported dialect names.
        """
        return list(cls._registry.keys())


class _SQLWriter:
    """Write SQL statements to stdout or a file.

    Parameters
    ----------
    file
        The file to write the SQL statements to. If None, the statements
        will be written to stdout.
    """

    def __init__(self, file: IO[str] | None = None) -> None:
        """Initialize the SQL writer."""
        self.file = file
        self.dialect: Dialect | None = None

    def write(self, sql: Any, *multiparams: Any, **params: Any) -> None:
        """Write the SQL statement to a file or stdout.

        Statements with parameters will be formatted with the values
        inserted into the resultant SQL output.

        Parameters
        ----------
        sql
            The SQL statement to write.
        *multiparams
            The multiparams to use for the SQL statement.
        **params
            The params to use for the SQL statement.

        Notes
        -----
        The functions arguments are typed very loosely because this method in
        SQLAlchemy is untyped, amd we do not call it directly.
        """
        compiled = sql.compile(dialect=self.dialect)
        sql_str = str(compiled) + ";"
        params_list = [compiled.params]
        for params in params_list:
            if not params:
                print(sql_str, file=self.file)
                continue
            new_params = {}
            for key, value in params.items():
                if isinstance(value, str):
                    new_params[key] = f"'{value}'"
                elif value is None:
                    new_params[key] = "null"
                else:
                    new_params[key] = value
            print(sql_str % new_params, file=self.file)


@DatabaseContextFactory.register()
class PostgreSQLContext(_BaseContext):
    """Database context for Postgres.

    Parameters
    ----------
    engine_url
        The SQLAlchemy database URL for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    """

    DIALECT = "postgresql"

    def __init__(self, engine_url: URL, metadata: MetaData):
        super().__init__(engine_url, metadata, require_schema=True)

    def initialize(self) -> None:
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Checking if PG schema exists: {schema_name}")
            result = self.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = :schema_name
                """,
                {"schema_name": schema_name},
            )
            if result.fetchone():
                return
            logger.debug(f"Creating PG schema: {schema_name}")
            self.execute(CreateSchema(schema_name))
        except SQLAlchemyError as e:
            raise DatabaseContextError(f"Error initializing Postgres schema: {e}") from e

    def drop(self) -> None:
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Dropping PostgreSQL schema if exists: {schema_name}")
            self.execute(DropSchema(schema_name, if_exists=True, cascade=True))
        except SQLAlchemyError as e:
            raise DatabaseContextError(f"Error dropping Postgres database: {e}") from e


@DatabaseContextFactory.register()
class MySQLContext(_BaseContext):
    """Database context for MySQL.

    Parameters
    ----------
    engine_url
        The SQLAlchemy database URL for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    """

    DIALECT = "mysql"

    def __init__(self, engine_url: URL, metadata: MetaData):
        super().__init__(engine_url, metadata, require_schema=True)

    def initialize(self) -> None:
        # The schema is instantiated as a database, as MySQL does not have a
        # distinct schema concept, unlike Postgres.
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Checking if MySQL database exists: {schema_name}")
            result = self.execute("SHOW DATABASES LIKE :schema_name", {"schema_name": schema_name})
            if result.fetchone():
                return
            logger.debug(f"Creating MySQL database: {schema_name}")
            from sqlalchemy import DDL

            create_stmt = DDL(f"CREATE DATABASE {quoted_name(schema_name, quote=True)}")
            self.execute(create_stmt)
        except SQLAlchemyError as e:
            raise DatabaseContextError(f"Error initializing MySQL database: {e}") from e

    def drop(self) -> None:
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Dropping MySQL database if exists: {schema_name}")
            from sqlalchemy import DDL

            drop_stmt = DDL(f"DROP DATABASE IF EXISTS {quoted_name(schema_name, quote=True)}")
            self.execute(drop_stmt)
        except SQLAlchemyError as e:
            raise DatabaseContextError(f"Error dropping MySQL database: {e}") from e


@DatabaseContextFactory.register()
class SQLiteContext(_BaseContext):
    """Database context for SQLite.

    Parameters
    ----------
    engine_url
        The SQLAlchemy database URL for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    """

    DIALECT = "sqlite"

    def __init__(self, engine_url: URL, metadata: MetaData):
        # Schema name needs to be cleared, if set.
        _clear_schema(metadata)
        # Schema name is not required.
        super().__init__(engine_url, metadata)

    def initialize(self) -> None:
        # Nothing needs to be done for SQLite initialization.
        return

    def drop(self) -> None:
        try:
            logger.debug("Dropping tables in SQLite schema")
            # Drop all the tables in the database file.
            self.metadata.drop_all(bind=self.engine)
        except SQLAlchemyError as e:
            raise DatabaseContextError(f"Error dropping SQLite database: {e}") from e


class MockContext(DatabaseContext):
    """Database context for a mock connection.

    Parameters
    ----------
    metadata
        The SQLAlchemy metadata defining the database objects.
    connection
        The SQLAlchemy mock connection.
    """

    def __init__(self, metadata: MetaData, connection: MockConnection):
        self._metadata = metadata
        self._connection = connection
        self._dialect = connection.dialect

    @property
    def dialect(self) -> Dialect:
        return self._dialect

    @property
    def dialect_name(self) -> str:
        return self.dialect.name

    @property
    def metadata(self) -> MetaData:
        return self._metadata

    @property
    def engine(self) -> Engine:
        raise DatabaseContextError("MockContext does not provide an engine.")

    def initialize(self) -> None:
        # Mock connection doesn't do any initialization.
        pass

    def drop(self) -> None:
        # Mock connection doesn't drop.
        pass

    def create_all(self) -> None:
        self._metadata.create_all(self._connection)

    def create_indexes(self) -> None:
        # Mock connection can't create indexes.
        pass

    def drop_indexes(self) -> None:
        # Mock connection can't drop indexes.
        pass

    def execute(self, statement: SQLStatement, parameters: dict[str, Any] | None = None) -> Result:
        statement = _normalize_statement(statement)
        if parameters:
            return self._connection.connect().execute(statement, parameters)
        else:
            return self._connection.connect().execute(statement)

    def close(self) -> None:
        """Close the mock connection (no-op)."""
        pass


def create_database_context(
    engine_url: str | URL,
    metadata: MetaData,
    output_file: IO[str] | None = None,
    dry_run: bool = False,
    echo: bool | None = None,
) -> DatabaseContext:
    """Create a DatabaseContext object based on the engine URL.

    Parameters
    ----------
    engine_url
        The database URL for the database connection.
    metadata
        The SQLAlchemy MetaData representing the database objects.
    output_file
        Output file for writing generated SQL commands.
    dry_run
        If True, configure the context to perform a dry run, where operations
        will not be executed.
        If False, use a normal context where operations are executed.
    echo
        If True, the SQLAlchemy engine will log all statements to the console.

    Returns
    -------
    DatabaseContext
        A database context appropriate for the given engine URL. This will be
        a `MockContext` if the URL appears like a mock URL or if ``dry_run`` is
        True, otherwise it will be a context based on the dialect using the
        factory pattern.

    Raises
    ------
    DatabaseContextError
        If the dialect is not supported or if there's an issue creating
        the context.
    """
    if isinstance(engine_url, str):
        engine_url = make_url(engine_url)

    if is_mock_url(engine_url) or dry_run:
        # Use a mock context for mock URLs or dry run mode.
        dialect_name = _dialect_name(engine_url)
        if dialect_name == "sqlite":
            _clear_schema(metadata)
        mock_connection = _create_mock_connection(engine_url, output_file)
        return MockContext(metadata, mock_connection)
    else:
        # Create a real engine and context for the given dialect.
        try:
            dialect_name = _dialect_name(engine_url)

            # Use the factory to create the appropriate context
            try:
                db_ctx = DatabaseContextFactory.create_context(dialect_name, engine_url, metadata)
                if echo is not None:
                    # This is settable for real contexts only.
                    if hasattr(db_ctx, "echo"):
                        db_ctx.echo = echo
                return db_ctx
            except ValueError as e:
                supported = DatabaseContextFactory.get_supported_dialects()
                raise DatabaseContextError(
                    f"Unsupported dialect: {dialect_name}. Supported dialects are: {', '.join(supported)}"
                ) from e

        except Exception as e:
            if isinstance(e, DatabaseContextError):
                raise
            raise DatabaseContextError(f"Failed to create database context: {e}") from e
