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
from collections.abc import Callable
from enum import Enum
from typing import IO, Any, TypeAlias

from sqlalchemy import (
    Engine,
    MetaData,
    create_engine,
    inspect,
    make_url,
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
    Index,
    Table,
)
from sqlalchemy.sql import (
    Executable,
    text,
)
from sqlalchemy.sql.elements import TextClause

__all__ = [
    "DatabaseContext",
    "DatabaseContextError",
    "DatabaseContextFactory",
    "MockContext",
    "MySQLContext",
    "PostgreSQLContext",
    "SQLiteContext",
    "SupportedDialect",
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


def _index_exists(conn: Connection, table: Table, index: Index, schema: str | None) -> bool:
    if index.name is None:
        # Anonymous indexes can't be reliably checked by name.
        return False

    inspector = inspect(conn)
    existing = {
        ix["name"]
        for ix in inspector.get_indexes(
            table_name=table.name,
            schema=schema,
        )
        if "name" in ix and ix["name"] is not None
    }
    return index.name in existing


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


class DatabaseContext:
    """Interface for managing database operations across different
    SQL dialects.
    """

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
        """The SQAlchemy engine for the context (`~sqlalchemy.engine.Engine`).

        Raises
        ------
        DatabaseContextError
            If an engine is not available for this context.
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
        """Get the dialect name for this database context."""
        ...

    @abstractmethod
    def initialize(self) -> None:
        """Create the target schema in the database if it does not exist
        already.

        Raises
        ------
        DatabaseContextError
            If there is an error instantiating the schema.
        """
        ...

    @abstractmethod
    def drop(self) -> None:
        """Drop the schema in the database if it exists.

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
    def execute(self, statement: SQLStatement) -> Result:
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
    engine
        The SQLAlchemy engine for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    require_schema
        True if a valid schema name is required on the MetaData, False if not.
    """

    # Will be set by __init_subclass__
    DIALECT: SupportedDialect

    @classmethod
    def __init_subclass__(cls, /, dialect: SupportedDialect, **kwargs: Any) -> None:
        """Register the dialect for this context subclass.

        Parameters
        ----------
        dialect
            The database dialect this context is for.
        kwargs
            Additional keyword arguments.
        """
        super().__init_subclass__(**kwargs)
        cls.DIALECT = dialect

    def __init__(self, engine: Engine, metadata: MetaData, require_schema: bool = False):
        self._engine = engine
        self._validate_engine_dialect(engine)
        self._dialect = engine.dialect

        self._metadata = metadata
        self._schema_name: str | None = metadata.schema

        # Ensure the schema name is set for dialects that require it.
        if require_schema and self._schema_name is None:
            raise DatabaseContextError(f"Schema name must be set for context: {self.dialect_name}")

    def _validate_engine_dialect(self, engine: Engine) -> None:
        """Validate that the engine dialect matches this context's expected
        dialect.

        Parameters
        ----------
        engine
            The SQLAlchemy engine to validate.

        Raises
        ------
        DatabaseContextError
            If the engine dialect doesn't match the context's expected dialect.
        """
        # Normalize both the engine dialect and expected dialect for comparison
        engine_dialect = _dialect_name(engine.url)
        expected_dialect = self.DIALECT.value.lower()

        if engine_dialect != expected_dialect:
            raise DatabaseContextError(
                f"Engine dialect '{engine_dialect}' does not match the context's expected dialect: "
                f"{expected_dialect}"
            )

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def metadata(self) -> MetaData:
        return self._metadata

    @property
    def dialect(self) -> Dialect:
        return self._dialect

    @property
    def dialect_name(self) -> str:
        """Get the dialect name for this database context.

        Returns
        -------
        str
            The normalized dialect name.
        """
        return self.DIALECT.value

    @property
    def schema_name(self) -> str | None:
        """Effective schema name for this context (may be None).

        Returns
        -------
        str | None
            The schema name, or None if no schema is set.
        """
        return self._schema_name

    def execute(self, statement: SQLStatement) -> Result:
        statement = _normalize_statement(statement)
        try:
            with self.engine.begin() as connection:
                result = connection.execute(statement)
                return result
        except SQLAlchemyError as e:
            raise DatabaseContextError("Error executing statement", e)

    def create_all(self) -> None:
        with self.engine.begin() as conn:
            try:
                self.metadata.create_all(bind=conn)
            except SQLAlchemyError as e:
                raise DatabaseContextError("Error creating database", e)

    def create_indexes(self) -> None:
        with self.engine.begin() as conn:
            try:
                for table in self.metadata.tables.values():
                    for index in table.indexes:
                        if _index_exists(conn, table, index, self.schema_name):
                            logger.warning(f"Skipping creation of index '{index.name}' which already exists")
                            continue
                        index.create(bind=conn, checkfirst=True)
                        logger.info(f"Created index '{index.name}'")
            except SQLAlchemyError as e:
                raise DatabaseContextError("Error creating indexes", e)

    def drop_indexes(self) -> None:
        with self.engine.begin() as conn:
            try:
                for table in self.metadata.tables.values():
                    for index in table.indexes:
                        if not _index_exists(conn, table, index, self.schema_name):
                            logger.warning(f"Skipping index '{index.name}' which does not exist")
                            continue
                        index.drop(bind=conn, checkfirst=True)
                        logger.info(f"Dropped index '{index.name}'")
            except SQLAlchemyError as e:
                raise DatabaseContextError("Error dropping indexes", e)

    def _required_schema_name(self) -> str:
        """Return the schema name, ensuring that it is set.

        This is mainly here for typing purposes, because the schema_name
        property may be None, and mypy doesn't understand that we already
        checked it during initialization.
        """
        if self.schema_name is None:
            raise DatabaseContextError("Schema name is required but not set.")
        return self.schema_name


class SupportedDialect(Enum):
    """Enumeration of supported database dialects."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"

    @classmethod
    def from_string(cls, dialect_name: str) -> SupportedDialect:
        """Create a SupportedDialect from a string, with normalization.

        Parameters
        ----------
        dialect_name
            The dialect name to convert (e.g., "postgresql+psycopg2").

        Returns
        -------
        SupportedDialect
            The corresponding dialect enum value.

        Raises
        ------
        ValueError
            If the dialect is not supported.
        """
        # Normalize dialect name (e.g., "postgresql+psycopg2" -> "postgresql")
        if "+" in dialect_name:
            dialect_name = dialect_name.split("+")[0]

        dialect_name = dialect_name.lower()

        for dialect in cls:
            if dialect.value == dialect_name:
                return dialect

        raise ValueError(f"Unsupported dialect: {dialect_name}")


_ContextClass: TypeAlias = type[_BaseContext]
_ContextDecorator: TypeAlias = Callable[[_ContextClass], _ContextClass]


class DatabaseContextFactory:
    """Factory for creating DatabaseContext instances based on dialect type."""

    _registry: dict[SupportedDialect, _ContextClass] = {}

    @classmethod
    def register(cls, dialect: SupportedDialect) -> _ContextDecorator:
        """Register a context class for a specific dialect.

        Parameters
        ----------
        dialect
            The dialect to register the decorated class for.

        Returns
        -------
        Callable
            The decorator function that registers the context class.

        Examples
        --------
        >>> @DatabaseContextFactory.register(SupportedDialect.POSTGRESQL)
        ... class PostgreSQLContext(_BaseContext):
        ...     pass
        """

        def decorator(context_class: type[_BaseContext]) -> type[_BaseContext]:
            cls._registry[dialect] = context_class
            return context_class

        return decorator

    @classmethod
    def register_class(cls, dialect: SupportedDialect, context_class: type[_BaseContext]) -> None:
        """Register a context class for a specific dialect programmatically.

        Parameters
        ----------
        dialect
            The dialect to register.
        context_class
            The context class to use for this dialect.
        """
        cls._registry[dialect] = context_class

    @classmethod
    def create_context(cls, dialect: SupportedDialect, engine: Engine, metadata: MetaData) -> DatabaseContext:
        """Create a context instance for the given dialect.

        Parameters
        ----------
        dialect
            The database dialect.
        engine
            The SQLAlchemy engine.
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
        if dialect not in cls._registry:
            raise ValueError(f"No context class registered for dialect: {dialect.value}")

        context_class = cls._registry[dialect]
        return context_class(engine, metadata)

    @classmethod
    def get_supported_dialects(cls) -> list[str]:
        """Get a list of supported dialect names.

        Returns
        -------
        list[str]
            List of supported dialect names.
        """
        return [dialect.value for dialect in SupportedDialect]


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


@DatabaseContextFactory.register(SupportedDialect.POSTGRESQL)
class PostgreSQLContext(_BaseContext, dialect=SupportedDialect.POSTGRESQL):
    """Database context for Postgres.

    Parameters
    ----------
    engine
        The SQLAlchemy engine for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    """

    def __init__(self, engine: Engine, metadata: MetaData):
        super().__init__(engine, metadata, require_schema=True)

    def initialize(self) -> None:
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Checking if PG schema exists: {schema_name}")
            result = self.execute(
                f"""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = '{schema_name}'
                """
            )
            if result.fetchone():
                raise ValueError(f"PostgreSQL schema '{schema_name}' already exists.")
            logger.debug(f"Creating PG schema: {schema_name}")
            self.execute(CreateSchema(schema_name))
        except SQLAlchemyError as e:
            raise DatabaseContextError("Error initializing Postgres database", e)

    def drop(self) -> None:
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Dropping PostgreSQL schema if exists: {schema_name}")
            self.execute(DropSchema(schema_name, if_exists=True, cascade=True))
        except SQLAlchemyError as e:
            raise DatabaseContextError("Error dropping Postgres database", e)


@DatabaseContextFactory.register(SupportedDialect.MYSQL)
class MySQLContext(_BaseContext, dialect=SupportedDialect.MYSQL):
    """Database context for MySQL.

    Parameters
    ----------
    engine
        The SQLAlchemy engine for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    """

    def __init__(self, engine: Engine, metadata: MetaData):
        super().__init__(engine, metadata, require_schema=True)

    def initialize(self) -> None:
        # The schema is instantiated as a database, as MySQL does not have a
        # distinct schema concept, unlike Postgres.
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Checking if MySQL database exists: {schema_name}")
            result = self.execute(f"SHOW DATABASES LIKE '{schema_name}'")
            if result.fetchone():
                raise ValueError(f"MySQL database '{schema_name}' already exists.")
            logger.debug(f"Creating MySQL database: {schema_name}")
            self.execute(f"CREATE DATABASE {schema_name}")
        except SQLAlchemyError as e:
            logger.exception(f"Error creating schema: {e}")
            raise

    def drop(self) -> None:
        schema_name = self._required_schema_name()
        try:
            logger.debug(f"Dropping MySQL database if exists: {schema_name}")
            self.execute(f"DROP DATABASE IF EXISTS {schema_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error dropping schema: {e}")
            raise


@DatabaseContextFactory.register(SupportedDialect.SQLITE)
class SQLiteContext(_BaseContext, dialect=SupportedDialect.SQLITE):
    """Database context for SQLite.

    Parameters
    ----------
    engine
        The SQLAlchemy engine for connecting to the database.
    metadata
        The SQLAlchemy metadata representing the database objects.
    """

    def __init__(self, engine: Engine, metadata: MetaData):
        # Schema name needs to be cleared, if set.
        _clear_schema(metadata)
        # Schema name is not required.
        super().__init__(engine, metadata)

    def initialize(self) -> None:
        # Nothing needs to be done for SQLite initialization.
        return

    def drop(self) -> None:
        try:
            logger.debug("Dropping tables in SQLite schema")
            # Drop all the tables in the database file.
            self.metadata.drop_all(bind=self.engine)
        except SQLAlchemyError as e:
            logger.exception(f"Error dropping schema: {e}")
            raise


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
        # Mock connection is a special case which does not provide an engine.
        raise DatabaseContextError("Mock connection does not provide an engine.")

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

    def execute(self, statement: SQLStatement) -> Result:
        statement = _normalize_statement(statement)
        return self._connection.connect().execute(statement)


def create_database_context(
    engine_url: str,
    metadata: MetaData,
    output_file: IO[str] | None = None,
    echo: bool = False,
    dry_run: bool = False,
) -> DatabaseContext:
    """Create a DatabaseContext object based on the engine URL.

    Parameters
    ----------
    engine_url
        The engine URL for the database connection.
    metadata
        The SQLAlchemy MetaData representing the database objects.
    output_file
        Output file for writing generated SQL commands.
    echo
        If True, echo SQL output to the console as it is executed.
        If False, do not echo SQL commands to the console.
    dry_run
        If True, configure the context to perform a dry run, where operations
        will not be executed.
        If False, use a normal context where operations are executed.

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
    url = make_url(engine_url)

    if is_mock_url(url) or dry_run:
        # Use a mock context for mock URLs or dry run mode.
        dialect_name = _dialect_name(url)
        if dialect_name == "sqlite":
            _clear_schema(metadata)
        mock_connection = _create_mock_connection(engine_url, output_file)
        return MockContext(metadata, mock_connection)
    else:
        # Create a real engine and context for the given dialect.
        try:
            engine = create_engine(url, echo=echo)
            dialect_name = _dialect_name(engine.url)

            # Use the factory to create the appropriate context
            try:
                supported_dialect = SupportedDialect.from_string(dialect_name)
                return DatabaseContextFactory.create_context(supported_dialect, engine, metadata)
            except ValueError as e:
                # Provide a more helpful error message with supported dialects
                supported = DatabaseContextFactory.get_supported_dialects()
                raise DatabaseContextError(
                    f"Unsupported dialect: {dialect_name}. Supported dialects are: {', '.join(supported)}"
                ) from e

        except Exception as e:
            if isinstance(e, DatabaseContextError):
                raise
            raise DatabaseContextError(f"Failed to create database context: {e}") from e
