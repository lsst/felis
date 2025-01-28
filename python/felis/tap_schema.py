"""Provides utilities for creating and populating the TAP_SCHEMA database."""

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

import logging
import os
import re
from typing import Any

from lsst.resources import ResourcePath
from sqlalchemy import MetaData, Table, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.dml import Insert

from felis import datamodel
from felis.datamodel import Schema
from felis.db.utils import is_valid_engine
from felis.metadata import MetaDataBuilder

from .types import FelisType

__all__ = ["TableManager", "DataLoader"]

logger = logging.getLogger(__name__)


class TableManager:
    """Manage creation of TAP_SCHEMA tables.

    Parameters
    ----------
    engine
        The SQLAlchemy engine for reflecting the TAP_SCHEMA tables from an
        existing database.
        This can be a mock connection or None, in which case the internal
        TAP_SCHEMA schema will be used by loading an internal YAML file.
    schema_name
        The name of the schema to use for the TAP_SCHEMA tables.
        Leave as None to use the standard name of "TAP_SCHEMA".
    apply_schema_to_metadata
        If True, apply the schema to the metadata as well as the tables.
        If False, these will be set to None, e.g., for sqlite.
    table_name_postfix
        A string to append to all the standard table names.
        This needs to be used in a way such that the resultant table names
        map to tables within the TAP_SCHEMA database.

    Notes
    -----
    The TAP_SCHEMA schema must either have been created already, in which case
    the ``engine`` should be provided. Or the internal TAP_SCHEMA schema will
    be used if ``engine`` is None or a ``MockConnection``.
    """

    _TABLE_NAMES_STD = ["schemas", "tables", "columns", "keys", "key_columns"]
    """The standard table names for the TAP_SCHEMA tables."""

    _SCHEMA_NAME_STD = "TAP_SCHEMA"
    """The standard schema name for the TAP_SCHEMA tables."""

    def __init__(
        self,
        engine: Engine | MockConnection | None = None,
        schema_name: str | None = None,
        apply_schema_to_metadata: bool = True,
        table_name_postfix: str = "",
    ):
        """Initialize the table manager."""
        self.table_name_postfix = table_name_postfix
        self.apply_schema_to_metadata = apply_schema_to_metadata
        self.schema_name = schema_name or TableManager._SCHEMA_NAME_STD
        self.table_name_postfix = table_name_postfix

        if is_valid_engine(engine):
            assert isinstance(engine, Engine)
            if table_name_postfix != "":
                logger.warning(
                    "Table name postfix '%s' will be ignored when reflecting TAP_SCHEMA database",
                    table_name_postfix,
                )
            logger.debug(
                "Reflecting TAP_SCHEMA database from existing database at %s",
                engine.url._replace(password="***"),
            )
            self._reflect(engine)
        else:
            self._load_yaml()

        self._create_table_map()
        self._check_tables()

    def _reflect(self, engine: Engine) -> None:
        """Reflect the TAP_SCHEMA database tables into the metadata.

        Parameters
        ----------
        engine
            The SQLAlchemy engine to use to reflect the tables.
        """
        self._metadata = MetaData(schema=self.schema_name if self.apply_schema_to_metadata else None)
        try:
            self.metadata.reflect(bind=engine)
        except SQLAlchemyError as e:
            logger.error("Error reflecting TAP_SCHEMA database: %s", e)
            raise

    def _load_yaml(self) -> None:
        """Load the standard TAP_SCHEMA schema from a Felis package
        resource.
        """
        self._load_schema()
        if self.schema_name != TableManager._SCHEMA_NAME_STD:
            self.schema.name = self.schema_name
        else:
            self.schema_name = self.schema.name

        self._metadata = MetaDataBuilder(
            self.schema,
            apply_schema_to_metadata=self.apply_schema_to_metadata,
            table_name_postfix=self.table_name_postfix,
        ).build()

        logger.debug("Loaded TAP_SCHEMA '%s' from YAML resource", self.schema_name)

    def __getitem__(self, table_name: str) -> Table:
        """Get one of the TAP_SCHEMA tables by its standard TAP_SCHEMA name.

        Parameters
        ----------
        table_name
            The name of the table to get.

        Returns
        -------
        Table
            The table with the given name.

        Notes
        -----
        This implements array semantics for the table manager, allowing
        tables to be accessed by their standard TAP_SCHEMA names.
        """
        if table_name not in self._table_map:
            raise KeyError(f"Table '{table_name}' not found in table map")
        return self.metadata.tables[self._table_map[table_name]]

    @property
    def schema(self) -> Schema:
        """Get the TAP_SCHEMA schema.

        Returns
        -------
        Schema
            The TAP_SCHEMA schema.

        Notes
        -----
        This will only be set if the TAP_SCHEMA schema was loaded from a
        Felis package resource. In the case where the TAP_SCHEMA schema was
        reflected from an existing database, this will be None.
        """
        return self._schema

    @property
    def metadata(self) -> MetaData:
        """Get the metadata for the TAP_SCHEMA tables.

        Returns
        -------
        `~sqlalchemy.sql.schema.MetaData`
            The metadata for the TAP_SCHEMA tables.

        Notes
        -----
        This will either be the metadata that was reflected from an existing
        database or the metadata that was loaded from a Felis package resource.
        """
        return self._metadata

    @classmethod
    def get_tap_schema_std_path(cls) -> str:
        """Get the path to the standard TAP_SCHEMA schema resource.

        Returns
        -------
        str
            The path to the standard TAP_SCHEMA schema resource.
        """
        return os.path.join(os.path.dirname(__file__), "schemas", "tap_schema_std.yaml")

    @classmethod
    def get_tap_schema_std_resource(cls) -> ResourcePath:
        """Get the standard TAP_SCHEMA schema resource.

        Returns
        -------
        `~lsst.resources.ResourcePath`
            The standard TAP_SCHEMA schema resource.
        """
        return ResourcePath("resource://felis/schemas/tap_schema_std.yaml")

    @classmethod
    def get_table_names_std(cls) -> list[str]:
        """Get the standard column names for the TAP_SCHEMA tables.

        Returns
        -------
        list
            The standard table names for the TAP_SCHEMA tables.
        """
        return cls._TABLE_NAMES_STD

    @classmethod
    def get_schema_name_std(cls) -> str:
        """Get the standard schema name for the TAP_SCHEMA tables.

        Returns
        -------
        str
            The standard schema name for the TAP_SCHEMA tables.
        """
        return cls._SCHEMA_NAME_STD

    @classmethod
    def load_schema_resource(cls) -> Schema:
        """Load the standard TAP_SCHEMA schema from a Felis package
        resource into a Felis `~felis.datamodel.Schema`.

        Returns
        -------
        Schema
            The TAP_SCHEMA schema.
        """
        rp = cls.get_tap_schema_std_resource()
        return Schema.from_uri(rp, context={"id_generation": True})

    def _load_schema(self) -> None:
        """Load the TAP_SCHEMA schema from a Felis package resource."""
        self._schema = self.load_schema_resource()

    def _create_table_map(self) -> None:
        """Create a mapping of standard table names to the table names modified
        with a postfix, as well as the prepended schema name if it is set.

        Returns
        -------
        dict
            A dictionary mapping the standard table names to the modified
            table names.

        Notes
        -----
        This is a private method that is called during initialization, allowing
        us to use table names like ``schemas11`` such as those used by the CADC
        TAP library instead of the standard table names. It also maps between
        the standard table names and those with the schema name prepended like
        SQLAlchemy uses.
        """
        self._table_map = {
            table_name: (
                f"{self.schema_name + '.' if self.apply_schema_to_metadata else ''}"
                f"{table_name}{self.table_name_postfix}"
            )
            for table_name in TableManager.get_table_names_std()
        }
        logger.debug(f"Created TAP_SCHEMA table map: {self._table_map}")

    def _check_tables(self) -> None:
        """Check that there is a valid mapping to each standard table.

        Raises
        ------
        KeyError
            If a table is missing from the table map.
        """
        for table_name in TableManager.get_table_names_std():
            self[table_name]

    def _create_schema(self, engine: Engine) -> None:
        """Create the database schema for TAP_SCHEMA if it does not already
        exist.

        Parameters
        ----------
        engine
            The SQLAlchemy engine to use to create the schema.

        Notes
        -----
        This method only creates the schema in the database. It does not create
        the tables.
        """
        create_schema_functions = {
            "postgresql": self._create_schema_postgresql,
            "mysql": self._create_schema_mysql,
        }

        dialect_name = engine.dialect.name
        if dialect_name == "sqlite":
            # SQLite doesn't have schemas.
            return

        create_function = create_schema_functions.get(dialect_name)

        if create_function:
            with engine.begin() as connection:
                create_function(connection)
        else:
            # Some other database engine we don't currently know how to handle.
            raise NotImplementedError(
                f"Database engine '{engine.dialect.name}' is not supported for schema creation"
            )

    def _create_schema_postgresql(self, connection: Connection) -> None:
        """Create the schema in a PostgreSQL database.

        Parameters
        ----------
        connection
            The SQLAlchemy connection to use to create the schema.
        """
        connection.execute(CreateSchema(self.schema_name, if_not_exists=True))

    def _create_schema_mysql(self, connection: Connection) -> None:
        """Create the schema in a MySQL database.

        Parameters
        ----------
        connection
            The SQLAlchemy connection to use to create the schema.
        """
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.schema_name}"))

    def initialize_database(self, engine: Engine) -> None:
        """Initialize a database with the TAP_SCHEMA tables.

        Parameters
        ----------
        engine
            The SQLAlchemy engine to use to create the tables.
        """
        logger.info("Creating TAP_SCHEMA database '%s'", self.schema_name)
        self._create_schema(engine)
        self.metadata.create_all(engine)


class DataLoader:
    """Load data into the TAP_SCHEMA tables.

    Parameters
    ----------
    schema
        The Felis ``Schema`` to load into the TAP_SCHEMA tables.
    mgr
        The table manager that contains the TAP_SCHEMA tables.
    engine
        The SQLAlchemy engine to use to connect to the database.
    tap_schema_index
        The index of the schema in the TAP_SCHEMA database.
    output_path
        The file to write the SQL statements to. If None, printing will be
        suppressed.
    print_sql
        If True, print the SQL statements that will be executed.
    dry_run
        If True, the data will not be loaded into the database.
    """

    def __init__(
        self,
        schema: Schema,
        mgr: TableManager,
        engine: Engine | MockConnection,
        tap_schema_index: int = 0,
        output_path: str | None = None,
        print_sql: bool = False,
        dry_run: bool = False,
    ):
        self.schema = schema
        self.mgr = mgr
        self.engine = engine
        self.tap_schema_index = tap_schema_index
        self.inserts: list[Insert] = []
        self.output_path = output_path
        self.print_sql = print_sql
        self.dry_run = dry_run

    def load(self) -> None:
        """Load the schema data into the TAP_SCHEMA tables.

        Notes
        -----
        This will generate inserts for the data, print the SQL statements if
        requested, save the SQL statements to a file if requested, and load the
        data into the database if not in dry run mode. These are done as
        sequential operations rather than for each insert. The logic is that
        the user may still want the complete SQL output to be printed or saved
        to a file even if loading into the database causes errors. If there are
        errors when inserting into the database, the SQLAlchemy error message
        should indicate which SQL statement caused the error.
        """
        self._generate_all_inserts()
        if self.print_sql:
            # Print to stdout.
            self._print_sql()
        if self.output_path:
            # Print to an output file.
            self._write_sql_to_file()
        if not self.dry_run:
            # Execute the inserts if not in dry run mode.
            self._execute_inserts()
        else:
            logger.info("Dry run - not loading data into database")

    def _insert_schemas(self) -> None:
        """Insert the schema data into the schemas table."""
        schema_record = {
            "schema_name": self.schema.name,
            "utype": self.schema.votable_utype,
            "description": self.schema.description,
            "schema_index": self.tap_schema_index,
        }
        self._insert("schemas", schema_record)

    def _get_table_name(self, table: datamodel.Table) -> str:
        """Get the name of the table with the schema name prepended.

        Parameters
        ----------
        table
            The table to get the name for.

        Returns
        -------
        str
            The name of the table with the schema name prepended.
        """
        return f"{self.schema.name}.{table.name}"

    def _insert_tables(self) -> None:
        """Insert the table data into the tables table."""
        for table in self.schema.tables:
            table_record = {
                "schema_name": self.schema.name,
                "table_name": self._get_table_name(table),
                "table_type": "table",
                "utype": table.votable_utype,
                "description": table.description,
                "table_index": 0 if table.tap_table_index is None else table.tap_table_index,
            }
            self._insert("tables", table_record)

    def _insert_columns(self) -> None:
        """Insert the column data into the columns table."""
        for table in self.schema.tables:
            for column in table.columns:
                felis_type = FelisType.felis_type(column.datatype.value)
                arraysize = str(column.votable_arraysize) if column.votable_arraysize else None
                size = DataLoader._get_size(column)
                indexed = DataLoader._is_indexed(column, table)
                tap_column_index = column.tap_column_index
                unit = column.ivoa_unit or column.fits_tunit

                column_record = {
                    "table_name": self._get_table_name(table),
                    "column_name": column.name,
                    "datatype": felis_type.votable_name,
                    "arraysize": arraysize,
                    "size": size,
                    "xtype": column.votable_xtype,
                    "description": column.description,
                    "utype": column.votable_utype,
                    "unit": unit,
                    "ucd": column.ivoa_ucd,
                    "indexed": indexed,
                    "principal": column.tap_principal,
                    "std": column.tap_std,
                    "column_index": tap_column_index,
                }
                self._insert("columns", column_record)

    def _insert_keys(self) -> None:
        """Insert the foreign keys into the keys and key_columns tables."""
        for table in self.schema.tables:
            for constraint in table.constraints:
                if isinstance(constraint, datamodel.ForeignKeyConstraint):
                    # Handle keys table
                    referenced_column = self.schema.find_object_by_id(
                        constraint.referenced_columns[0], datamodel.Column
                    )
                    referenced_table = self.schema.get_table_by_column(referenced_column)
                    key_record = {
                        "key_id": constraint.name,
                        "from_table": self._get_table_name(table),
                        "target_table": self._get_table_name(referenced_table),
                        "description": constraint.description,
                        "utype": constraint.votable_utype,
                    }
                    self._insert("keys", key_record)

                    # Handle key_columns table
                    from_column = self.schema.find_object_by_id(constraint.columns[0], datamodel.Column)
                    target_column = self.schema.find_object_by_id(
                        constraint.referenced_columns[0], datamodel.Column
                    )
                    key_columns_record = {
                        "key_id": constraint.name,
                        "from_column": from_column.name,
                        "target_column": target_column.name,
                    }
                    self._insert("key_columns", key_columns_record)

    def _generate_all_inserts(self) -> None:
        """Generate the inserts for all the data."""
        self.inserts.clear()
        self._insert_schemas()
        self._insert_tables()
        self._insert_columns()
        self._insert_keys()
        logger.debug("Generated %d insert statements", len(self.inserts))

    def _execute_inserts(self) -> None:
        """Load the `~felis.datamodel.Schema` data into the TAP_SCHEMA
        tables.
        """
        if isinstance(self.engine, Engine):
            with self.engine.connect() as connection:
                transaction = connection.begin()
                try:
                    for insert in self.inserts:
                        connection.execute(insert)
                    transaction.commit()
                except Exception as e:
                    logger.error("Error loading data into database: %s", e)
                    transaction.rollback()
                    raise

    def _compiled_inserts(self) -> list[str]:
        """Compile the inserts to SQL.

        Returns
        -------
        list
            A list of the compiled insert statements.
        """
        return [
            str(insert.compile(self.engine, compile_kwargs={"literal_binds": True}))
            for insert in self.inserts
        ]

    def _print_sql(self) -> None:
        """Print the generated inserts to stdout."""
        for insert_str in self._compiled_inserts():
            print(insert_str + ";")

    def _write_sql_to_file(self) -> None:
        """Write the generated insert statements to a file."""
        if not self.output_path:
            raise ValueError("No output path specified")
        with open(self.output_path, "w") as outfile:
            for insert_str in self._compiled_inserts():
                outfile.write(insert_str + ";" + "\n")

    def _insert(self, table_name: str, record: list[Any] | dict[str, Any]) -> None:
        """Generate an insert statement for a record.

        Parameters
        ----------
        table_name
            The name of the table to insert the record into.
        record
            The record to insert into the table.
        """
        table = self.mgr[table_name]
        insert_statement = table.insert().values(record)
        self.inserts.append(insert_statement)

    @staticmethod
    def _get_size(column: datamodel.Column) -> int | None:
        """Get the size of the column.

        Parameters
        ----------
        column
            The column to get the size for.

        Returns
        -------
        int or None
            The size of the column or None if not applicable.
        """
        arraysize = column.votable_arraysize

        if not arraysize:
            return None

        arraysize_str = str(arraysize)
        if arraysize_str.isdigit():
            return int(arraysize_str)

        match = re.match(r"^([0-9]+)\*$", arraysize_str)
        if match and match.group(1) is not None:
            return int(match.group(1))

        return None

    @staticmethod
    def _is_indexed(column: datamodel.Column, table: datamodel.Table) -> int:
        """Check if the column is indexed in the table.

        Parameters
        ----------
        column
            The column to check.
        table
            The table to check.

        Returns
        -------
        int
            1 if the column is indexed, 0 otherwise.
        """
        if isinstance(table.primary_key, str) and table.primary_key == column.id:
            return 1
        for index in table.indexes:
            if index.columns and len(index.columns) == 1 and index.columns[0] == column.id:
                return 1
        return 0
