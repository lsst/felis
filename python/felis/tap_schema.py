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

import csv
import io
import logging
import os
import re
from typing import IO, Any

from lsst.resources import ResourcePath
from sqlalchemy import MetaData, Table, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.dml import Insert

from . import datamodel
from .datamodel import Constraint, Schema
from .db.database_context import DatabaseContext, is_sqlite_url
from .metadata import MetaDataBuilder
from .types import FelisType

__all__ = ["DataLoader", "MetadataInserter", "TableManager"]

logger = logging.getLogger(__name__)


class TableManager:
    """Manage TAP_SCHEMA table definitions and access.

    This class provides a streamlined interface for managing TAP_SCHEMA tables,
    automatically handling dialect-specific requirements and providing
    consistent access to TAP_SCHEMA tables through a dictionary-like interface.

    Parameters
    ----------
    engine_url
        Database engine URL for automatic dialect detection and schema
        handling.
    db_context
        Optional database context for reflecting existing TAP_SCHEMA tables.
        If None, loads from internal YAML schema.
    schema_name
        The name of the schema to use for TAP_SCHEMA tables.
        Defaults to "TAP_SCHEMA".
    table_name_postfix
        A string to append to standard table names for customization.
    extensions_path
        Path to additional TAP_SCHEMA table definitions.

    Notes
    -----
    The TableManager automatically detects SQLite vs. schema-supporting
    databases and handles schema application appropriately.
    """

    _TABLE_NAMES_STD = ["schemas", "tables", "columns", "keys", "key_columns"]
    """The standard table names for the TAP_SCHEMA tables."""

    _SCHEMA_NAME_STD = "TAP_SCHEMA"
    """The standard schema name for the TAP_SCHEMA tables."""

    def __init__(
        self,
        engine_url: str | None = None,
        db_context: DatabaseContext | None = None,
        schema_name: str | None = None,
        table_name_postfix: str = "",
        extensions_path: str | None = None,
    ):
        """Initialize the table manager."""
        self.table_name_postfix = table_name_postfix
        self.schema_name = schema_name or self._SCHEMA_NAME_STD
        self.extensions_path = extensions_path

        # Automatic dialect detection from engine URL
        if engine_url is not None:
            self.apply_schema_to_metadata = not is_sqlite_url(engine_url)
        else:
            # Default case: assume SQLite
            engine_url = "sqlite:///:memory:"
            self.apply_schema_to_metadata = False

        if db_context is not None:
            if table_name_postfix != "":
                logger.warning(
                    "Table name postfix '%s' will be ignored when reflecting TAP_SCHEMA database",
                    table_name_postfix,
                )
            logger.debug(
                "Reflecting TAP_SCHEMA database from existing database at %s",
                db_context.engine.url._replace(password="***"),
            )
            self._reflect_from_database(db_context)
        else:
            self._load_from_yaml()

        self._create_table_map()
        self._check_tables()

    def _load_from_yaml(self) -> None:
        """Load TAP_SCHEMA from YAML resources and build metadata."""
        # Load the base schema
        self._schema = self.load_schema_resource()

        # Override schema name if specified
        if self.schema_name != self._SCHEMA_NAME_STD:
            self._schema.name = self.schema_name
        else:
            self.schema_name = self._schema.name

        # Apply any extensions
        self._apply_extensions()

        # Build metadata using streamlined approach
        self._metadata = MetaDataBuilder(
            self._schema,
            apply_schema_to_metadata=self.apply_schema_to_metadata,
            table_name_postfix=self.table_name_postfix,
        ).build()

        logger.debug("Loaded TAP_SCHEMA '%s' from YAML resource", self.schema_name)

    def _reflect_from_database(self, db_context: DatabaseContext) -> None:
        """Reflect TAP_SCHEMA tables from an existing database.

        Parameters
        ----------
        db_context
            The database context to use for reflection.
        """
        self._metadata = MetaData(schema=self.schema_name if self.apply_schema_to_metadata else None)
        try:
            self._metadata.reflect(bind=db_context.engine)
        except SQLAlchemyError as e:
            logger.error("Error reflecting TAP_SCHEMA database: %s", e)
            raise

    def _apply_extensions(self) -> None:
        """Apply extensions from a YAML file to the TAP_SCHEMA schema.

        This method loads extension column definitions from a YAML file and
        adds them to the appropriate TAP_SCHEMA tables.
        """
        if not self.extensions_path:
            return

        logger.info("Loading TAP_SCHEMA extensions from: %s", self.extensions_path)
        extensions_schema = Schema.from_uri(self.extensions_path, context={"id_generation": True})

        if not extensions_schema.tables:
            logger.warning("Extensions schema does not contain any tables, no extensions applied")
            return

        extension_count = 0
        extension_tables = {table.name: table.columns for table in extensions_schema.tables}

        for table in self.schema.tables:
            extension_columns = extension_tables.get(table.name)
            if extension_columns:
                table.columns = list(table.columns) + list(extension_columns)
                extension_count += len(extension_columns)
                logger.debug("Added %d extension columns to table '%s'", len(extension_columns), table.name)

        logger.info("Applied %d extension columns to TAP_SCHEMA", extension_count)

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
            raise KeyError(f"Table '{table_name}' not found in TAP_SCHEMA")
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
        return os.path.join(os.path.dirname(__file__), "config", "tap_schema", "tap_schema_std.yaml")

    @classmethod
    def get_tap_schema_std_resource(cls) -> ResourcePath:
        """Get the standard TAP_SCHEMA schema resource.

        Returns
        -------
        `~lsst.resources.ResourcePath`
            The standard TAP_SCHEMA schema resource.
        """
        return ResourcePath("resource://felis/config/tap_schema/tap_schema_std.yaml")

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

        Notes
        -----
        This is a private method that is called during initialization, allowing
        us to use table names like ``schemas11`` such as those used by the CADC
        TAP library instead of the standard table names. It also maps between
        the standard table names and those with the schema name prepended like
        SQLAlchemy uses. The mapping is stored in ``self._table_map``.
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

    def initialize_database(self, db_context: DatabaseContext) -> None:
        """Initialize a database with the TAP_SCHEMA tables.

        Parameters
        ----------
        db_context
            The database context to use to create the tables.
        """
        logger.info("Creating TAP_SCHEMA database '%s'", self.schema_name)
        db_context.initialize()
        db_context.create_all()

    def select(
        self,
        db_context: DatabaseContext,
        table_name: str,
        filter_condition: str = "",
    ) -> list[dict[str, Any]]:
        """Select all rows from a TAP_SCHEMA table with an optional filter
        condition.

        Parameters
        ----------
        db_context
            The database context to use to connect to the database.
        table_name
            The name of the table to select from.
        filter_condition
            The filter condition as a string. If empty, no filter will be
            applied.

        Returns
        -------
        list
            A list of dictionaries containing the rows from the table.
        """
        table = self[table_name]
        query = select(table)
        if filter_condition:
            query = query.where(text(filter_condition))
        with db_context.engine.connect() as connection:
            result = connection.execute(query)
            rows = [dict(row._mapping) for row in result]
        return rows


class DataLoader:
    """Load data into the TAP_SCHEMA tables.

    Parameters
    ----------
    schema
        The Felis ``Schema`` to load into the TAP_SCHEMA tables.
    mgr
        The table manager that contains the TAP_SCHEMA tables.
    db_context
        The database context to use to connect to the database.
    tap_schema_index
        The index of the schema in the TAP_SCHEMA database.
    output_file
        The file object to write the SQL statements to. If None, file output
        will be suppressed.
    print_sql
        If True, print the SQL statements that will be executed.
    dry_run
        If True, the data will not be loaded into the database.
    unique_keys
        If True, prepend the schema name to the key name to make it unique
        when loading data into the keys and key_columns tables.
    """

    def __init__(
        self,
        schema: Schema,
        mgr: TableManager,
        db_context: DatabaseContext,
        tap_schema_index: int = 0,
        output_file: IO[str] | None = None,
        print_sql: bool = False,
        dry_run: bool = False,
        unique_keys: bool = False,
    ):
        self.schema = schema
        self.mgr = mgr
        self._db_context = db_context
        self.tap_schema_index = tap_schema_index
        self.inserts: list[Insert] = []
        self.output_file = output_file
        self.print_sql = print_sql
        self.dry_run = dry_run
        self.unique_keys = unique_keys

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
        if self.output_file:
            # Print to an output file.
            self._write_sql_to_file()
        if not self.dry_run:
            # Execute the inserts if not in dry run mode.
            self._execute_inserts()
        else:
            logger.info("Dry run - skipped loading into database")

    def _insert_schemas(self) -> None:
        """Insert the schema data into the ``schemas`` table."""
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
        """Insert the table data into the ``tables`` table."""
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
        """Insert the column data into the ``columns`` table."""
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

    def _get_key(self, constraint: Constraint) -> str:
        """Get the key name for a constraint.

        Parameters
        ----------
        constraint
            The constraint to get the key name for.

        Returns
        -------
        str
            The key name for the constraint.

        Notes
        -----
        This will prepend the name of the schema to the key name if the
        `unique_keys` attribute is set to True. Otherwise, it will just return
        the name of the constraint.
        """
        if self.unique_keys:
            key_id = f"{self.schema.name}_{constraint.name}"
            logger.debug("Generated unique key_id: %s -> %s", constraint.name, key_id)
        else:
            key_id = constraint.name
        return key_id

    def _insert_keys(self) -> None:
        """Insert the foreign keys into the ``keys`` and ``key_columns``
        tables.
        """
        for table in self.schema.tables:
            for constraint in table.constraints:
                if isinstance(constraint, datamodel.ForeignKeyConstraint):
                    ###########################################################
                    # Handle keys table
                    ###########################################################
                    referenced_column = self.schema.find_object_by_id(
                        constraint.referenced_columns[0], datamodel.Column
                    )
                    referenced_table = self.schema.get_table_by_column(referenced_column)
                    key_id = self._get_key(constraint)
                    key_record = {
                        "key_id": key_id,
                        "from_table": self._get_table_name(table),
                        "target_table": self._get_table_name(referenced_table),
                        "description": constraint.description,
                        "utype": constraint.votable_utype,
                    }
                    self._insert("keys", key_record)

                    ###########################################################
                    # Handle key_columns table
                    ###########################################################
                    # Loop over the corresponding columns and referenced
                    # columns and insert a record for each pair. This is
                    # necessary for proper handling of composite keys.
                    for from_column_id, target_column_id in zip(
                        constraint.columns, constraint.referenced_columns
                    ):
                        from_column = self.schema.find_object_by_id(from_column_id, datamodel.Column)
                        target_column = self.schema.find_object_by_id(target_column_id, datamodel.Column)
                        key_columns_record = {
                            "key_id": key_id,
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
        try:
            with self._db_context.engine.begin() as connection:
                for insert in self.inserts:
                    connection.execute(insert)
        except Exception as e:
            logger.error("Error loading data into database: %s", e)
            raise

    def _compiled_inserts(self) -> list[str]:
        """Compile the inserts to SQL.

        Returns
        -------
        list
            A list of the compiled insert statements.
        """
        return [
            str(
                insert.compile(
                    dialect=self._db_context.dialect,
                    compile_kwargs={"literal_binds": True},
                ),
            )
            for insert in self.inserts
        ]

    def _print_sql(self) -> None:
        """Print the generated inserts to stdout."""
        for insert_str in self._compiled_inserts():
            print(insert_str + ";")

    def _write_sql_to_file(self) -> None:
        """Write the generated insert statements to a file."""
        if not self.output_file:
            raise ValueError("No output file specified")
        for insert_str in self._compiled_inserts():
            self.output_file.write(insert_str + ";" + "\n")

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


class MetadataInserter:
    """Insert TAP_SCHEMA self-description rows into the database.

    Parameters
    ----------
    mgr
        The table manager that contains the TAP_SCHEMA tables.
    db_context
        The database context for connecting to the TAP_SCHEMA database.
    """

    def __init__(self, mgr: TableManager, db_context: DatabaseContext):
        """Initialize the metadata inserter.

        Parameters
        ----------
        mgr
            The table manager representing the TAP_SCHEMA tables.
        db_context
            The database context for connecting to the database.
        """
        self._mgr = mgr
        self._db_context = db_context

    def insert_metadata(self) -> None:
        """Insert the TAP_SCHEMA metadata into the database."""
        with self._db_context.engine.begin() as conn:
            for table_name in self._mgr.get_table_names_std():
                table = self._mgr[table_name]
                csv_bytes = ResourcePath(f"resource://felis/config/tap_schema/{table_name}.csv").read()
                text_stream = io.TextIOWrapper(io.BytesIO(csv_bytes), encoding="utf-8")
                reader = csv.reader(text_stream)
                headers = next(reader)
                rows = [
                    {key: None if value == "\\N" else value for key, value in zip(headers, row)}
                    for row in reader
                ]
                logger.debug(
                    "Inserting %d rows into table '%s' with headers: %s",
                    len(rows),
                    table_name,
                    headers,
                )
                conn.execute(table.insert(), rows)
