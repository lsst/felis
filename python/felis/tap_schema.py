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
import re
import sys
from typing import IO, Any

from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.dml import Insert

from felis import datamodel
from felis.datamodel import Schema

from .types import FelisType

logger = logging.getLogger(__name__)

_IDENTIFIER_LENGTH = 128
_SMALL_FIELD_LENGTH = 32
_SIMPLE_FIELD_LENGTH = 128
_TEXT_FIELD_LENGTH = 2048
_QUALIFIED_TABLE_LENGTH = 3 * _IDENTIFIER_LENGTH + 2

_COLUMNS: dict[str, dict[str, Column[Any]]] = {
    "schemas": {
        "schema_name": Column(String(_IDENTIFIER_LENGTH), primary_key=True, nullable=False),
        "utype": Column(String(_SIMPLE_FIELD_LENGTH)),
        "description": Column(String(_TEXT_FIELD_LENGTH)),
        "schema_index": Column(Integer),
    },
    "tables": {
        "schema_name": Column(String(_IDENTIFIER_LENGTH), nullable=False),
        "table_name": Column(String(_QUALIFIED_TABLE_LENGTH), nullable=False, primary_key=True),
        "table_type": Column(String(_SMALL_FIELD_LENGTH), nullable=False),
        "utype": Column(String(_SIMPLE_FIELD_LENGTH)),
        "description": Column(String(_TEXT_FIELD_LENGTH)),
        "table_index": Column(Integer),
    },
    "columns": {
        "table_name": Column(String(_QUALIFIED_TABLE_LENGTH), nullable=False, primary_key=True),
        "column_name": Column(String(_IDENTIFIER_LENGTH), nullable=False, primary_key=True),
        "datatype": Column(String(_SIMPLE_FIELD_LENGTH), nullable=False),
        "arraysize": Column(String(10)),
        "xtype": Column(String(_SIMPLE_FIELD_LENGTH)),
        "size": Column(Integer, name="size", quote=True),
        "description": Column(String(_TEXT_FIELD_LENGTH)),
        "utype": Column(String(_SIMPLE_FIELD_LENGTH)),
        "unit": Column(String(_SIMPLE_FIELD_LENGTH)),
        "ucd": Column(String(_SIMPLE_FIELD_LENGTH)),
        "indexed": Column(Integer, nullable=False),
        "principal": Column(Integer, nullable=False),
        "std": Column(Integer, nullable=False),
        "column_index": Column(Integer),
    },
    "keys": {
        "key_id": Column(String(_IDENTIFIER_LENGTH), nullable=False, primary_key=True),
        "from_table": Column(String(_QUALIFIED_TABLE_LENGTH), nullable=False),
        "target_table": Column(String(_QUALIFIED_TABLE_LENGTH), nullable=False),
        "description": Column(String(_TEXT_FIELD_LENGTH)),
        "utype": Column(String(_SIMPLE_FIELD_LENGTH)),
    },
    "key_columns": {
        "key_id": Column(String(_IDENTIFIER_LENGTH), nullable=False, primary_key=True),
        "from_column": Column(String(_IDENTIFIER_LENGTH), nullable=False, primary_key=True),
        "target_column": Column(String(_IDENTIFIER_LENGTH), nullable=False, primary_key=True),
    },
}
"""Dictionary of table names to column definitions for standard TAP_SCHEMA
tables.

This is intended for internal use only. The ``TableManager`` should be used
to create and manage the tables.
"""

_TAP_SCHEMA_NAME = "TAP_SCHEMA"
"""The default name of the TAP_SCHEMA schema."""


class TableManager:
    """Manage creation of TAP_SCHEMA tables.

    The user does not need to create the tables manually. The tables are
    created automatically when the class is instantiated.

    Parameters
    ----------
    tap_schema_name
        The name of the TAP_SCHEMA schema. By default it is "TAP_SCHEMA". This
        can be set to None if the tables should not be created in a schema.
    table_name_postfix
        A string to append to all the standard table names.

    Notes
    -----
    This class should not be shared between threads because it modifies the
    shared base metadata that SQLAlchemy uses to globally manage ORMs. Only a
    single instance should be used within a process or there could be problems
    with concurrent modifcation or deletion of this shared state.
    """

    _base = declarative_base()

    _columns = _COLUMNS

    def __init__(self, tap_schema_name: str | None = _TAP_SCHEMA_NAME, table_name_postfix: str = ""):
        """Initialize the table manager."""
        self.tap_schema_name = tap_schema_name
        self.table_name_postfix = table_name_postfix
        self.table_map = self._create_table_map()
        self._create_metadata()

    @property
    def standard_table_names(self) -> list[str]:
        """Get a list of the canonical TAP_SCHEMA table names.

        Returns
        -------
        list
            A list of the standard table names.
        """
        return list(_COLUMNS.keys())

    def _create_table_map(self) -> dict[str, str]:
        """Create a mapping of standard table names to the table names modified
        with a postfix or other changes, as well as the schema name if it is
        set.

        Returns
        -------
        dict
            A dictionary mapping the standard table names to the modified
            table names.
        """
        return {
            table_name: (
                f"{self.tap_schema_name + '.' if self.tap_schema_name else ''}"
                f"{table_name}{self.table_name_postfix}"
            )
            for table_name in _COLUMNS
        }

    def __getitem__(self, table_name: str) -> Table:
        """Get a table by its standard TAP_SCHEMA name.

        Parameters
        ----------
        table_name
            The name of the table to get.
        """
        if table_name not in self.table_map:
            raise KeyError(f"Table {table_name} not found in table map")
        return self.tables[self.table_map[table_name]]

    @property
    def tables(self) -> dict[str, Any]:
        """Get a dictionary of the SQLAlchemy tables from the metadata.

        Returns
        -------
        dict
            A dictionary of table names to SQLAlchemy tables.
        """
        return self._base.metadata.tables

    @property
    def metadata(self) -> MetaData:
        """Get the metadata for the tables.

        Returns
        -------
        MetaData
            The metadata for the tables.
        """
        return TableManager._base.metadata

    def _create_table(self, table_name: str, columns: dict[str, Column]) -> None:
        """Create a table with the given name and columns in the metadata.

        Parameters
        ----------
        table_name
            The name of the table to create.
        columns
            The columns to add to the table.
        """
        attributes = {"__tablename__": table_name, **columns}
        type(table_name, (TableManager._base,), attributes)

    def _create_metadata(self) -> None:
        """Create the TAP_SCHEMA tables within the SQLAlchemy declarative
        base metadata.

        Notes
        -----
        If there are already tables in the base metadata, they will be cleared
        automatically before creating the new ones.
        """
        logger.info("Creating TAP_SCHEMA tables")
        if len(self.tables) > 0:
            logger.info("TAP_SCHEMA tables already exist, clearing first")
            self._clear()
        TableManager._base.metadata.schema = self.tap_schema_name
        for table_name, columns in _COLUMNS.items():
            table_name = table_name + self.table_name_postfix
            logger.info("Creating table %s", table_name + self.table_name_postfix)
            self._create_table(table_name, columns)

    def _create_schema(self, engine: Engine) -> None:
        """Create the schema for the TAP_SCHEMA tables if it does not already
        exist.

        Parameters
        ----------
        engine
            The SQLAlchemy engine to use to create the schema.

        Notes
        -----
        This will only work for a PostgreSQL connection.
        """
        if engine.dialect.name == "postgresql":
            with engine.connect() as conn:
                trans = conn.begin()  # Start a transaction
                try:
                    if conn.dialect.name == "postgresql":
                        conn.execute(CreateSchema(self.tap_schema_name, if_not_exists=True))
                    trans.commit()
                except Exception:
                    trans.rollback()
                    raise
        else:
            logger.warning("Database engine %s does not support CREATE SCHEMA", engine.dialect.name)

    def initialize_database(self, engine: Engine) -> None:
        """Initialize a database with the TAP_SCHEMA tables from the
        metadata.

        Parameters
        ----------
        engine
            The SQLAlchemy engine to use to create the tables.
        """
        self._create_schema(engine)
        self.metadata.create_all(engine)

    def _clear_columns(self) -> None:
        """Clear the table references from the columns."""
        for columns in TableManager._columns.values():
            for column in columns.values():
                column.table = None  # type: ignore

    def _clear(self) -> None:
        """Clear the state of the table manager."""
        TableManager._base.metadata.clear()
        TableManager._base.metadata.schema = None
        TableManager._base.registry.dispose(cascade=True)
        self._clear_columns()


class DataLoader:
    """Load data into the TAP_SCHEMA tables.

    Parameters
    ----------
    schema
        The schema to load into the TAP_SCHEMA tables.
    mgr
        The table manager that contains the TAP_SCHEMA tables.
    engine
        The SQLAlchemy engine to use to connect to the database.
    tap_schema_index
        The index of the schema in the TAP_SCHEMA database.
    outfile
        The file to write the SQL statements to. If None, printing will be
        suppressed.
    dry_run
        If True, the data will not be loaded into the database.
    """

    def __init__(
        self,
        schema: Schema,
        mgr: TableManager,
        engine: Engine,
        tap_schema_index: int = 0,
        outfile: IO[str] = sys.stdout,
        dry_run: bool = False,
    ):
        self.schema = schema
        self.mgr = mgr
        self.engine = engine
        self.tap_schema_index = tap_schema_index
        self.inserts: list[Insert] = []
        self.outfile = outfile
        self.dry_run = dry_run

    def load(self) -> None:
        """Load the schema data into the TAP_SCHEMA tables. This will first
        generate inserts for the data and then load the data into the database.
        If enabled it will also print the SQL statements that will be executed.
        This is the main entry point for the class.
        """
        self._insert_all()
        self._load_data()
        self._print_sql()

    def _insert_schema(self) -> None:
        """Insert the schema data into the schemas table."""
        schema_record = self._new_record(
            self.schema.name, self.schema.description, self.schema.votable_utype, self.tap_schema_index
        )
        self._insert("schemas", schema_record)

    def _insert_tables(self) -> None:
        """Insert the table data into the tables table."""
        for table in self.schema.tables:
            table_record = DataLoader._new_record(
                self.schema.name,
                table.name,
                "table",
                table.votable_utype,
                table.description,
                0 if table.tap_table_index is None else table.tap_table_index,
            )
            self._insert("tables", table_record)

    def _insert_columns(self) -> None:
        """Insert the column data into the columns table."""
        for table in self.schema.tables:
            for column in table.columns:
                felis_type = FelisType.felis_type(column.datatype.value)
                arraysize = DataLoader._get_arraysize(column, felis_type)
                size = DataLoader._get_size(column)
                indexed = DataLoader._is_indexed(column, table)
                tap_column_index = 0 if column.tap_column_index is None else column.tap_column_index
                unit = column.ivoa_unit or column.fits_tunit
                column_record = self._new_record(
                    table.name,
                    column.name,
                    column.datatype,
                    arraysize,
                    column.votable_xtype,
                    size,
                    column.description,
                    column.votable_utype,
                    unit,
                    column.ivoa_ucd,
                    indexed,
                    column.tap_principal,
                    column.tap_std,
                    tap_column_index,
                )
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
                    key_record = self._new_record(
                        constraint.name,
                        table.name,
                        referenced_table.name,
                        constraint.description,
                        constraint.votable_utype,
                    )
                    self._insert("keys", key_record)

                    # Handle key_columns table
                    from_column = self.schema.find_object_by_id(constraint.columns[0], datamodel.Column)
                    target_column = self.schema.find_object_by_id(
                        constraint.referenced_columns[0], datamodel.Column
                    )
                    key_columns_record = self._new_record(
                        constraint.name,
                        from_column.name,
                        target_column.name,
                    )
                    self._insert("key_columns", key_columns_record)

    def _insert_all(self) -> None:
        """Generate the inserts for all the data."""
        self.inserts.clear()
        self._insert_schema()
        self._insert_tables()
        self._insert_columns()
        self._insert_keys()

    def _load_data(self) -> None:
        """Load the `~felis.datamodel.Schema` data into the TAP_SCHEMA
        tables.
        """
        if not self.dry_run:
            with self.engine.connect() as connection:
                for insert in self.inserts:
                    connection.execute(insert)
        else:
            logger.info("Dry run: not loading data into database")

    def _print_sql(self) -> None:
        """Print the SQL statements that will be executed to load the data."""
        if self.outfile is not None:
            for insert in self.inserts:
                insert_str = str(insert.compile(self.engine, compile_kwargs={"literal_binds": True}))
                print(insert_str, file=self.outfile)

    def _insert(self, table_name: str, record: list[Any]) -> None:
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
    def _new_record(*args: Any) -> list[Any]:
        """Create a new record for insertion into a table.

        Parameters
        ----------
        args
            The values to insert into the record.

        Returns
        -------
        list
            A list of values to insert into the table.
        """
        return [value for value in args]

    @staticmethod
    def _get_arraysize(column: datamodel.Column, felis_type: type[FelisType]) -> str | None:
        """Get the VOTable ``arraysize`` for the column.

        Parameters
        ----------
        column
            The column to get the arraysize for.
        felis_type
            The FelisType for the column.

        Returns
        -------
        str or int or None
            The VOTable arraysize for the column.
        """
        arraysize = column.votable_arraysize or column.length
        if (felis_type.is_timestamp or column.datatype == "text") and arraysize is None:
            arraysize = "*"
        return str(arraysize) if arraysize else None

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

        def _is_int(s: str) -> bool:
            try:
                int(s)
                return True
            except ValueError:
                return False

        size = None
        arraysize = column.votable_arraysize
        if arraysize is not None and str(arraysize) != "":
            if isinstance(arraysize, int):
                size = arraysize
            elif _is_int(arraysize):
                size = int(arraysize)
            elif bool(re.match(r"^[0-9]+\*$", arraysize)):
                size = int(arraysize.replace("*", ""))
        return size

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
