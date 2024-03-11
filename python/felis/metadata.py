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
from typing import Any, Literal, TextIO

import sqlalchemy.schema as sqa_schema
from sqlalchemy import (
    CheckConstraint,
    Column,
    Constraint,
    Engine,
    ForeignKeyConstraint,
    Index,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
    create_mock_engine,
    make_url,
    text,
)
from sqlalchemy.types import TypeEngine

from . import datamodel as dm
from .db import sqltypes
from .sql import COLUMN_VARIANT_OVERRIDE, _process_variant_override
from .types import FelisType

logger = logging.getLogger(__name__)


class InsertDump:
    """An Insert Dumper for SQL statements which supports writing messages
    to stdout or a file.

    Copied and modified slightly from `cli.py` in Felis. That module may be
    removed soon.
    """

    dialect: Any = None

    file: TextIO | None = None

    def dump(self, sql: Any, *multiparams: Any, **params: Any) -> None:
        """Dump the SQL statement to a file or stdout."""
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


class SchemaMetaData(MetaData):
    """A sqlalchemy (SQA) Metadata object derived from the information in a
    single `felis.datamodel.Schema` object.

    Notes
    -----
    The `SchemaMetaData` is built from a schema object which has already been
    validated. This class is intended as a drop-in replacement when a
    sqlalchemy `MetaData` object is needed, and it replaces the visitor
    pattern used previously for building this type of object.

    Users should not call any of the build methods themselves, and doing so
    will result in errors. The object is built automatically from the provided
    `Schema` when the class is instantiated.
    """

    def __init__(self, schema_obj: dm.Schema, schema_name: str | None) -> None:
        """Initialize SQA `MetaData` from a `felis.datamodel.Schema` object.

        Parameters
        ----------
        schema_obj : `felis.datamodel.Schema`
            The schema object to build the metadata from.
        schema_name : `str`
            Alternate schema name to override the Felis file.
        """
        MetaData.__init__(self, schema=schema_name if schema_name else schema_obj.name)
        self._schema_obj = schema_obj
        self._object_index: dict[str, Any] = {}
        self._build_all()

    def _build_all(self) -> None:
        """Build metadata from the registered schema into this object."""
        self._build_tables()
        self._build_constraints()

    def _build_tables(self) -> None:
        """Build the SQA tables from the schema.

        Notes
        -----
        This is the main function for building the SQA tables from the schema
        including objects within tables such as constraints, primary keys,
        and indices, which have their own dedicated sub-functions.
        """
        for table in self._schema_obj.tables:
            self._build_table(table)
            if table.primaryKey:
                primary_key = self._build_primary_key(table.primaryKey)
                self[table.id].append_constraint(primary_key)

    def _build_primary_key(self, primary_key_columns: str | list[str]) -> PrimaryKeyConstraint:
        """Build a SQA `PrimaryKeyConstraint` from a single column ID or a list
        or them.

        Parameters
        ----------
        primary_key_columns : `str` or `list` of `str`
            The column ID or list of column IDs from which to build the primary
            key.
        """
        columns: list[Column] = []
        if isinstance(primary_key_columns, str):
            columns.append(self[primary_key_columns])
        else:
            columns.extend([self[column_id] for column_id in primary_key_columns])
        return PrimaryKeyConstraint(*columns)

    def _build_table(self, table_obj: dm.Table) -> None:
        """Build a SQA table from a `datamodel.Table` object and it to this
        `MetaData` object.

        Parameters
        ----------
        table_obj : `felis.datamodel.Table`
            The table object to build the SQA table from.
        """
        # Process mysql table options.
        optargs = {}
        if table_obj.mysql_engine:
            optargs["mysql_engine"] = table_obj.mysql_engine
        if table_obj.mysql_charset:
            optargs["mysql_charset"] = table_obj.mysql_charset

        # Create the SQA table object and its columns.
        name = table_obj.name
        id = table_obj.id
        description = table_obj.description
        columns = [self._build_column(column) for column in table_obj.columns]
        table = Table(name, self, *columns, comment=description, **optargs)

        # Create the indexes and add them to the table.
        indexes = [self._build_index(index) for index in table_obj.indexes]
        for index in indexes:
            index._set_parent(table)
            table.indexes.add(index)

        self[id] = table

    def _build_column(self, column_obj: dm.Column) -> Column:
        """Build a SQA column from a `felis.datamodel.Column` object.

        Parameters
        ----------
        column_obj : `felis.datamodel.Column`
            The column object from which to build the SQA column.

        Returns
        -------
        column: `Column`
            The SQA column object.
        """
        # Get basic column attributes.
        name = column_obj.name
        id = column_obj.id
        datatype_name: str = column_obj.datatype  # type: ignore[assignment]
        description = column_obj.description
        default = column_obj.value
        length = column_obj.length

        # Handle variant overrides based on code from Felis `sql` module.
        variant_dict = SchemaMetaData._make_variant_dict(column_obj)
        felis_type = FelisType.felis_type(datatype_name)
        datatype_fun = getattr(sqltypes, datatype_name)
        if felis_type.is_sized:
            datatype = datatype_fun(length, **variant_dict)
        else:
            datatype = datatype_fun(**variant_dict)

        # Set default value of nullable based on column type and then whether
        # it was explicitly provided in the schema data.
        nullable_default = False if isinstance(datatype, Numeric) else True
        nullable = column_obj.nullable if column_obj.nullable is not None else nullable_default

        # Set autoincrement depending on if it was provided explicitly.
        autoincrement: Literal["auto", "ignore_fk"] | bool = (
            column_obj.autoincrement if column_obj.autoincrement else "auto"
        )

        column: Column = Column(
            name,
            datatype,
            comment=description,
            autoincrement=autoincrement,
            nullable=nullable,
            server_default=default,
        )

        self[id] = column

        return column

    def _build_constraints(self) -> None:
        """Build the SQA constraints in the Felis schema and append them to the
        associated `Table`.
        """
        for table_obj in self._schema_obj.tables:
            table = self[table_obj.id]
            for constraint_obj in table_obj.constraints:
                constraint = self._build_constraint(constraint_obj)
                table.append_constraint(constraint)

    def _build_constraint(self, constraint_obj: dm.Constraint) -> Constraint:
        """Build a SQA `Constraint` from a `felis.datamodel.Constraint` object.

        Parameters
        ----------
        constraint_obj : `felis.datamodel.Constraint`
            The constraint object from which to build the SQA constraint.

        Returns
        -------
        constraint: `Constraint`
            The SQA constraint object.
        """
        args: dict[str, Any] = {}
        args["name"] = constraint_obj.name if constraint_obj.name else None
        args["info"] = constraint_obj.description if constraint_obj.description else None
        args["deferrable"] = constraint_obj.deferrable if constraint_obj.deferrable else None
        args["initially"] = constraint_obj.initially if constraint_obj.initially else None

        constraint: Constraint
        constraint_type = constraint_obj.type

        if constraint_type == "ForeignKey":
            if isinstance(constraint_obj, dm.ForeignKeyConstraint):
                fk_obj: dm.ForeignKeyConstraint = constraint_obj
                refcolumns = [self[column_id] for column_id in fk_obj.referenced_columns]
                constraint = ForeignKeyConstraint(refcolumns, refcolumns, **args)
            else:
                raise TypeError("Unexpected constraint type for ForeignKey: ", type(constraint_obj))
        elif constraint_type == "Check":
            if isinstance(constraint_obj, dm.CheckConstraint):
                check_obj: dm.CheckConstraint = constraint_obj
                expression = check_obj.expression
                constraint = CheckConstraint(expression, **args)
            else:
                raise TypeError("Unexpected constraint type for CheckConstraint: ", type(constraint_obj))
        elif constraint_type == "Unique":
            if isinstance(constraint_obj, dm.UniqueConstraint):
                unique_obj: dm.UniqueConstraint = constraint_obj
                columns = [self[column_id] for column_id in unique_obj.columns]
                constraint = UniqueConstraint(*columns, **args)
            else:
                raise TypeError("Unexpected constraint type for UniqueConstraint: ", type(constraint_obj))
        else:
            raise ValueError(f"Unexpected constraint type: {constraint_type}")

        self[constraint_obj.id] = constraint

        return constraint

    def _build_index(self, index_obj: dm.Index) -> Index:
        """Build a SQA `Index` from a `felis.datamodel.Index` object.

        Parameters
        ----------
        index_obj : `felis.datamodel.Index`
            The index object from which to build the SQA index.

        Returns
        -------
        index: `Index`
            The SQA index object.
        """
        columns = [self[c_id] for c_id in (index_obj.columns if index_obj.columns else [])]
        expressions = index_obj.expressions if index_obj.expressions else []
        index = Index(index_obj.name, *columns, *expressions)
        self[index_obj.id] = index
        return index

    @classmethod
    def _make_variant_dict(cls, column_obj: dm.Column) -> dict[str, TypeEngine[Any]]:
        """Handle variant overrides, based on logic from `sql.py`.

        Parameters
        ----------
        column_obj : felis.datamodel.Column
            The column object from which to build the variant dictionary.

        Returns
        -------
        variant_dict : `dict`
            The dictionary of `str` to `TypeEngine` containing variant datatype
            information (e.g., for mysql, postgresql, etc).
        """
        variant_dict = {}
        for field_name, value in iter(column_obj):
            if field_name in COLUMN_VARIANT_OVERRIDE:
                dialect = COLUMN_VARIANT_OVERRIDE[field_name]
                variant: TypeEngine = _process_variant_override(dialect, value)
                variant_dict[dialect] = variant
        return variant_dict

    def __getitem__(self, id: str) -> Any:
        """Get a SQA object by its ID field.

        Parameters
        ----------
        id : str
            The ID of the object to retrieve.

        Returns
        -------
        obj : Any
            The object with the given ID.

        Raises
        ------
        KeyError
            If the object is not found in the `MetaData` object.

        Notes
        -----
        This function should not interfere with the behavior of the base
        `MetaData` class, which does not implment `__getitem__`.
        """
        if id not in self._object_index:
            raise KeyError(f"Object not found in MetaData with id: {id}")
        return self._object_index[id]

    def __setitem__(self, id: str, obj: Any) -> None:
        """Register a sqlalchemy object by its ID field.

        Parameters
        ----------
        id : str
            The ID of the object to register.
        obj : Any
            The object to register.

        Raises
        ------
        KeyError
            If the object is already found in the `MetaData` object.

        Notes
        -----
        This function should not interfere with the behavior of the base
        `MetaData` class, which does not implment `__setitem__`.
        """
        if id in self._object_index:
            raise KeyError(f"Object already exists in MetaData with id: {id}")
        self._object_index[id] = obj

    def dump(self, connection_string: str = "sqlite://", file: TextIO | None = None) -> None:
        """Dump the metadata to a file or stdout.

        Parameters
        ----------
        connection_string : str
            The connection string to use for dumping the metadata.

        file : TextIO | None
            The file to write the dump to. If `None`, the dump will be written to
            stdout.
        """
        dumper = InsertDump()
        engine = create_mock_engine(make_url(connection_string), executor=dumper.dump)
        dumper.dialect = engine.dialect
        dumper.file = file
        self.create_all(engine)

    def create_if_not_exists(self, engine: Engine) -> None:
        """Create the schema in the database if it does not exist.

        In MySQL, this will create a new database. In PostgreSQL, it will create a
        new schema. For other variants, this is unsupported for now.

        Parameters
        ----------
        engine: `Engine`
            The SQLAlchemy engine object.
        schema_name: `str`
            The name of the schema (or database) to create.
        """
        db_type = engine.dialect.name
        schema_name = self.schema
        if db_type == "mysql":
            with engine.connect() as connection:
                logger.info(f"Creating MySQL database: {schema_name}")
                connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {schema_name}"))
        elif db_type == "postgresql":
            logger.info(f"Creating PostgreSQL schema: {schema_name}")
            with engine.connect() as connection:
                if not engine.dialect.has_schema(engine, schema_name):
                    connection.execute(sqa_schema.CreateSchema(schema_name))
                else:
                    logger.info("Schema already exists: {schema_name}")
        else:
            raise ValueError("Unsupported database type:" + db_type)
