"""Build SQLAlchemy metadata from a Felis schema."""

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
from typing import Any, Literal

from lsst.utils.iteration import ensure_iterable
from sqlalchemy import (
    CheckConstraint,
    Column,
    Constraint,
    ForeignKeyConstraint,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    TextClause,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.types import TypeEngine

from felis.datamodel import Schema
from felis.db.variants import make_variant_dict

from . import datamodel
from .db import sqltypes
from .types import FelisType

__all__ = ("MetaDataBuilder", "get_datatype_with_variants")

logger = logging.getLogger(__name__)


def _handle_timestamp_column(column_obj: datamodel.Column, variant_dict: dict[str, TypeEngine[Any]]) -> None:
    """Handle columns with the timestamp datatype.

    Parameters
    ----------
    column_obj
        The column object representing the timestamp.
    variant_dict
        The dictionary of variant overrides for the datatype.

    Notes
    -----
    This function updates the variant dictionary with the appropriate
    timestamp type for the column object but only if the precision is set.
    Otherwise, the default timestamp objects defined in the Felis type system
    will be used instead.
    """
    if column_obj.precision is not None:
        args: Any = [False, column_obj.precision]  # Turn off timezone.
        variant_dict.update({"postgresql": postgresql.TIMESTAMP(*args), "mysql": mysql.DATETIME(*args)})


def get_datatype_with_variants(column_obj: datamodel.Column) -> TypeEngine:
    """Use the Felis type system to get a SQLAlchemy datatype with variant
    overrides from the information in a Felis column object.

    Parameters
    ----------
    column_obj
        The column object from which to get the datatype.

    Returns
    -------
    `~sqlalchemy.types.TypeEngine`
        The SQLAlchemy datatype object.

    Raises
    ------
    ValueError
        Raised if the column has a sized type but no length or if the datatype
        is invalid.
    """
    variant_dict = make_variant_dict(column_obj)
    felis_type = FelisType.felis_type(column_obj.datatype.value)
    datatype_fun = getattr(sqltypes, column_obj.datatype.value, None)
    if datatype_fun is None:
        raise ValueError(f"Unknown datatype: {column_obj.datatype.value}")
    args = []
    if felis_type.is_sized:
        # Add length argument for size types.
        if not column_obj.length:
            raise ValueError(f"Column {column_obj.name} has sized type '{column_obj.datatype}' but no length")
        args = [column_obj.length]
    if felis_type.is_timestamp:
        _handle_timestamp_column(column_obj, variant_dict)
    return datatype_fun(*args, **variant_dict)


_VALID_SERVER_DEFAULTS = ("CURRENT_TIMESTAMP", "NOW()", "LOCALTIMESTAMP", "NULL")


class MetaDataBuilder:
    """Build a SQLAlchemy metadata object from a Felis schema.

    Parameters
    ----------
    schema
        The schema object from which to build the SQLAlchemy metadata.
    apply_schema_to_metadata
        Whether to apply the schema name to the metadata object.
    apply_schema_to_tables
        Whether to apply the schema name to the tables.
    ignore_constraints
        Whether to ignore constraints when building the metadata.
    """

    def __init__(
        self,
        schema: Schema,
        apply_schema_to_metadata: bool = True,
        apply_schema_to_tables: bool = True,
        ignore_constraints: bool = False,
    ) -> None:
        """Initialize the metadata builder."""
        self.schema = schema
        if not apply_schema_to_metadata:
            logger.debug("Schema name will not be applied to metadata")
        if not apply_schema_to_tables:
            logger.debug("Schema name will not be applied to tables")
        self.metadata = MetaData(schema=schema.name if apply_schema_to_metadata else None)
        self._objects: dict[str, Any] = {}
        self.apply_schema_to_tables = apply_schema_to_tables
        self.ignore_constraints = ignore_constraints

    def build(self) -> MetaData:
        """Build the SQLAlchemy tables and constraints from the schema.

        Notes
        -----
        This first builds the tables and then makes a second pass to build the
        constraints. This is necessary because the constraints may reference
        objects that are not yet created when the tables are built.

        Returns
        -------
        `~sqlalchemy.sql.schema.MetaData`
            The SQLAlchemy metadata object.
        """
        self.build_tables()
        if not self.ignore_constraints:
            self.build_constraints()
        else:
            logger.warning("Ignoring constraints")
        return self.metadata

    def build_tables(self) -> None:
        """Build the SQLAlchemy tables from the schema."""
        for table in self.schema.tables:
            self.build_table(table)
            if table.primary_key:
                primary_key = self.build_primary_key(table.primary_key)
                self._objects[table.id].append_constraint(primary_key)

    def build_primary_key(self, primary_key_columns: str | list[str]) -> PrimaryKeyConstraint:
        """Build a SQAlchemy ``PrimaryKeyConstraint`` from a single column ID
        or a list of them.

        Parameters
        ----------
        primary_key_columns
            The column ID or list of column IDs from which to build the primary
            key.

        Returns
        -------
        `~sqlalchemy.sql.schema.PrimaryKeyConstraint`
            The SQLAlchemy primary key constraint object.

        Notes
        -----
        The ``primary_key_columns`` is a string or a list of strings
        representing IDs which will be used to find the columnn objects in the
        builder's internal ID map.
        """
        return PrimaryKeyConstraint(
            *[self._objects[column_id] for column_id in ensure_iterable(primary_key_columns)]
        )

    def build_table(self, table_obj: datamodel.Table) -> None:
        """Build a SQLAlchemy ``Table`` from a Felis table and add it to the
        metadata.

        Parameters
        ----------
        table_obj
            The Felis table object from which to build the SQLAlchemy table.

        Notes
        -----
        Several MySQL table options, including the engine and charset, are
        handled by adding annotations to the table. This is not needed for
        Postgres, as Felis does not support any table options for this dialect.
        """
        # Process mysql table options.
        optargs = {}
        if table_obj.mysql_engine:
            optargs["mysql_engine"] = table_obj.mysql_engine
        if table_obj.mysql_charset:
            optargs["mysql_charset"] = table_obj.mysql_charset

        # Create the SQLAlchemy table object and its columns.
        name = table_obj.name
        id = table_obj.id
        description = table_obj.description
        columns = [self.build_column(column) for column in table_obj.columns]
        table = Table(
            name,
            self.metadata,
            *columns,
            comment=description,
            schema=self.schema.name if self.apply_schema_to_tables else None,
            **optargs,  # type: ignore[arg-type]
        )

        # Create the indexes and add them to the table.
        indexes = [self.build_index(index) for index in table_obj.indexes]
        for index in indexes:
            index._set_parent(table)
            table.indexes.add(index)

        self._objects[id] = table

    def build_column(self, column_obj: datamodel.Column) -> Column:
        """Build a SQLAlchemy ``Column`` from a Felis column object.

        Parameters
        ----------
        column_obj
            The column object from which to build the SQLAlchemy column.

        Returns
        -------
        `~sqlalchemy.sql.schema.Column`
            The SQLAlchemy column object.
        """
        # Get basic column attributes.
        name = column_obj.name
        id = column_obj.id
        description = column_obj.description
        value = column_obj.value
        nullable = column_obj.nullable

        # Get datatype, handling variant overrides such as "mysql:datatype".
        datatype = get_datatype_with_variants(column_obj)

        # Set autoincrement, depending on if it was provided explicitly.
        autoincrement: Literal["auto"] | bool = (
            column_obj.autoincrement if column_obj.autoincrement is not None else "auto"
        )

        server_default: str | TextClause | None = None
        if value is not None:
            server_default = str(value)
            if server_default in _VALID_SERVER_DEFAULTS or not isinstance(value, str):
                # If the server default is a valid keyword or not a string,
                # use it as is.
                server_default = text(server_default)

        if server_default is not None:
            logger.debug(f"Column '{id}' has default value: {server_default}")

        column: Column = Column(
            name,
            datatype,
            comment=description,
            autoincrement=autoincrement,
            nullable=nullable,
            server_default=server_default,
        )

        self._objects[id] = column

        return column

    def build_constraints(self) -> None:
        """Build the SQLAlchemy constraints from the Felis schema and append
        them to the associated table in the metadata.

        Notes
        -----
        This is performed as a separate step after building the tables so that
        all the referenced objects in the constraints will be present and can
        be looked up by their ID.
        """
        for table_obj in self.schema.tables:
            table = self._objects[table_obj.id]
            for constraint_obj in table_obj.constraints:
                constraint = self.build_constraint(constraint_obj)
                table.append_constraint(constraint)

    def build_constraint(self, constraint_obj: datamodel.Constraint) -> Constraint:
        """Build a SQLAlchemy ``Constraint`` from a  Felis constraint.

        Parameters
        ----------
        constraint_obj
            The Felis object from which to build the constraint.

        Returns
        -------
        `~sqlalchemy.sql.schema.Constraint`
            The SQLAlchemy constraint object.

        Raises
        ------
        ValueError
            If the constraint type is not recognized.
        TypeError
            If the constraint object is not the expected type.
        """
        args: dict[str, Any] = {
            "name": constraint_obj.name or None,
            "comment": constraint_obj.description or None,
            "deferrable": constraint_obj.deferrable or None,
            "initially": constraint_obj.initially or None,
        }
        constraint: Constraint

        if isinstance(constraint_obj, datamodel.ForeignKeyConstraint):
            fk_obj: datamodel.ForeignKeyConstraint = constraint_obj
            columns = [self._objects[column_id] for column_id in fk_obj.columns]
            refcolumns = [self._objects[column_id] for column_id in fk_obj.referenced_columns]
            constraint = ForeignKeyConstraint(columns, refcolumns, **args)
        elif isinstance(constraint_obj, datamodel.CheckConstraint):
            check_obj: datamodel.CheckConstraint = constraint_obj
            expression = check_obj.expression
            constraint = CheckConstraint(expression, **args)
        elif isinstance(constraint_obj, datamodel.UniqueConstraint):
            uniq_obj: datamodel.UniqueConstraint = constraint_obj
            columns = [self._objects[column_id] for column_id in uniq_obj.columns]
            constraint = UniqueConstraint(*columns, **args)
        else:
            raise ValueError(f"Unknown constraint type: {type(constraint_obj)}")

        self._objects[constraint_obj.id] = constraint

        return constraint

    def build_index(self, index_obj: datamodel.Index) -> Index:
        """Build a SQLAlchemy ``Index`` from a Felis `~felis.datamodel.Index`.

        Parameters
        ----------
        index_obj
            The Felis object from which to build the SQLAlchemy index.

        Returns
        -------
        `~sqlalchemy.sql.schema.Index`
            The SQLAlchemy index object.
        """
        columns = [self._objects[c_id] for c_id in (index_obj.columns if index_obj.columns else [])]
        expressions = index_obj.expressions if index_obj.expressions else []
        index = Index(index_obj.name, *columns, *expressions)
        self._objects[index_obj.id] = index
        return index
