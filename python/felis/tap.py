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

__all__ = ["Tap11Base", "TapLoadingVisitor", "init_tables"]

import logging
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any

from sqlalchemy import Column, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.expression import Insert, insert

from .check import FelisValidator
from .types import FelisType
from .visitor import Visitor

_Mapping = Mapping[str, Any]

Tap11Base: Any = declarative_base()  # Any to avoid mypy mess with SA 2
logger = logging.getLogger("felis")

IDENTIFIER_LENGTH = 128
SMALL_FIELD_LENGTH = 32
SIMPLE_FIELD_LENGTH = 128
TEXT_FIELD_LENGTH = 2048
QUALIFIED_TABLE_LENGTH = 3 * IDENTIFIER_LENGTH + 2

_init_table_once = False


def init_tables(
    tap_schema_name: str | None = None,
    tap_tables_postfix: str | None = None,
    tap_schemas_table: str | None = None,
    tap_tables_table: str | None = None,
    tap_columns_table: str | None = None,
    tap_keys_table: str | None = None,
    tap_key_columns_table: str | None = None,
) -> MutableMapping[str, Any]:
    """Generate definitions for TAP tables."""
    postfix = tap_tables_postfix or ""

    # Dirty hack to enable this method to be called more than once, replaces
    # MetaData instance with a fresh copy if called more than once.
    # TODO: probably replace ORM stuff with core sqlalchemy functions.
    global _init_table_once
    if not _init_table_once:
        _init_table_once = True
    else:
        Tap11Base.metadata = MetaData()

    if tap_schema_name:
        Tap11Base.metadata.schema = tap_schema_name

    class Tap11Schemas(Tap11Base):
        __tablename__ = (tap_schemas_table or "schemas") + postfix
        schema_name = Column(String(IDENTIFIER_LENGTH), primary_key=True, nullable=False)
        utype = Column(String(SIMPLE_FIELD_LENGTH))
        description = Column(String(TEXT_FIELD_LENGTH))
        schema_index = Column(Integer)

    class Tap11Tables(Tap11Base):
        __tablename__ = (tap_tables_table or "tables") + postfix
        schema_name = Column(String(IDENTIFIER_LENGTH), nullable=False)
        table_name = Column(String(QUALIFIED_TABLE_LENGTH), nullable=False, primary_key=True)
        table_type = Column(String(SMALL_FIELD_LENGTH), nullable=False)
        utype = Column(String(SIMPLE_FIELD_LENGTH))
        description = Column(String(TEXT_FIELD_LENGTH))
        table_index = Column(Integer)

    class Tap11Columns(Tap11Base):
        __tablename__ = (tap_columns_table or "columns") + postfix
        table_name = Column(String(QUALIFIED_TABLE_LENGTH), nullable=False, primary_key=True)
        column_name = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)
        datatype = Column(String(SIMPLE_FIELD_LENGTH), nullable=False)
        arraysize = Column(String(10))
        xtype = Column(String(SIMPLE_FIELD_LENGTH))
        # Size is deprecated
        # size = Column(Integer(), quote=True)
        description = Column(String(TEXT_FIELD_LENGTH))
        utype = Column(String(SIMPLE_FIELD_LENGTH))
        unit = Column(String(SIMPLE_FIELD_LENGTH))
        ucd = Column(String(SIMPLE_FIELD_LENGTH))
        indexed = Column(Integer, nullable=False)
        principal = Column(Integer, nullable=False)
        std = Column(Integer, nullable=False)
        column_index = Column(Integer)

    class Tap11Keys(Tap11Base):
        __tablename__ = (tap_keys_table or "keys") + postfix
        key_id = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)
        from_table = Column(String(QUALIFIED_TABLE_LENGTH), nullable=False)
        target_table = Column(String(QUALIFIED_TABLE_LENGTH), nullable=False)
        description = Column(String(TEXT_FIELD_LENGTH))
        utype = Column(String(SIMPLE_FIELD_LENGTH))

    class Tap11KeyColumns(Tap11Base):
        __tablename__ = (tap_key_columns_table or "key_columns") + postfix
        key_id = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)
        from_column = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)
        target_column = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)

    return dict(
        schemas=Tap11Schemas,
        tables=Tap11Tables,
        columns=Tap11Columns,
        keys=Tap11Keys,
        key_columns=Tap11KeyColumns,
    )


class TapLoadingVisitor(Visitor[None, tuple, Tap11Base, None, tuple, None, None]):
    """Felis schema visitor for generating TAP schema.

    Parameters
    ----------
    engine : `sqlalchemy.engine.Engine` or `None`
        SQLAlchemy engine instance.
    catalog_name : `str` or `None`
        Name of the database catalog.
    schema_name : `str` or `None`
        Name of the database schema.
    tap_tables : `~collections.abc.Mapping`
        Optional mapping of table name to its declarative base class.
    """

    def __init__(
        self,
        engine: Engine | None,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        tap_tables: MutableMapping[str, Any] | None = None,
    ):
        self.graph_index: MutableMapping[str, Any] = {}
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.engine = engine
        self._mock_connection: MockConnection | None = None
        self.tables = tap_tables or init_tables()
        self.checker = FelisValidator()

    @classmethod
    def from_mock_connection(
        cls,
        mock_connection: MockConnection,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        tap_tables: MutableMapping[str, Any] | None = None,
    ) -> TapLoadingVisitor:
        visitor = cls(engine=None, catalog_name=catalog_name, schema_name=schema_name, tap_tables=tap_tables)
        visitor._mock_connection = mock_connection
        return visitor

    def visit_schema(self, schema_obj: _Mapping) -> None:
        self.checker.check_schema(schema_obj)
        if (version_obj := schema_obj.get("version")) is not None:
            self.visit_schema_version(version_obj, schema_obj)
        schema = self.tables["schemas"]()
        # Override with default
        self.schema_name = self.schema_name or schema_obj["name"]

        schema.schema_name = self._schema_name()
        schema.description = schema_obj.get("description")
        schema.utype = schema_obj.get("votable:utype")
        schema.schema_index = int(schema_obj.get("tap:schema_index", 0))

        if self.engine is not None:
            session: Session = sessionmaker(self.engine)()

            session.add(schema)

            for table_obj in schema_obj["tables"]:
                table, columns = self.visit_table(table_obj, schema_obj)
                session.add(table)
                session.add_all(columns)

            keys, key_columns = self.visit_constraints(schema_obj)
            session.add_all(keys)
            session.add_all(key_columns)

            session.commit()
        else:
            logger.info("Dry run, not inserting into database")

            # Only if we are mocking (dry run)
            assert self._mock_connection is not None, "Mock connection must not be None"
            conn = self._mock_connection
            conn.execute(_insert(self.tables["schemas"], schema))

            for table_obj in schema_obj["tables"]:
                table, columns = self.visit_table(table_obj, schema_obj)
                conn.execute(_insert(self.tables["tables"], table))
                for column in columns:
                    conn.execute(_insert(self.tables["columns"], column))

            keys, key_columns = self.visit_constraints(schema_obj)
            for key in keys:
                conn.execute(_insert(self.tables["keys"], key))
            for key_column in key_columns:
                conn.execute(_insert(self.tables["key_columns"], key_column))

    def visit_constraints(self, schema_obj: _Mapping) -> tuple:
        all_keys = []
        all_key_columns = []
        for table_obj in schema_obj["tables"]:
            for c in table_obj.get("constraints", []):
                key, key_columns = self.visit_constraint(c, table_obj)
                if not key:
                    continue
                all_keys.append(key)
                all_key_columns += key_columns
        return all_keys, all_key_columns

    def visit_schema_version(
        self, version_obj: str | Mapping[str, Any], schema_obj: Mapping[str, Any]
    ) -> None:
        # Docstring is inherited.

        # For now we ignore schema versioning completely, still do some checks.
        self.checker.check_schema_version(version_obj, schema_obj)

    def visit_table(self, table_obj: _Mapping, schema_obj: _Mapping) -> tuple:
        self.checker.check_table(table_obj, schema_obj)
        table_id = table_obj["@id"]
        table = self.tables["tables"]()
        table.schema_name = self._schema_name()
        table.table_name = self._table_name(table_obj["name"])
        table.table_type = "table"
        table.utype = table_obj.get("votable:utype")
        table.description = table_obj.get("description")
        table.table_index = int(table_obj.get("tap:table_index", 0))

        columns = [self.visit_column(c, table_obj) for c in table_obj["columns"]]
        self.visit_primary_key(table_obj.get("primaryKey", []), table_obj)

        for i in table_obj.get("indexes", []):
            self.visit_index(i, table)

        self.graph_index[table_id] = table
        return table, columns

    def check_column(self, column_obj: _Mapping, table_obj: _Mapping) -> None:
        self.checker.check_column(column_obj, table_obj)
        _id = column_obj["@id"]
        # Guaranteed to exist at this point, for mypy use "" as default
        datatype_name = column_obj.get("datatype", "")
        felis_type = FelisType.felis_type(datatype_name)
        if felis_type.is_sized:
            # It is expected that both arraysize and length are fine for
            # length types.
            arraysize = column_obj.get("votable:arraysize", column_obj.get("length"))
            if arraysize is None:
                logger.warning(
                    f"votable:arraysize and length for {_id} are None for type {datatype_name}. "
                    'Using length "*". '
                    "Consider setting `votable:arraysize` or `length`."
                )
        if felis_type.is_timestamp:
            # datetime types really should have a votable:arraysize, because
            # they are converted to strings and the `length` is loosely to the
            # string size
            if "votable:arraysize" not in column_obj:
                logger.warning(
                    f"votable:arraysize for {_id} is None for type {datatype_name}. "
                    f'Using length "*". '
                    "Consider setting `votable:arraysize` to an appropriate size for "
                    "materialized datetime/timestamp strings."
                )

    def visit_column(self, column_obj: _Mapping, table_obj: _Mapping) -> Tap11Base:
        self.check_column(column_obj, table_obj)
        column_id = column_obj["@id"]
        table_name = self._table_name(table_obj["name"])

        column = self.tables["columns"]()
        column.table_name = table_name
        column.column_name = column_obj["name"]

        felis_datatype = column_obj["datatype"]
        felis_type = FelisType.felis_type(felis_datatype)
        column.datatype = column_obj.get("votable:datatype", felis_type.votable_name)

        arraysize = None
        if felis_type.is_sized:
            # prefer votable:arraysize to length, fall back to `*`
            arraysize = column_obj.get("votable:arraysize", column_obj.get("length", "*"))
        if felis_type.is_timestamp:
            arraysize = column_obj.get("votable:arraysize", "*")
        column.arraysize = arraysize

        column.xtype = column_obj.get("votable:xtype")
        column.description = column_obj.get("description")
        column.utype = column_obj.get("votable:utype")

        unit = column_obj.get("ivoa:unit") or column_obj.get("fits:tunit")
        column.unit = unit
        column.ucd = column_obj.get("ivoa:ucd")

        # We modify this after we process columns
        column.indexed = 0

        column.principal = column_obj.get("tap:principal", 0)
        column.std = column_obj.get("tap:std", 0)
        column.column_index = column_obj.get("tap:column_index")

        self.graph_index[column_id] = column
        return column

    def visit_primary_key(self, primary_key_obj: str | Iterable[str], table_obj: _Mapping) -> None:
        self.checker.check_primary_key(primary_key_obj, table_obj)
        if primary_key_obj:
            if isinstance(primary_key_obj, str):
                primary_key_obj = [primary_key_obj]
            columns = [self.graph_index[c_id] for c_id in primary_key_obj]
            # if just one column and it's indexed, update the object
            if len(columns) == 1:
                columns[0].indexed = 1
        return None

    def visit_constraint(self, constraint_obj: _Mapping, table_obj: _Mapping) -> tuple:
        self.checker.check_constraint(constraint_obj, table_obj)
        constraint_type = constraint_obj["@type"]
        key = None
        key_columns = []
        if constraint_type == "ForeignKey":
            constraint_name = constraint_obj["name"]
            description = constraint_obj.get("description")
            utype = constraint_obj.get("votable:utype")

            columns = [self.graph_index[col["@id"]] for col in constraint_obj.get("columns", [])]
            refcolumns = [
                self.graph_index[refcol["@id"]] for refcol in constraint_obj.get("referencedColumns", [])
            ]

            table_name = None
            for column in columns:
                if not table_name:
                    table_name = column.table_name
                if table_name != column.table_name:
                    raise ValueError("Inconsisent use of table names")

            table_name = None
            for column in refcolumns:
                if not table_name:
                    table_name = column.table_name
                if table_name != column.table_name:
                    raise ValueError("Inconsisent use of table names")
            first_column = columns[0]
            first_refcolumn = refcolumns[0]

            key = self.tables["keys"]()
            key.key_id = constraint_name
            key.from_table = first_column.table_name
            key.target_table = first_refcolumn.table_name
            key.description = description
            key.utype = utype
            for column, refcolumn in zip(columns, refcolumns):
                key_column = self.tables["key_columns"]()
                key_column.key_id = constraint_name
                key_column.from_column = column.column_name
                key_column.target_column = refcolumn.column_name
                key_columns.append(key_column)
        return key, key_columns

    def visit_index(self, index_obj: _Mapping, table_obj: _Mapping) -> None:
        self.checker.check_index(index_obj, table_obj)
        columns = [self.graph_index[col["@id"]] for col in index_obj.get("columns", [])]
        # if just one column and it's indexed, update the object
        if len(columns) == 1:
            columns[0].indexed = 1
        return None

    def _schema_name(self, schema_name: str | None = None) -> str | None:
        # If _schema_name is None, SQLAlchemy will catch it
        _schema_name = schema_name or self.schema_name
        if self.catalog_name and _schema_name:
            return ".".join([self.catalog_name, _schema_name])
        return _schema_name

    def _table_name(self, table_name: str) -> str:
        schema_name = self._schema_name()
        if schema_name:
            return ".".join([schema_name, table_name])
        return table_name


def _insert(table: Tap11Base, value: Any) -> Insert:
    """Return a SQLAlchemy insert statement.

    Parameters
    ----------
    table : `Tap11Base`
        The table we are inserting into.
    value : `Any`
        An object representing the object we are inserting to the table.

    Returns
    -------
    statement
        A SQLAlchemy insert statement
    """
    values_dict = {}
    for i in table.__table__.columns:
        name = i.name
        column_value = getattr(value, i.name)
        if isinstance(column_value, str):
            column_value = column_value.replace("'", "''")
        values_dict[name] = column_value
    return insert(table).values(values_dict)
