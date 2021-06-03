import logging

from sqlalchemy import Column, String, Integer, Table
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql.expression import insert
from .felistypes import LENGTH_TYPES, VOTABLE_MAP, DATETIME_TYPES

from .model import VisitorBase

Tap11Base = declarative_base()
logger = logging.getLogger("felis")

IDENTIFIER_LENGTH = 128
SMALL_FIELD_LENGTH = 32
SIMPLE_FIELD_LENGTH = 128
TEXT_FIELD_LENGTH = 2048
QUALIFIED_TABLE_LENGTH = 3 * IDENTIFIER_LENGTH + 2


def init_tables(
    tap_schema_name=None,
    tap_schemas_table=None,
    tap_tables_table=None,
    tap_columns_table=None,
    tap_keys_table=None,
    tap_key_columns_table=None,
):
    if tap_schema_name:
        Tap11Base.metadata.schema = tap_schema_name

    class Tap11Schemas(Tap11Base):
        __tablename__ = tap_schemas_table or "schemas"
        schema_name = Column(String(IDENTIFIER_LENGTH), primary_key=True, nullable=False)
        utype = Column(String(SIMPLE_FIELD_LENGTH))
        description = Column(String(TEXT_FIELD_LENGTH))
        schema_index = Column(Integer)

    class Tap11Tables(Tap11Base):
        __tablename__ = tap_tables_table or "tables"
        schema_name = Column(String(IDENTIFIER_LENGTH), nullable=False)
        table_name = Column(String(QUALIFIED_TABLE_LENGTH), nullable=False, primary_key=True)
        table_type = Column(String(SMALL_FIELD_LENGTH), nullable=False)
        utype = Column(String(SIMPLE_FIELD_LENGTH))
        description = Column(String(TEXT_FIELD_LENGTH))
        table_index = Column(Integer)

    class Tap11Columns(Tap11Base):
        __tablename__ = tap_columns_table or "columns"
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
        __tablename__ = tap_keys_table or "keys"
        key_id = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)
        from_table = Column(String(QUALIFIED_TABLE_LENGTH), nullable=False)
        target_table = Column(String(QUALIFIED_TABLE_LENGTH), nullable=False)
        description = Column(String(TEXT_FIELD_LENGTH))
        utype = Column(String(SIMPLE_FIELD_LENGTH))

    class Tap11KeyColumns(Tap11Base):
        __tablename__ = tap_key_columns_table or "key_columns"
        key_id = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)
        from_column = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)
        target_column = Column(String(IDENTIFIER_LENGTH), nullable=False, primary_key=True)

    return dict(
        schemas=Tap11Schemas, tables=Tap11Tables, columns=Tap11Columns, keys=Tap11Keys, key_columns=Tap11KeyColumns
    )


class TapLoadingVisitor(VisitorBase):
    def __init__(self, engine: Engine, catalog_name=None, schema_name=None, mock=False, tap_tables=None):
        self.graph_index = {}
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.engine = engine
        self.mock = mock
        self.tables = tap_tables or init_tables()

    def visit_schema(self, schema_obj):
        schema = self.tables["schemas"]
        # Override with default
        self.schema_name = self.schema_name or schema_obj["name"]

        schema.schema_name = self._schema_name()
        schema.description = schema_obj.get("description")
        schema.utype = schema_obj.get("votable:utype")
        schema.schema_index = int(schema_obj.get("tap:schema_index", 0))

        if not self.mock:
            session: Session = sessionmaker(self.engine)()
            session.add(schema)
            for table_obj in schema_obj["tables"]:
                table, columns, keys, key_columns = self.visit_table(table_obj, schema_obj)
                session.add(table)
                session.add_all(columns)
                session.add_all(keys)
                session.add_all(key_columns)
            session.commit()
        else:
            # Only if we are mocking (dry run)
            conn = self.engine
            conn.execute(_insert(self.tables["schemas"], schema))
            for table_obj in schema_obj["tables"]:
                table, columns, keys, key_columns = self.visit_table(table_obj, schema_obj)
                conn.execute(_insert(self.tables["tables"], table))
                for column in columns:
                    conn.execute(_insert(self.tables["columns"], column))
                for key in keys:
                    conn.execute(_insert(self.tables["keys"], key))
                for key_column in key_columns:
                    conn.execute(_insert(self.tables["key_columns"], key_column))

    def visit_table(self, table_obj, schema_obj):
        self.check_table(table_obj, schema_obj)
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
        all_keys = []
        all_key_columns = []
        for c in table_obj.get("constraints", []):
            key, key_columns = self.visit_constraint(c, table)
            if not key:
                continue
            all_keys.append(key)
            all_key_columns += key_columns

        for i in table_obj.get("indexes", []):
            self.visit_index(i, table)

        self.graph_index[table_id] = table
        return table, columns, all_keys, all_key_columns

    def check_column(self, column_obj, table_obj):
        super().check_column(column_obj, table_obj)
        _id = column_obj["@id"]
        datatype_name = column_obj.get("datatype")
        if datatype_name in LENGTH_TYPES:
            # It is expected that both arraysize and length are fine for length types.
            arraysize = column_obj.get("votable:arraysize", column_obj.get("length"))
            if arraysize is None:
                logger.warning(
                    f"votable:arraysize and length for {_id} are None for type {datatype_name}. "
                    'Using length "*". '
                    "Consider setting `votable:arraysize` or `length`."
                )
        if datatype_name in DATETIME_TYPES:
            # datetime types really should have a votable:arraysize, because they are converted
            # to strings and the `length` is loosely to the string size
            if "votable:arraysize" not in column_obj:
                logger.warning(
                    f"votable:arraysize for {_id} is None for type {datatype_name}. "
                    f'Using length "*". '
                    "Consider setting `votable:arraysize` to an appropriate size for "
                    "materialized datetime/timestamp strings."
                )

    def visit_column(self, column_obj, table_obj):
        self.check_column(column_obj, table_obj)
        column_id = column_obj["@id"]
        table_name = self._table_name(table_obj["name"])

        column = self.tables["columns"]()
        column.table_name = table_name
        column.column_name = column_obj["name"]

        felis_datatype = column_obj["datatype"]
        ivoa_datatype = column_obj.get("votable:datatype", VOTABLE_MAP[felis_datatype])
        column.datatype = column_obj.get("votable:datatype", ivoa_datatype)

        arraysize = None
        if felis_datatype in LENGTH_TYPES:
            # prefer votable:arraysize to length, fall back to `*`
            arraysize = column_obj.get("votable:arraysize", column_obj.get("length", "*"))
        if felis_datatype in DATETIME_TYPES:
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

    def visit_primary_key(self, primary_key_obj, table_obj):
        if primary_key_obj:
            if not isinstance(primary_key_obj, list):
                primary_key_obj = [primary_key_obj]
            columns = [self.graph_index[c_id] for c_id in primary_key_obj]
            # if just one column and it's indexed, update the object
            if len(columns) == 1:
                columns[0].indexed = 1
        return None

    def visit_constraint(self, constraint_obj, table_obj):
        self.check_constraint(constraint_obj, table_obj)
        constraint_type = constraint_obj["@type"]
        key = None
        key_columns = []
        if constraint_type == "ForeignKey":
            constraint_name = constraint_obj["name"]
            description = constraint_obj.get("description")
            utype = constraint_obj.get("votable:utype")

            columns = [self.graph_index[c_id] for c_id in constraint_obj.get("columns", [])]
            refcolumns = [self.graph_index[c_id] for c_id in constraint_obj.get("referencedColumns", [])]

            table_name = None
            for column in columns:
                if not column.table_name:
                    table_name = column.table_name
                if table_name != column.table_name:
                    raise ValueError("Inconsisent use of table names")

            table_name = None
            for column in refcolumns:
                if not column.table_name:
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

    def visit_index(self, index_obj, table_obj):
        self.check_index(index_obj, table_obj)
        columns = [self.graph_index[c_id] for c_id in index_obj.get("columns", [])]
        # if just one column and it's indexed, update the object
        if len(columns) == 1:
            columns[0].indexed = 1
        return None

    def _schema_name(self, schema_name=None):
        # If _schema_name is None, SQLAlchemy will catch it
        _schema_name = schema_name or self.schema_name
        if self.catalog_name:
            return ".".join([self.catalog_name, self.schema_name])
        return self.schema_name

    def _table_name(self, table_name):
        return ".".join([self._schema_name(), table_name])


def _insert(table, value):
    """
    Return a SQLAlchemy insert statement based on
    :param table: The table we are inserting to
    :param value: An object representing the object we are inserting
    to the table
    :return: A SQLAlchemy insert statement
    """
    values_dict = {}
    for i in table.__table__.columns:
        name = i.name
        column_value = getattr(value, i.name)
        if type(column_value) == str:
            column_value = column_value.replace("'", "''")
        values_dict[name] = column_value
    return insert(table, values=values_dict)
