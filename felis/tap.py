from typing import List

from sqlalchemy import Column, String, Integer
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

from felistypes import LENGTH_TYPES

Base = declarative_base()


class Tap11Schemas(Base):
    __tablename__ = "schemas"
    schema_name = Column(String(), primary_key=True, nullable=False)
    utype = Column(String())
    description = Column(String())
    schema_index = Column(Integer())


class Tap11Tables(Base):
    __tablename__ = "tables"
    schema_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False, primary_key=True)
    table_type = Column(String, nullable=False)
    utype = Column(String)
    description = Column(String)
    table_index = Column(Integer)


class Tap11Columns(Base):
    __tablename__ = "columns"
    table_name = Column(String, nullable=False, primary_key=True)
    column_name = Column(String, nullable=False, primary_key=True)
    datatype = Column(String, nullable=False)
    arraysize = Column(String)
    xtype = Column(String)
    # Size is deprecated
    # size = Column(Integer(), quote=True)
    description = Column(String)
    utype = Column(String)
    unit = Column(String)
    ucd = Column(String)
    indexed = Column(Integer, nullable=False)
    principal = Column(Integer, nullable=False)
    std = Column(Integer, nullable=False)
    column_index = Column(Integer)


class Tap11Keys(Base):
    __tablename__ = "keys"
    key_id = Column(String, nullable=False, primary_key=True)
    from_table = Column(String, nullable=False)
    target_table = Column(String, nullable=False)
    description = Column(String)
    utype = Column(String)


class Tap11KeyColumns(Base):
    __tablename__ = "key_columns"
    key_id = Column(String, nullable=False, primary_key=True)
    from_column = Column(String, nullable=False, primary_key=True)
    target_column = Column(String, nullable=False, primary_key=True)


class TapVisitor:
    def __init__(self, engine: Engine, catalog_name=None, schema_name=None):
        self.graph_index = {}
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.session: Session = sessionmaker(engine)()

    def visit_schema(self, schema_obj):
        schema = Tap11Schemas()
        # Override with default
        self.schema_name = self.schema_name or schema_obj["name"]

        schema.schema_name = self._schema_name()
        schema.description = schema_obj.get("description")
        schema.utype = schema_obj.get("votable:utype")
        schema.schema_index = int(schema_obj.get("tap:schema_index",0))
        self.session.add(schema)
        for table_obj in schema_obj["tables"]:
            table, columns, keys, key_columns = self.visit_table(table_obj, schema_obj)
            self.session.add(table)
            self.session.add_all(columns)
            self.session.add_all(keys)
            self.session.add_all(key_columns)
        self.session.commit()

    def visit_table(self, table_obj, schema_obj):
        table_id = table_obj["@id"]
        table = Tap11Tables()
        table.schema_name = self._schema_name()
        table.table_name = self._table_name(table_obj["name"])
        table.table_type = "TABLE"
        table.utype = table_obj.get("votable:utype")
        table.description = table_obj.get("description")
        table.table_index = int(schema_obj.get("tap:table_index", 0))

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

    def visit_column(self, column_obj, table_obj):
        column_id = column_obj["@id"]
        table_name = self._table_name(table_obj["name"])
        felis_datatype = column_obj["datatype"]

        column = Tap11Columns()
        column.table_name = table_name
        column.column_name = column_obj["name"]
        column.datatype = column_obj.get("votable:datatype", felis_datatype)

        arraysize = None
        if felis_datatype in LENGTH_TYPES:
            arraysize = column_obj.get("votable:arraysize", column_obj.get("length"))
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

    def visit_primary_key(self, primary_key_obj, table):
        if primary_key_obj:
            if not isinstance(primary_key_obj, list):
                primary_key_obj = [primary_key_obj]
            columns = [
                self.graph_index[c_id] for c_id in primary_key_obj
            ]
            # if just one column and it's indexed, update the object
            if len(columns) == 1:
                columns[0].indexed = 1
        return None

    def visit_constraint(self, constraint_obj, table):
        constraint_type = constraint_obj["@type"]
        key = None
        key_columns = []
        if constraint_type == "ForeignKey":
            constraint_name = constraint_obj["name"]
            description = constraint_obj.get("description")
            utype = constraint_obj.get("votable:utype")

            columns: List[Tap11Columns] = [
                self.graph_index[c_id] for c_id in constraint_obj.get("columns", [])
            ]
            refcolumns: List[Tap11Columns] = [
                self.graph_index[c_id] for c_id in constraint_obj.get("referencedColumns", [])
            ]

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

            key = Tap11Keys()
            key.key_id = constraint_name
            key.from_table = first_column.table_name
            key.target_table = first_refcolumn.table_name
            key.description = description
            key.utype = utype
            for column, refcolumn in zip(columns, refcolumns):
                key_column = Tap11KeyColumns()
                key_column.key_id = constraint_name
                key_column.from_column = column.column_name
                key_column.target_column = refcolumn.column_name
                key_columns.append(key_column)
        return key, key_columns

    def visit_index(self, index_obj, table):
        columns = [
            self.graph_index[c_id] for c_id in index_obj.get("columns", [])
        ]
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


def test():
    import yaml
    obj = yaml.load(open("test.yml"))

    metadata = Base.metadata

    engine = create_engine("sqlite:///test.db")
    metadata.create_all(engine)

    tv = TapVisitor(engine)
    tv.visit_schema(obj)
