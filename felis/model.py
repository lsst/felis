import re

from sqlalchemy import create_engine, MetaData, Column, Numeric, ForeignKeyConstraint, \
    CheckConstraint, UniqueConstraint, PrimaryKeyConstraint, Index
from sqlalchemy.dialects import mysql, oracle, postgresql, sqlite
from sqlalchemy.schema import Table

from felis.db import sqltypes
from felis.felistypes import TYPE_NAMES, LENGTH_TYPES

MYSQL = "mysql"
ORACLE = "oracle"
POSTGRES = "postgresql"
SQLITE = "sqlite"

TABLE_OPTS = {
    "mysql:engine": "mysql_engine",
    "mysql:charset": "mysql_charset",
    "oracle:compress": "oracle_compress"
}

COLUMN_VARIANT_OVERRIDE = {
    "mysql:datatype": "mysql",
    "oracle:datatype": "oracle",
    "postgresql:datatype": "postgresql",
    "sqlite:datatype": "sqlite"
}

DIALECT_MODULES = {
    MYSQL: mysql,
    ORACLE: oracle,
    SQLITE: sqlite,
    POSTGRES: postgresql
}

length_regex = re.compile(r'\((.+)\)')


class Schema:
    pass


class Visitor:
    def __init__(self):
        self.graph_index = {}
        self.metadata = MetaData()

    def visit_schema(self, schema_obj):
        schema = Schema()
        schema.name = schema_obj["name"]
        schema.tables = [self.visit_table(t, schema_obj) for t in schema_obj["tables"]]
        schema.metadata = self.metadata
        schema.graph_index = self.graph_index
        return schema

    def visit_table(self, table_obj, schema_obj):
        columns = [self.visit_column(c, table_obj) for c in table_obj["columns"]]

        name = table_obj["name"]
        table_id = table_obj["@id"]
        description = table_obj.get("description")

        table = Table(
            name,
            self.metadata,
            *columns,
            schema=schema_obj["name"],
            comment=description
        )

        primary_key = self.visit_primary_key(table_obj.get("primaryKey", []), table_obj)
        if primary_key:
            table.append_constraint(primary_key)

        primary_key = self.visit_primary_key(table_obj.get("primaryKey"), table)
        if primary_key:
            table.append_constraint(primary_key)

        constraints = [self.visit_constraint(c, table) for c in table_obj.get("constraints", [])]
        for constraint in constraints:
            table.append_constraint(constraint)

        indexes = [self.visit_index(i, table) for i in table_obj.get("indexes", [])]
        for index in indexes:
            # FIXME: Hack because there's no table.add_index
            index._set_parent(table)
            table.indexes.add(index)

        self.graph_index[table_id] = table

    def visit_column(self, column_obj, table_obj):
        column_name = column_obj["name"]
        column_id = column_obj.get("@id")
        datatype_name = column_obj["datatype"]
        if datatype_name not in TYPE_NAMES:
            raise ValueError(f"Incorrect Type Name: {datatype_name}")
        column_description = column_obj.get("description")
        column_default = column_obj.get("value")
        column_length = column_obj.get("length")

        kwargs = {}
        for column_opt in column_obj.keys():
            if column_opt in COLUMN_VARIANT_OVERRIDE:
                dialect = COLUMN_VARIANT_OVERRIDE[column_opt]
                variant = _process_variant_override(dialect, column_obj[column_opt])
                kwargs[dialect] = variant

        datatype_fun = getattr(sqltypes, datatype_name)

        if datatype_fun.__name__ in LENGTH_TYPES:
            datatype = datatype_fun(column_length, **kwargs)
        else:
            datatype = datatype_fun(**kwargs)

        nullable_default = True
        if isinstance(datatype, Numeric):
            nullable_default = False

        column_nullable = column_obj.get("nullable", nullable_default)
        column_autoincrement = column_obj.get("autoincrement", "auto")

        column = Column(
            column_name,
            datatype,
            comment=column_description,
            autoincrement=column_autoincrement,
            nullable=column_nullable,
            server_default=column_default
        )
        self.graph_index[column_id] = column
        return column

    def visit_primary_key(self, primary_key_obj, table):
        if primary_key_obj:
            if not isinstance(primary_key_obj, list):
                primary_key_obj = [primary_key_obj]
            columns = [
                self.graph_index[c_id] for c_id in primary_key_obj
            ]
            return PrimaryKeyConstraint(*columns)
        return None

    def visit_constraint(self, constraint_obj, table):
        constraint_type = constraint_obj["@type"]
        constraint_id = constraint_obj.get("@id")

        constraint_args = {}
        # The following are not used on every constraint
        _set_if("name", constraint_obj.get("name"), constraint_args)
        _set_if("info", constraint_obj.get("description"), constraint_args)
        _set_if("expression", constraint_obj.get("expression"), constraint_args)
        _set_if("deferrable", constraint_obj.get("deferrable"), constraint_args)
        _set_if("initially", constraint_obj.get("initially"), constraint_args)

        columns = [
            self.graph_index[c_id] for c_id in constraint_obj.get("columns", [])
        ]
        if constraint_type == "ForeignKey":
            refcolumns = [
                self.graph_index[c_id] for c_id in constraint_obj.get("referencedColumns", [])
            ]
            constraint = ForeignKeyConstraint(columns, refcolumns, **constraint_args)
        elif constraint_type == "Check":
            expression = constraint_obj["expression"]
            constraint = CheckConstraint(expression, **constraint_args)
        elif constraint_type == "Unique":
            constraint = UniqueConstraint(*columns, **constraint_args)
        else:
            raise ValueError("Not a valid constraint type")
        self.graph_index[constraint_id] = constraint
        return constraint

    def visit_index(self, index_obj, table):
        name = index_obj["name"]
        description = index_obj.get("description")
        columns = [
            self.graph_index[c_id] for c_id in index_obj.get("columns", [])
        ]
        expressions = index_obj.get("expressions", [])
        return Index(name, *columns, *expressions, info=description)


def _set_if(key, value, mapping):
    if value is not None:
        mapping[key] = value


def _process_variant_override(dialect_name, variant_override_str):
    """Simple Data Type Override"""
    match = length_regex.search(variant_override_str)
    dialect = DIALECT_MODULES[dialect_name]
    variant_type_name = variant_override_str.split("(")[0]

    # Process Variant Type
    if variant_type_name not in dir(dialect):
        raise ValueError(f"Type {variant_type_name} not found in dialect {dialect_name}")
    variant_type = getattr(dialect, variant_type_name)
    length_params = []
    if match:
        length_params.extend([int(i) for i in match.group(1).split(",")])
    return variant_type(*length_params)


def test():
    import yaml
    obj = yaml.load(open("test.yml"))

    visitor = Visitor()
    schema = visitor.visit_schema(obj)

    metadata = schema.metadata

    def metadata_dump(sql, *multiparams, **params):
        # print or write to log or file etc
        print(sql.compile(dialect=engine.dialect))

    print("sqlite")
    engine = create_engine("sqlite:///:mem:", strategy='mock', executor=metadata_dump)
    metadata.create_all(engine)
    #
    print("mysql")
    engine = create_engine("mysql://", strategy='mock', executor=metadata_dump)
    metadata.create_all(engine)

    # print("oracle")
    # engine = create_engine("oracle://", strategy='mock', executor=metadata_dump)
    # metadata.create_all(engine)

    print("postgresql")
    engine = create_engine("postgresql://", strategy='mock', executor=metadata_dump)
    metadata.create_all(engine)


test()
