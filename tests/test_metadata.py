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

import os
import unittest

import yaml
from sqlalchemy import (
    CheckConstraint,
    Constraint,
    ForeignKeyConstraint,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    UniqueConstraint,
    create_engine,
)

from felis import datamodel as dm
from felis.datamodel import Schema
from felis.db.utils import DatabaseContext
from felis.metadata import MetaDataBuilder, get_datatype_with_variants

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "sales.yaml")


class MetaDataTestCase(unittest.TestCase):
    """Test creation of SQLAlchemy metadata from a Felis schema."""

    def setUp(self) -> None:
        """Create an in-memory SQLite database and load the test data."""
        self.engine = create_engine("sqlite://")
        with open(TEST_YAML) as data:
            self.yaml_data = yaml.safe_load(data)

    def test_create_all(self) -> None:
        """Create all tables in the schema using the metadata object and a
        SQLite connection.

        Check that the reflected metadata matches that created by the builder.
        """

        def _sorted_indexes(indexes: set[Index]) -> list[Index]:
            """Return a sorted list of indexes."""
            return sorted(indexes, key=lambda i: i.name)

        def _sorted_constraints(constraints: set[Constraint]) -> list[Constraint]:
            """Return a sorted list of constraints with the
            `PrimaryKeyConstraint` objects filtered out.
            """
            return sorted(
                [c for c in constraints if not isinstance(c, PrimaryKeyConstraint)], key=lambda c: c.name
            )

        schema = Schema.model_validate(self.yaml_data)
        schema.name = "main"
        builder = MetaDataBuilder(schema)
        md = builder.build()

        ctx = DatabaseContext(md, self.engine)

        ctx.create_all()

        md_db = MetaData()
        md_db.reflect(self.engine.connect(), schema=schema.name)

        self.assertEqual(md_db.tables.keys(), md.tables.keys())

        for md_table_name in md.tables.keys():
            md_table = md.tables[md_table_name]
            md_db_table = md_db.tables[md_table_name]
            self.assertEqual(md_table.columns.keys(), md_db_table.columns.keys())
            for md_column_name in md_table.columns.keys():
                md_column = md_table.columns[md_column_name]
                md_db_column = md_db_table.columns[md_column_name]
                self.assertEqual(type(md_column.type), type(md_db_column.type))
                self.assertEqual(md_column.nullable, md_db_column.nullable)
                self.assertEqual(md_column.primary_key, md_db_column.primary_key)
            self.assertTrue(
                (md_table.constraints and md_db_table.constraints)
                or (not md_table.constraints and not md_table.constraints),
                "Constraints not created correctly",
            )
            if md_table.constraints:
                self.assertEqual(len(md_table.constraints), len(md_db_table.constraints))
                md_constraints = _sorted_constraints(md_table.constraints)
                md_db_constraints = _sorted_constraints(md_db_table.constraints)
                for md_constraint, md_db_constraint in zip(md_constraints, md_db_constraints):
                    self.assertEqual(md_constraint.name, md_db_constraint.name)
                    self.assertEqual(md_constraint.deferrable, md_db_constraint.deferrable)
                    self.assertEqual(md_constraint.initially, md_db_constraint.initially)
                    self.assertEqual(
                        type(md_constraint), type(md_db_constraint), "Constraint types do not match"
                    )
                    if isinstance(md_constraint, ForeignKeyConstraint) and isinstance(
                        md_db_constraint, ForeignKeyConstraint
                    ):
                        md_fk: ForeignKeyConstraint = md_constraint
                        md_db_fk: ForeignKeyConstraint = md_db_constraint
                        self.assertEqual(md_fk.referred_table.name, md_db_fk.referred_table.name)
                        self.assertEqual(md_fk.column_keys, md_db_fk.column_keys)
                    elif isinstance(md_constraint, UniqueConstraint) and isinstance(
                        md_db_constraint, UniqueConstraint
                    ):
                        md_uniq: UniqueConstraint = md_constraint
                        md_db_uniq: UniqueConstraint = md_db_constraint
                        self.assertEqual(md_uniq.columns.keys(), md_db_uniq.columns.keys())
                    elif isinstance(md_constraint, CheckConstraint) and isinstance(
                        md_db_constraint, CheckConstraint
                    ):
                        md_check: CheckConstraint = md_constraint
                        md_db_check: CheckConstraint = md_db_constraint
                        self.assertEqual(str(md_check.sqltext), str(md_db_check.sqltext))
            self.assertTrue(
                (md_table.indexes and md_db_table.indexes) or (not md_table.indexes and not md_table.indexes),
                "Indexes not created correctly",
            )
            if md_table.indexes:
                md_indexes = _sorted_indexes(md_table.indexes)
                md_db_indexes = _sorted_indexes(md_db_table.indexes)
                self.assertEqual(len(md_indexes), len(md_db_indexes))
                for md_index, md_db_index in zip(md_table.indexes, md_db_table.indexes):
                    self.assertEqual(md_index.name, md_db_index.name)
                    self.assertEqual(md_index.columns.keys(), md_db_index.columns.keys())

    def test_builder(self) -> None:
        """Test that the information in the metadata object created by the
        builder matches the data in the Felis schema used to create it.
        """
        sch = Schema.model_validate(self.yaml_data)
        bld = MetaDataBuilder(sch, apply_schema_to_metadata=False)
        md = bld.build()

        self.assertEqual(len(sch.tables), len(md.tables))
        self.assertEqual([table.name for table in sch.tables], list(md.tables.keys()))
        for table in sch.tables:
            md_table = md.tables[table.name]
            self.assertEqual(table.name, md_table.name)
            self.assertEqual(len(table.columns), len(md_table.columns))
            for column in table.columns:
                md_table_column = md_table.columns[column.name]
                datatype = get_datatype_with_variants(column)
                self.assertEqual(type(datatype), type(md_table_column.type))
                if column.nullable is not None:
                    self.assertEqual(column.nullable, md_table_column.nullable)
            for constraint in table.constraints:
                md_constraint = [mdc for mdc in md_table.constraints if mdc.name == constraint.name][0]
                if isinstance(constraint, dm.ForeignKeyConstraint):
                    self.assertTrue(isinstance(md_constraint, ForeignKeyConstraint))
                    self.assertTrue(
                        sorted([sch[column_id].name for column_id in constraint.columns]),
                        sorted(md_constraint.columns.keys()),
                    )
                elif isinstance(constraint, dm.UniqueConstraint):
                    self.assertEqual(
                        sorted([sch[column_id].name for column_id in constraint.columns]),
                        sorted(md_constraint.columns.keys()),
                    )
                elif isinstance(constraint, dm.CheckConstraint):
                    self.assertEqual(constraint.expression, str(md_constraint.sqltext))
            for index in table.indexes:
                md_index = [mdi for mdi in md_table.indexes if mdi.name == index.name][0]
                self.assertEqual(
                    sorted([sch[column_id].name for column_id in index.columns]),
                    sorted(md_index.columns.keys()),
                )
            if table.primary_key:
                if isinstance(table.primary_key, str):
                    primary_keys = [sch[table.primary_key].name]
                else:
                    primary_keys = [sch[pk].name for pk in table.primary_key]
                for primary_key in primary_keys:
                    self.assertTrue(md_table.columns[primary_key].primary_key)

    def test_timestamp(self) -> None:
        """Test that the `timestamp` datatype is created correctly."""
        for precision in [None, 6]:
            col = dm.Column(
                **{
                    "name": "timestamp_test",
                    "id": "#timestamp_test",
                    "datatype": "timestamp",
                    "precision": precision,
                }
            )
            datatype = get_datatype_with_variants(col)
            variant_dict = datatype._variant_mapping
            self.assertTrue("mysql" in variant_dict)
            self.assertTrue("postgresql" in variant_dict)
            pg_timestamp = variant_dict["postgresql"]
            self.assertEqual(pg_timestamp.timezone, False)
            self.assertEqual(pg_timestamp.precision, precision)
            mysql_timestamp = variant_dict["mysql"]
            self.assertEqual(mysql_timestamp.timezone, False)
            self.assertEqual(mysql_timestamp.fsp, precision)

    def test_ignore_constraints(self) -> None:
        """Test that constraints are not created when the
        ``ignore_constraints`` flag is set on the metadata builder.
        """
        schema = Schema.model_validate(self.yaml_data)
        schema.name = "main"
        builder = MetaDataBuilder(schema, ignore_constraints=True)
        md = builder.build()
        for table in md.tables.values():
            non_primary_key_constraints = [
                c for c in table.constraints if not isinstance(c, PrimaryKeyConstraint)
            ]
            self.assertEqual(
                len(non_primary_key_constraints),
                0,
                msg=f"Table {table.name} has non-primary key constraints defined",
            )

    def test_table_name_postfix(self) -> None:
        """Test that table name postfixes are correctly applied."""
        schema = Schema.model_validate(self.yaml_data)
        schema.name = "main"
        builder = MetaDataBuilder(schema, table_name_postfix="_test")
        md = builder.build()
        for table in md.tables.values():
            self.assertTrue(table.name.endswith("_test"))


if __name__ == "__main__":
    unittest.main()
