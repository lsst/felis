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
import pathlib
import unittest
from collections import defaultdict

import yaml
from lsst.resources import ResourcePath
from pydantic import ValidationError

from felis.datamodel import (
    CheckConstraint,
    Column,
    Constraint,
    DataType,
    ForeignKeyConstraint,
    Index,
    Schema,
    SchemaVersion,
    Table,
    UniqueConstraint,
)
from felis.tests.utils import get_test_file_path, open_test_file

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class ColumnTestCase(unittest.TestCase):
    """Test the ``Column`` class."""

    def test_validation(self) -> None:
        """Test Pydantic validation of the ``Column`` class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Column()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Column(name="testColumn")

        # Setting name and id should throw an exception from missing datatype.
        with self.assertRaises(ValidationError):
            Column(name="testColumn", id="#test_id")

        # Setting name, id, and datatype should not throw an exception and
        # should load data correctly.
        col = Column(name="testColumn", id="#test_id", datatype="string", length=256)
        self.assertEqual(col.name, "testColumn", "name should be 'testColumn'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.datatype, DataType.string, "datatype should be 'DataType.string'")

        # Creating from data dictionary should work and load data correctly.
        data = {"name": "testColumn", "id": "#test_id", "datatype": "string", "length": 256}
        col = Column(**data)
        self.assertEqual(col.name, "testColumn", "name should be 'testColumn'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.datatype, DataType.string, "datatype should be 'DataType.string'")

        # Setting a bad IVOA UCD should throw an error.
        with self.assertRaises(ValidationError):
            Column(**data, ivoa_ucd="bad")

        # Setting a valid IVOA UCD should not throw an error.
        col = Column(**data, ivoa_ucd="meta.id")
        self.assertEqual(col.ivoa_ucd, "meta.id", "ivoa_ucd should be 'meta.id'")

        units_data = data.copy()

        # Setting a bad IVOA unit should throw an error.
        units_data["ivoa:unit"] = "bad"
        with self.assertRaises(ValidationError):
            Column(**units_data)

        # Setting a valid IVOA unit should not throw an error.
        units_data["ivoa:unit"] = "m"
        col = Column(**units_data)
        self.assertEqual(col.ivoa_unit, "m", "ivoa_unit should be 'm'")

        units_data = data.copy()

        # Setting a bad FITS TUNIT should throw an error.
        units_data["fits:tunit"] = "bad"
        with self.assertRaises(ValidationError):
            Column(**units_data)

        # Setting a valid FITS TUNIT should not throw an error.
        units_data["fits:tunit"] = "m"
        col = Column(**units_data)
        self.assertEqual(col.fits_tunit, "m", "fits_tunit should be 'm'")

        # Setting both IVOA unit and FITS TUNIT should throw an error.
        units_data["ivoa:unit"] = "m"
        with self.assertRaises(ValidationError):
            Column(**units_data)

    def test_description(self) -> None:
        """Test Pydantic validation of the ``description`` attribute."""
        # Creating a column with a description of 'None' should throw.
        with self.assertRaises(ValueError):
            Column(
                **{
                    "name": "testColumn",
                    "@id": "#test_col_id",
                    "datatype": "string",
                    "description": None,
                }
            )

        # Creating a column with an empty description should throw.
        with self.assertRaises(ValueError):
            Column(
                **{
                    "name": "testColumn",
                    "@id": "#test_col_id",
                    "datatype": "string",
                    "description": "",
                }
            )

        # Creating a column with a description that is too short should throw.
        with self.assertRaises(ValidationError):
            Column(
                **{
                    "name": "testColumn",
                    "@id": "#test_col_id",
                    "datatype": "string",
                    "description": "xy",
                }
            )

    def test_values(self) -> None:
        """Test Pydantic validation of the ``value`` attribute."""

        # Define a function to return the default column data
        def default_coldata():
            return defaultdict(str, {"name": "testColumn", "@id": "#test_col_id"})

        # Setting both value and autoincrement should throw.
        autoincr_coldata = default_coldata()
        autoincr_coldata["datatype"] = "int"
        autoincr_coldata["autoincrement"] = True
        autoincr_coldata["value"] = 1
        with self.assertRaises(ValueError):
            Column(**autoincr_coldata)

        # Setting an invalid default on a column with an integer type should
        # throw.
        bad_numeric_coldata = default_coldata()
        for datatype in ["int", "long", "short", "byte"]:
            for value in ["bad", "1.0", "1", 1.1]:
                bad_numeric_coldata["datatype"] = datatype
                bad_numeric_coldata["value"] = value
                with self.assertRaises(ValueError):
                    Column(**bad_numeric_coldata)

        # Setting an invalid default on a column with a decimal type should
        # throw.
        bad_numeric_coldata = default_coldata()
        for datatype in ["double", "float"]:
            for value in ["bad", "1.0", "1", 1]:
                bad_numeric_coldata["datatype"] = datatype
                bad_numeric_coldata["value"] = value
                with self.assertRaises(ValueError):
                    Column(**bad_numeric_coldata)

        # Setting a bad default on a string column should throw.
        bad_str_coldata = default_coldata()
        bad_str_coldata["value"] = 1
        bad_str_coldata["length"] = 256
        for datatype in ["string", "char", "unicode", "text"]:
            for value in [1, 1.1, True, "", " ", "    ", "\n", "\t"]:
                bad_str_coldata["datatype"] = datatype
                bad_str_coldata["value"] = value
                with self.assertRaises(ValueError):
                    Column(**bad_str_coldata)

        # Setting a non-boolean value on a boolean column should throw.
        bool_coldata = default_coldata()
        bool_coldata["datatype"] = "boolean"
        bool_coldata["value"] = "bad"
        with self.assertRaises(ValueError):
            for value in ["bad", 1, 1.1]:
                bool_coldata["value"] = value
                Column(**bool_coldata)

        # Setting a valid value on a string column should be okay.
        str_coldata = default_coldata()
        str_coldata["value"] = 1
        str_coldata["length"] = 256
        str_coldata["value"] = "okay"
        for datatype in ["string", "char", "unicode", "text"]:
            str_coldata["datatype"] = datatype
            Column(**str_coldata)

        # Setting an integer value on a column with an int type should be okay.
        int_coldata = default_coldata()
        int_coldata["value"] = 1
        for datatype in ["int", "long", "short", "byte"]:
            int_coldata["datatype"] = datatype
            Column(**int_coldata)

        # Setting a decimal value on a column with a float type should be okay.
        bool_coldata = default_coldata()
        bool_coldata["datatype"] = "boolean"
        bool_coldata["value"] = True
        Column(**bool_coldata)


class TableTestCase(unittest.TestCase):
    """Test Pydantic validation of the ``Table`` class."""

    def test_validation(self) -> None:
        """Test Pydantic validation of the ``Table`` class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Table()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Table(name="testTable")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            Index(name="testTable", id="#test_id")

        testCol = Column(name="testColumn", id="#test_id", datatype="string", length=256)

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        tbl = Table(name="testTable", id="#test_id", columns=[testCol])
        self.assertEqual(tbl.name, "testTable", "name should be 'testTable'")
        self.assertEqual(tbl.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(tbl.columns, [testCol], "columns should be ['testColumn']")

        # Creating a table with duplicate column names should raise an
        # exception.
        with self.assertRaises(ValidationError):
            Table(name="testTable", id="#test_id", columns=[testCol, testCol])


class ConstraintTestCase(unittest.TestCase):
    """Test Pydantic validation of the different constraint classes."""

    def test_base_constraint(self) -> None:
        """Test validation of base constraint type."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Constraint()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Constraint(name="test_constraint")

        # Setting name and id should not throw an exception and should load
        # data correctly.
        Constraint(name="test_constraint", id="#test_constraint")

        # Setting initially without deferrable should throw an exception.
        with self.assertRaises(ValidationError):
            Constraint(name="test_constraint", id="#test_constraint", deferrable=False, initially="IMMEDIATE")

        # Seting a bad value for initially should throw an exception.
        with self.assertRaises(ValidationError):
            Constraint(name="test_constraint", id="#test_constraint", deferrable=True, initially="BAD_VALUE")

        # Setting a valid value for initially should not throw an exception.
        Constraint(name="test_constraint", id="#test_constraint", deferrable=True, initially="IMMEDIATE")
        Constraint(name="test_constraint", id="#test_constraint", deferrable=True, initially="DEFERRED")

    def test_unique_constraint(self) -> None:
        """Test validation of unique constraints."""
        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            UniqueConstraint(name="test_constraint", id="#test_constraint")

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        constraint = UniqueConstraint(name="uniq_test", id="#uniq_test", columns=["test_column"])
        self.assertEqual(constraint.name, "uniq_test", "name should be 'uniq_test'")
        self.assertEqual(constraint.id, "#uniq_test", "id should be '#uniq_test'")
        self.assertEqual(constraint.columns, ["test_column"], "columns should be ['test_column']")

        # Creating from data dictionary should work and load data correctly.
        data = {"name": "uniq_test", "id": "#uniq_test", "columns": ["test_column"]}
        constraint = UniqueConstraint(**data)
        self.assertEqual(constraint.name, "uniq_test", "name should be 'uniq_test'")
        self.assertEqual(constraint.id, "#uniq_test", "id should be '#uniq_test'")
        self.assertEqual(constraint.columns, ["test_column"], "columns should be ['test_column']")

    def test_foreign_key_constraint(self) -> None:
        """Test validation of foreign key constraints."""
        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            ForeignKeyConstraint(name="fk_test", id="#fk_test")

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        constraint = ForeignKeyConstraint(
            name="fk_test", id="#fk_test", columns=["test_column"], referenced_columns=["test_column"]
        )
        self.assertEqual(constraint.name, "fk_test", "name should be 'fk_test'")
        self.assertEqual(constraint.id, "#fk_test", "id should be '#fk_test'")
        self.assertEqual(constraint.columns, ["test_column"], "columns should be ['test_column']")
        self.assertEqual(
            constraint.referenced_columns, ["test_column"], "referenced_columns should be ['test_column']"
        )

        # Creating from data dictionary should work and load data correctly.
        data = {
            "name": "fk_test",
            "id": "#fk_test",
            "columns": ["test_column"],
            "referenced_columns": ["test_column"],
        }
        constraint = ForeignKeyConstraint(**data)
        self.assertEqual(constraint.name, "fk_test", "name should be 'fk_test'")
        self.assertEqual(constraint.id, "#fk_test", "id should be '#fk_test'")
        self.assertEqual(constraint.columns, ["test_column"], "columns should be ['test_column']")
        self.assertEqual(
            constraint.referenced_columns, ["test_column"], "referenced_columns should be ['test_column']"
        )

    def test_check_constraint(self) -> None:
        """Test validation of check constraints."""
        # Setting name and id should throw an exception from missing
        # expression.
        with self.assertRaises(ValidationError):
            CheckConstraint(name="check_test", id="#check_test")

        # Setting name, id, and expression should not throw an exception and
        # should load data correctly.
        constraint = CheckConstraint(name="check_test", id="#check_test", expression="1+2")
        self.assertEqual(constraint.name, "check_test", "name should be 'check_test'")
        self.assertEqual(constraint.id, "#check_test", "id should be '#check_test'")
        self.assertEqual(constraint.expression, "1+2", "expression should be '1+2'")

        # Creating from data dictionary should work and load data correctly.
        data = {
            "name": "check_test",
            "id": "#check_test",
            "expression": "1+2",
        }
        constraint = CheckConstraint(**data)
        self.assertEqual(constraint.name, "check_test", "name should be 'check_test'")
        self.assertEqual(constraint.id, "#check_test", "id should be '#test_id'")
        self.assertEqual(constraint.expression, "1+2", "expression should be '1+2'")

    def test_bad_constraint_type(self) -> None:
        with self.assertRaises(ValidationError):
            UniqueConstraint(name="uniq_test", id="#uniq_test", columns=["test_column"], type="BAD_TYPE")


class IndexTestCase(unittest.TestCase):
    """Test Pydantic validation of the ``Index`` class."""

    def test_index_validation(self) -> None:
        """Test validation of indexes."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Index()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Index(name="idx_test")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            Index(name="idx_test", id="#idx_test")

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        idx = Index(name="idx_test", id="#idx_test", columns=["#test_column"])
        self.assertEqual(idx.name, "idx_test", "name should be 'test_constraint'")
        self.assertEqual(idx.id, "#idx_test", "id should be '#test_id'")
        self.assertEqual(idx.columns, ["#test_column"], "columns should be ['test_column']")

        # Creating from data dictionary should work and load data correctly.
        data = {"name": "idx_test", "id": "#idx_test", "columns": ["test_column"]}
        idx = Index(**data)
        self.assertEqual(idx.name, "idx_test", "name should be 'idx_test'")
        self.assertEqual(idx.id, "#idx_test", "id should be '#idx_test'")
        self.assertEqual(idx.columns, ["test_column"], "columns should be ['test_column']")

        # Setting both columns and expressions on an index should throw an
        # exception.
        with self.assertRaises(ValidationError):
            Index(name="idx_test", id="#idx_test", columns=["test_column"], expressions=["1+2"])


class SchemaTestCase(unittest.TestCase):
    """Test Pydantic validation of the ``Schema`` class."""

    def test_validation(self) -> None:
        """Test Pydantic validation of the main schema class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Schema()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Schema(name="testSchema")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            Schema(name="testSchema", id="#test_id")

        test_col = Column(name="testColumn", id="#test_col_id", datatype="string", length=256)
        test_tbl = Table(name="testTable", id="#test_tbl_id", columns=[test_col])

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        sch = Schema(name="testSchema", id="#test_sch_id", tables=[test_tbl])
        self.assertEqual(sch.name, "testSchema", "name should be 'testSchema'")
        self.assertEqual(sch.id, "#test_sch_id", "id should be '#test_sch_id'")
        self.assertEqual(sch.tables, [test_tbl], "tables should be ['testTable']")

        # Creating a schema with duplicate table names should raise an
        # exception.
        with self.assertRaises(ValidationError):
            Schema(name="testSchema", id="#test_id", tables=[test_tbl, test_tbl])

        # Using an undefined YAML field should raise an exception.
        with self.assertRaises(ValidationError):
            Schema(**{"name": "testSchema", "id": "#test_sch_id", "bad_field": "1234"}, tables=[test_tbl])

        # Creating a schema containing duplicate IDs should raise an error.
        with self.assertRaises(ValidationError):
            Schema(
                name="testSchema",
                id="#test_sch_id",
                tables=[
                    Table(
                        name="testTable",
                        id="#test_tbl_id",
                        columns=[
                            Column(name="testColumn", id="#test_col_id", datatype="string"),
                            Column(name="testColumn2", id="#test_col_id", datatype="string"),
                        ],
                    )
                ],
            )

    def test_schema_object_ids(self) -> None:
        """Test that the ``id_map`` is properly populated."""
        test_col = Column(name="testColumn", id="#test_col_id", datatype="string", length=256)
        test_tbl = Table(name="testTable", id="#test_table_id", columns=[test_col])
        sch = Schema(name="testSchema", id="#test_schema_id", tables=[test_tbl])

        for id in ["#test_col_id", "#test_table_id", "#test_schema_id"]:
            # Test that the schema contains the expected id.
            self.assertTrue(id in sch, f"schema should contain '{id}'")

        # Check that types of returned objects are correct.
        self.assertIsInstance(sch["#test_col_id"], Column, "schema[id] should return a Column")
        self.assertIsInstance(sch["#test_table_id"], Table, "schema[id] should return a Table")
        self.assertIsInstance(sch["#test_schema_id"], Schema, "schema[id] should return a Schema")

        with self.assertRaises(KeyError):
            # Test that an invalid id raises an exception.
            sch["#bad_id"]

    def test_check_unique_constraint_names(self) -> None:
        """Test that constraint names are unique."""
        test_col = Column(name="testColumn", id="#test_col_id", datatype="string", length=256)
        test_tbl = Table(name="testTable", id="#test_table_id", columns=[test_col])
        test_cons = UniqueConstraint(name="testConstraint", id="#test_constraint_id", columns=["testColumn"])
        test_cons2 = UniqueConstraint(
            name="testConstraint", id="#test_constraint2_id", columns=["testColumn"]
        )
        test_tbl.constraints = [test_cons, test_cons2]
        with self.assertRaises(ValidationError):
            Schema(name="testSchema", id="#test_id", tables=[test_tbl])

    def test_check_unique_index_names(self) -> None:
        """Test that index names are unique."""
        test_col = Column(name="test_column1", id="#test_table#test_column1", datatype="int")
        test_col2 = Column(name="test_column2", id="##test_table#test_column2", datatype="string", length=256)
        test_tbl = Table(name="test_table", id="#test_table", columns=[test_col, test_col2])
        test_idx = Index(name="idx_test", id="#idx_test", columns=[test_col.id])
        test_idx2 = Index(name="idx_test", id="#idx_test2", columns=[test_col2.id])
        test_tbl.indexes = [test_idx, test_idx2]
        with self.assertRaises(ValidationError):
            Schema(name="test_schema", id="#test-schema", tables=[test_tbl])

    def test_model_validate(self) -> None:
        """Load a YAML test file and validate the schema data model."""
        with open(TEST_YAML) as test_yaml:
            data = yaml.safe_load(test_yaml)
            Schema.model_validate(data)

    def test_id_generation(self) -> None:
        """Test ID generation."""
        test_path = os.path.join(TESTDIR, "data", "test_id_generation.yaml")
        with open(test_path) as test_yaml:
            yaml_data = yaml.safe_load(test_yaml)
            # Generate IDs for objects in the test schema.
            Schema.model_validate(yaml_data, context={"id_generation": True})
        with open(test_path) as test_yaml:
            yaml_data = yaml.safe_load(test_yaml)
            # Test that an error is raised when id generation is disabled.
            with self.assertRaises(ValidationError):
                Schema.model_validate(yaml_data, context={"id_generation": False})

    def test_get_table_by_column(self) -> None:
        """Test the ``get_table_by_column`` method."""
        # Test that the correct table is returned when searching by column.
        test_col = Column(name="test_column", id="#test_tbl.test_col", datatype="string", length=256)
        test_tbl = Table(name="test_table", id="#test_tbl", columns=[test_col])
        sch = Schema(name="testSchema", id="#test_sch_id", tables=[test_tbl])
        self.assertEqual(sch.get_table_by_column(test_col), test_tbl)

        # Test that an error is raised when the column is not found.
        bad_col = Column(name="bad_column", id="#test_tbl.bad_column", datatype="string", length=256)
        with self.assertRaises(ValueError):
            sch.get_table_by_column(bad_col)

    def test_find_object_by_id(self) -> None:
        test_col = Column(name="test_column", id="#test_tbl.test_col", datatype="string", length=256)
        test_tbl = Table(name="test_table", id="#test_tbl", columns=[test_col])
        sch = Schema(name="testSchema", id="#test_sch_id", tables=[test_tbl])
        self.assertEqual(sch.find_object_by_id("#test_tbl.test_col", Column), test_col)
        with self.assertRaises(KeyError):
            sch.find_object_by_id("#bad_id", Column)
        with self.assertRaises(TypeError):
            sch.find_object_by_id("#test_tbl", Column)

    def test_from_file(self) -> None:
        """Test loading a schema from a file."""
        # Test file object.
        with open_test_file("sales.yaml") as test_file:
            schema = Schema.from_stream(test_file)
            self.assertIsInstance(schema, Schema)

        # Test path string.
        test_file_str = get_test_file_path("sales.yaml")
        schema = Schema.from_stream(open(test_file_str))
        self.assertIsInstance(schema, Schema)

        # Path object.
        test_file_path = pathlib.Path(test_file_str)
        schema = Schema.from_uri(test_file_path)
        self.assertIsInstance(schema, Schema)

    def test_from_resource(self) -> None:
        """Test loading a schema from a resource."""
        # Test loading a schema from a resource string.
        schema = Schema.from_uri(
            "resource://felis/schemas/tap_schema_std.yaml", context={"id_generation": True}
        )
        self.assertIsInstance(schema, Schema)

        # Test loading a schema from a ResourcePath.
        schema = Schema.from_uri(
            ResourcePath("resource://felis/schemas/tap_schema_std.yaml"), context={"id_generation": True}
        )
        self.assertIsInstance(schema, Schema)

        # Test loading from a nonexistant resource.
        with self.assertRaises(ValueError):
            Schema.from_uri("resource://fake/schemas/bad_schema.yaml")

        # Without ID generation enabled, this schema should fail validation.
        with self.assertRaises(ValidationError):
            Schema.from_uri("resource://felis/schemas/tap_schema_std.yaml")


class SchemaVersionTest(unittest.TestCase):
    """Test the schema version."""

    def test_validation(self) -> None:
        """Test validation of the schema version class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            SchemaVersion()

        # Setting current should not throw an exception and should load data
        # correctly.
        sv = SchemaVersion(current="1.0.0")
        self.assertEqual(sv.current, "1.0.0", "current should be '1.0.0'")

        # Check that schema version can be specified as a single string or
        # an object.
        data = {
            "name": "schema",
            "@id": "#schema",
            "tables": [],
            "version": "1.2.3",
        }
        schema = Schema.model_validate(data)
        self.assertEqual(schema.version, "1.2.3")

        data = {
            "name": "schema",
            "@id": "#schema",
            "tables": [],
            "version": {
                "current": "1.2.3",
                "compatible": ["1.2.0", "1.2.1", "1.2.2"],
                "read_compatible": ["1.1.0", "1.1.1"],
            },
        }
        schema = Schema.model_validate(data)
        self.assertEqual(schema.version.current, "1.2.3")
        self.assertEqual(schema.version.compatible, ["1.2.0", "1.2.1", "1.2.2"])
        self.assertEqual(schema.version.read_compatible, ["1.1.0", "1.1.1"])


class ValidationFlagsTest(unittest.TestCase):
    """Test optional validation flags on the schema."""

    def test_check_tap_table_indexes(self) -> None:
        """Test the ``check_tap_table_indexes`` validation flag."""
        cxt = {"check_tap_table_indexes": True}
        schema_dict = {
            "name": "testSchema",
            "id": "#test_schema_id",
            "tables": [
                {
                    "name": "test_table",
                    "id": "#test_table_id",
                    "columns": [{"name": "test_col", "id": "#test_col", "datatype": "int"}],
                }
            ],
        }

        # Creating a schema without a TAP table index should throw.
        with self.assertRaises(ValidationError):
            Schema.model_validate(schema_dict, context=cxt)

        # Creating a schema with a TAP table index should not throw.
        schema_dict["tables"][0]["tap_table_index"] = 1
        Schema.model_validate(schema_dict, context=cxt)
        schema_dict["tables"].append(
            {
                "name": "test_table2",
                "id": "#test_table2",
                "tap_table_index": 1,
                "columns": [{"name": "test_col2", "id": "#test_col2", "datatype": "int"}],
            }
        )

        # Creating a schema with a duplicate TAP table index should throw.
        with self.assertRaises(ValidationError):
            Schema.model_validate(schema_dict, context=cxt)

        # Multiple, unique TAP table indexes should not throw.
        schema_dict["tables"][1]["tap_table_index"] = 2
        Schema.model_validate(schema_dict, context=cxt)

    def test_check_tap_principal(self) -> None:
        """Test the ``check_tap_principal` validation flag."""
        cxt = {"check_tap_principal": True}
        schema_dict = {
            "name": "testSchema",
            "id": "#test_schema_id",
            "tables": [
                {
                    "name": "test_table",
                    "id": "#test_table_id",
                    "columns": [{"name": "test_col", "id": "#test_col", "datatype": "int"}],
                }
            ],
        }

        # Creating a table without a TAP table principal column should throw.
        with self.assertRaises(ValidationError):
            Schema.model_validate(schema_dict, context=cxt)

        # Creating a table with a TAP table principal column should not throw.
        schema_dict["tables"][0]["columns"][0]["tap_principal"] = 1
        Schema.model_validate(schema_dict, context=cxt)

    def test_check_description(self) -> None:
        """Test the ``check_description`` flag."""
        cxt = {"check_description": True}
        schema_dict = {
            "name": "testSchema",
            "id": "#test_schema_id",
            "tables": [
                {
                    "name": "test_table",
                    "id": "#test_table_id",
                    "columns": [{"name": "test_col", "id": "#test_col", "datatype": "int"}],
                }
            ],
        }

        # Creating a schema without object descriptions should throw.
        with self.assertRaises(ValidationError):
            Schema.model_validate(schema_dict, context=cxt)

        # Creating a schema with object descriptions should not throw.
        schema_dict["description"] = "Test schema"
        schema_dict["tables"][0]["description"] = "Test table"
        schema_dict["tables"][0]["columns"][0]["description"] = "Test column"
        Schema.model_validate(schema_dict, context=cxt)


class RedundantDatatypesTest(unittest.TestCase):
    """Test validation of redundant datatype definitions."""

    def test_mysql_datatypes(self) -> None:
        class ColumnGenerator:
            """Generate column data for redundant datatype testing."""

            def __init__(self, name, id, db_name):
                self.name = name
                self.id = id
                self.db_name = db_name
                self.context = {"check_redundant_datatypes": True}

            def col(self, datatype: str, db_datatype: str, length=None):
                return Column.model_validate(
                    {
                        "name": self.name,
                        "@id": self.id,
                        "datatype": datatype,
                        f"{self.db_name}:datatype": db_datatype,
                        "length": length,
                    },
                    context=self.context,
                )

        """Test that redundant datatype definitions raise an error."""
        coldata = ColumnGenerator("test_col", "#test_col_id", "mysql")

        with self.assertRaises(ValidationError):
            coldata.col("double", "DOUBLE")

        with self.assertRaises(ValidationError):
            coldata.col("int", "INTEGER")

        with self.assertRaises(ValidationError):
            coldata.col("float", "FLOAT")

        with self.assertRaises(ValidationError):
            coldata.col("char", "CHAR", length=8)

        with self.assertRaises(ValidationError):
            coldata.col("string", "VARCHAR", length=32)

        with self.assertRaises(ValidationError):
            coldata.col("byte", "TINYINT")

        with self.assertRaises(ValidationError):
            coldata.col("short", "SMALLINT")

        with self.assertRaises(ValidationError):
            coldata.col("long", "BIGINT")

        with self.assertRaises(ValidationError):
            coldata.col("boolean", "BOOLEAN")

        with self.assertRaises(ValidationError):
            coldata.col("unicode", "NVARCHAR", length=32)

        with self.assertRaises(ValidationError):
            coldata.col("timestamp", "DATETIME")

        # DM-42257: Felis does not handle unbounded text types properly.
        # coldata.col("text", "TEXT", length=32)

        with self.assertRaises(ValidationError):
            coldata.col("binary", "LONGBLOB", length=1024)

        with self.assertRaises(ValidationError):
            # Same type and length
            coldata.col("string", "VARCHAR(128)", length=128)

        # Check the old type mapping for MySQL, which is now okay
        coldata.col("boolean", "BIT(1)")

        # Different types, which is okay
        coldata.col("double", "FLOAT")

        # Same base type with different lengths, which is okay
        coldata.col("string", "VARCHAR(128)", length=32)

        # Different string types, which is okay
        coldata.col("string", "CHAR", length=32)
        coldata.col("unicode", "CHAR", length=32)

    def test_precision(self) -> None:
        """Test that precision is not allowed for datatypes other than
        timestamp.
        """
        with self.assertRaises(ValidationError):
            Column(**{"name": "testColumn", "@id": "#test_col_id", "datatype": "double", "precision": 6})


if __name__ == "__main__":
    unittest.main()
