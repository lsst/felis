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
from pydantic import ValidationError

from felis.datamodel import (
    CheckConstraint,
    Column,
    DataType,
    ForeignKeyConstraint,
    Index,
    Schema,
    SchemaVersion,
    Table,
    UniqueConstraint,
)

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class DataModelTestCase(unittest.TestCase):
    """Test validation of a test schema from a YAML file."""

    schema_obj: Schema

    def test_validation(self) -> None:
        """Load test file and validate it using the data model."""
        with open(TEST_YAML) as test_yaml:
            data = yaml.safe_load(test_yaml)
            self.schema_obj = Schema.model_validate(data)


class ColumnTestCase(unittest.TestCase):
    """Test the `Column` class."""

    def test_validation(self) -> None:
        """Test validation of the `Column` class."""
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
        col = Column(name="testColumn", id="#test_id", datatype="string")
        self.assertEqual(col.name, "testColumn", "name should be 'testColumn'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.datatype, DataType.string, "datatype should be 'DataType.string'")

        # Creating from data dictionary should work and load data correctly.
        data = {"name": "testColumn", "id": "#test_id", "datatype": "string"}
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

    def test_require_description(self) -> None:
        """Test the require_description flag for the `Column` class."""

        class MockValidationInfo:
            """Mock context object for passing to validation method."""

            def __init__(self):
                self.context = {"require_description": True}

        info = MockValidationInfo()

        def _check_description(col: Column):
            Schema.check_description(col, info)

        # Creating a column without a description should throw.
        with self.assertRaises(ValueError):
            _check_description(
                Column(
                    **{
                        "name": "testColumn",
                        "@id": "#test_col_id",
                        "datatype": "string",
                    }
                )
            )

        # Creating a column with a description of 'None' should throw.
        with self.assertRaises(ValueError):
            _check_description(
                Column(
                    **{
                        "name": "testColumn",
                        "@id": "#test_col_id",
                        "datatype": "string",
                        "description": None,
                    }
                )
            )

        # Creating a column with an empty description should throw.
        with self.assertRaises(ValueError):
            _check_description(
                Column(
                    **{
                        "name": "testColumn",
                        "@id": "#test_col_id",
                        "datatype": "string",
                        "require_description": True,
                        "description": "",
                    }
                )
            )

        # Creating a column with a description that is too short should throw.
        with self.assertRaises(ValidationError):
            _check_description(
                Column(
                    **{
                        "name": "testColumn",
                        "@id": "#test_col_id",
                        "datatype": "string",
                        "require_description": True,
                        "description": "xy",
                    }
                )
            )


class ConstraintTestCase(unittest.TestCase):
    """Test the `UniqueConstraint`, `Index`, `CheckConstraint`, and
    `ForeignKeyConstraint` classes.
    """

    def test_unique_constraint_validation(self) -> None:
        """Test validation of the `UniqueConstraint` class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            UniqueConstraint()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            UniqueConstraint(name="testConstraint")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            UniqueConstraint(name="testConstraint", id="#test_id")

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        col = UniqueConstraint(name="testConstraint", id="#test_id", columns=["testColumn"])
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.columns, ["testColumn"], "columns should be ['testColumn']")

        # Creating from data dictionary should work and load data correctly.
        data = {"name": "testConstraint", "id": "#test_id", "columns": ["testColumn"]}
        col = UniqueConstraint(**data)
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.columns, ["testColumn"], "columns should be ['testColumn']")

    def test_index_validation(self) -> None:
        """Test validation of the `Index` class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Index()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Index(name="testConstraint")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            Index(name="testConstraint", id="#test_id")

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        col = Index(name="testConstraint", id="#test_id", columns=["testColumn"])
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.columns, ["testColumn"], "columns should be ['testColumn']")

        # Creating from data dictionary should work and load data correctly.
        data = {"name": "testConstraint", "id": "#test_id", "columns": ["testColumn"]}
        col = Index(**data)
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.columns, ["testColumn"], "columns should be ['testColumn']")

        # Setting both columns and expressions on an index should throw an
        # exception.
        with self.assertRaises(ValidationError):
            Index(name="testConstraint", id="#test_id", columns=["testColumn"], expressions=["1+2"])

    def test_foreign_key_validation(self) -> None:
        """Test validation of the `ForeignKeyConstraint` class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            ForeignKeyConstraint()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            ForeignKeyConstraint(name="testConstraint")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            ForeignKeyConstraint(name="testConstraint", id="#test_id")

        # Setting name, id, and columns should not throw an exception and
        # should load data correctly.
        col = ForeignKeyConstraint(
            name="testConstraint", id="#test_id", columns=["testColumn"], referenced_columns=["testColumn"]
        )
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.columns, ["testColumn"], "columns should be ['testColumn']")
        self.assertEqual(
            col.referenced_columns, ["testColumn"], "referenced_columns should be ['testColumn']"
        )

        # Creating from data dictionary should work and load data correctly.
        data = {
            "name": "testConstraint",
            "id": "#test_id",
            "columns": ["testColumn"],
            "referenced_columns": ["testColumn"],
        }
        col = ForeignKeyConstraint(**data)
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.columns, ["testColumn"], "columns should be ['testColumn']")
        self.assertEqual(
            col.referenced_columns, ["testColumn"], "referenced_columns should be ['testColumn']"
        )

    def test_check_constraint_validation(self) -> None:
        """Check validation of the `CheckConstraint` class."""
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            CheckConstraint()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            CheckConstraint(name="testConstraint")

        # Setting name and id should throw an exception from missing
        # expression.
        with self.assertRaises(ValidationError):
            CheckConstraint(name="testConstraint", id="#test_id")

        # Setting name, id, and expression should not throw an exception and
        # should load data correctly.
        col = CheckConstraint(name="testConstraint", id="#test_id", expression="1+2")
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.expression, "1+2", "expression should be '1+2'")

        # Creating from data dictionary should work and load data correctly.
        data = {
            "name": "testConstraint",
            "id": "#test_id",
            "expression": "1+2",
        }
        col = CheckConstraint(**data)
        self.assertEqual(col.name, "testConstraint", "name should be 'testConstraint'")
        self.assertEqual(col.id, "#test_id", "id should be '#test_id'")
        self.assertEqual(col.expression, "1+2", "expression should be '1+2'")


class TableTestCase(unittest.TestCase):
    """Test the `Table` class."""

    def test_validation(self) -> None:
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Table()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Table(name="testTable")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            Index(name="testTable", id="#test_id")

        testCol = Column(name="testColumn", id="#test_id", datatype="string")

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


class SchemaTestCase(unittest.TestCase):
    """Test the `Schema` class."""

    def test_validation(self) -> None:
        # Default initialization should throw an exception.
        with self.assertRaises(ValidationError):
            Schema()

        # Setting only name should throw an exception.
        with self.assertRaises(ValidationError):
            Schema(name="testSchema")

        # Setting name and id should throw an exception from missing columns.
        with self.assertRaises(ValidationError):
            Schema(name="testSchema", id="#test_id")

        test_col = Column(name="testColumn", id="#test_col_id", datatype="string")
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
        """Test that the id_map is properly populated."""
        test_col = Column(name="testColumn", id="#test_col_id", datatype="string")
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


class SchemaVersionTest(unittest.TestCase):
    """Test the `SchemaVersion` class."""

    def test_validation(self) -> None:
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


if __name__ == "__main__":
    unittest.main()
