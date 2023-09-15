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

import contextlib
import copy
import os
import unittest
from collections.abc import Iterator, MutableMapping
from typing import Any

import yaml

from felis import DEFAULT_FRAME, CheckingVisitor

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


@contextlib.contextmanager
def remove_key(mapping: MutableMapping[str, Any], key: str) -> Iterator[MutableMapping[str, Any]]:
    """Remove the key from the dictionary."""
    value = mapping.pop(key)
    yield mapping
    mapping[key] = value


@contextlib.contextmanager
def replace_key(
    mapping: MutableMapping[str, Any], key: str, value: Any
) -> Iterator[MutableMapping[str, Any]]:
    """Replace key value in the dictionary."""
    if key in mapping:
        value, mapping[key] = mapping[key], value
        yield mapping
        value, mapping[key] = mapping[key], value
    else:
        mapping[key] = value
        yield mapping
        del mapping[key]


class VisitorTestCase(unittest.TestCase):
    """Tests for CheckingVisitor class."""

    schema_obj: MutableMapping[str, Any] = {}

    def setUp(self) -> None:
        """Load data from test file."""
        with open(TEST_YAML) as test_yaml:
            self.schema_obj = yaml.load(test_yaml, Loader=yaml.SafeLoader)
            self.schema_obj.update(DEFAULT_FRAME)

    def test_check(self) -> None:
        """Check YAML consistency using CheckingVisitor visitor."""
        visitor = CheckingVisitor()
        visitor.visit_schema(self.schema_obj)

    def test_error_schema(self) -> None:
        """Check for errors at schema level."""
        schema = copy.deepcopy(self.schema_obj)

        # Missing @id
        with remove_key(schema, "@id"):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                CheckingVisitor().visit_schema(schema)

        # Delete tables.
        with remove_key(schema, "tables"):
            with self.assertRaisesRegex(KeyError, "'tables'"):
                CheckingVisitor().visit_schema(schema)

    def test_error_table(self) -> None:
        """Check for errors at table level."""
        schema = copy.deepcopy(self.schema_obj)
        table = schema["tables"][0]

        # Missing @id
        with remove_key(table, "@id"):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                CheckingVisitor().visit_schema(schema)

        # Missing name.
        with remove_key(table, "name"):
            with self.assertRaisesRegex(ValueError, "No name for table object"):
                CheckingVisitor().visit_schema(schema)

        # Missing columns.
        with remove_key(table, "columns"):
            with self.assertRaisesRegex(KeyError, "'columns'"):
                CheckingVisitor().visit_schema(schema)

        # Duplicate table @id causes warning.
        table2 = schema["tables"][1]
        with replace_key(table, "@id", "#duplicateID"), replace_key(table2, "@id", "#duplicateID"):
            with self.assertLogs(logger="felis", level="WARNING") as cm:
                CheckingVisitor().visit_schema(schema)
        self.assertEqual(cm.output, ["WARNING:felis:Duplication of @id #duplicateID"])

    def test_error_column(self) -> None:
        """Check for errors at column level."""
        schema = copy.deepcopy(self.schema_obj)
        column = schema["tables"][0]["columns"][0]

        # Missing @id
        with remove_key(column, "@id"):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                CheckingVisitor().visit_schema(schema)

        # Missing name.
        with remove_key(column, "name"):
            with self.assertRaisesRegex(ValueError, "No name for table object"):
                CheckingVisitor().visit_schema(schema)

        # Missing datatype.
        with remove_key(column, "datatype"):
            with self.assertRaisesRegex(ValueError, "No datatype defined"):
                CheckingVisitor().visit_schema(schema)

        # Incorrect datatype.
        with replace_key(column, "datatype", "nibble"):
            with self.assertRaisesRegex(ValueError, "Incorrect Type Name"):
                CheckingVisitor().visit_schema(schema)

        # Duplicate @id causes warning.
        table2 = schema["tables"][1]
        with replace_key(column, "@id", "#duplicateID"), replace_key(table2, "@id", "#duplicateID"):
            with self.assertLogs(logger="felis", level="WARNING") as cm:
                CheckingVisitor().visit_schema(schema)
        self.assertEqual(cm.output, ["WARNING:felis:Duplication of @id #duplicateID"])

    def test_error_index(self) -> None:
        """Check for errors at index level."""
        schema = copy.deepcopy(self.schema_obj)
        table = schema["tables"][0]

        # Missing @id
        index = {"name": "IDX_index", "columns": [table["columns"][0]["@id"]]}
        with replace_key(table, "indexes", [index]):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                CheckingVisitor().visit_schema(schema)

        # Missing name.
        index = {
            "@id": "#IDX_index",
            "columns": [table["columns"][0]["@id"]],
        }
        with replace_key(table, "indexes", [index]):
            with self.assertRaisesRegex(ValueError, "No name for table object"):
                CheckingVisitor().visit_schema(schema)

        # Both columns and expressions specified.
        index = {
            "@id": "#IDX_index",
            "name": "IDX_index",
            "columns": [table["columns"][0]["@id"]],
            "expressions": ["1+2"],
        }
        with replace_key(table, "indexes", [index]):
            with self.assertRaisesRegex(ValueError, "Defining columns and expressions is not valid"):
                CheckingVisitor().visit_schema(schema)

        # Duplicate @id causes warning.
        index = {
            "@id": "#duplicateID",
            "name": "IDX_index",
            "columns": [table["columns"][0]["@id"]],
        }
        table2 = schema["tables"][1]
        with replace_key(table, "indexes", [index]), replace_key(table2, "@id", "#duplicateID"):
            with self.assertLogs(logger="felis", level="WARNING") as cm:
                CheckingVisitor().visit_schema(schema)
        self.assertEqual(cm.output, ["WARNING:felis:Duplication of @id #duplicateID"])

    def test_version_errors(self) -> None:
        """Test errors in version specification."""
        schema_obj: dict[str, Any] = {
            "name": "schema",
            "@id": "#schema",
            "tables": [],
        }

        schema_obj["version"] = 1
        with self.assertRaisesRegex(TypeError, "version description is not a string or object"):
            CheckingVisitor().visit_schema(schema_obj)

        schema_obj["version"] = {}
        with self.assertRaisesRegex(ValueError, "missing 'current' key in schema version"):
            CheckingVisitor().visit_schema(schema_obj)

        schema_obj["version"] = {"current": 1}
        with self.assertRaisesRegex(TypeError, "schema version 'current' value is not a string"):
            CheckingVisitor().visit_schema(schema_obj)

        schema_obj["version"] = {"current": "v1", "extra": "v2"}
        with self.assertLogs("felis", "ERROR") as cm:
            CheckingVisitor().visit_schema(schema_obj)
        self.assertEqual(cm.output, ["ERROR:felis:unexpected keys in schema version description: ['extra']"])

        schema_obj["version"] = {"current": "v1", "compatible": "v2"}
        with self.assertRaisesRegex(TypeError, "schema version 'compatible' value is not a list"):
            CheckingVisitor().visit_schema(schema_obj)

        schema_obj["version"] = {"current": "v1", "compatible": ["1", "2", 3]}
        with self.assertRaisesRegex(TypeError, "items in 'compatible' value are not strings"):
            CheckingVisitor().visit_schema(schema_obj)

        schema_obj["version"] = {"current": "v1", "read_compatible": "v2"}
        with self.assertRaisesRegex(TypeError, "schema version 'read_compatible' value is not a list"):
            CheckingVisitor().visit_schema(schema_obj)

        schema_obj["version"] = {"current": "v1", "read_compatible": ["1", "2", 3]}
        with self.assertRaisesRegex(TypeError, "items in 'read_compatible' value are not strings"):
            CheckingVisitor().visit_schema(schema_obj)


if __name__ == "__main__":
    unittest.main()
