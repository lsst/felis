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

import unittest

from pydantic import ValidationError

from felis.datamodel import Schema
from felis.validation import RspColumn, RspSchema, RspTable, get_schema


class RSPSchemaTestCase(unittest.TestCase):
    """Test validation of RSP schema data."""

    def test_rsp_validation(self) -> None:
        # Creating an empty RSP column should throw an exception.
        with self.assertRaises(ValidationError):
            RspColumn()

        # Missing column description should throw an exception.
        with self.assertRaises(ValidationError):
            RspColumn(name="testColumn", id="#test_col_id", datatype="string")

        # A column description with only whitespace should throw an exception.
        with self.assertRaises(ValidationError):
            RspColumn(name="testColumn", id="#test_col_id", datatype="string", description="  ")

        # A column description of `None` should throw an exception.
        with self.assertRaises(ValidationError):
            RspColumn(name="testColumn", id="#test_col_id", datatype="string", description=None)

        # A column description which is not long enough should throw.
        with self.assertRaises(ValidationError):
            RspColumn(name="testColumn", id="#test_col_id", datatype="string", description="xy")

        # Creating a valid RSP column should not throw an exception.
        col = RspColumn(
            **{
                "name": "testColumn",
                "@id": "#test_col_id",
                "datatype": "string",
                "description": "test column",
                "tap:principal": 1,
            }
        )

        # Creating an empty RSP table should throw an exception.
        with self.assertRaises(ValidationError):
            RspTable()

        # Missing table description should throw an exception.
        with self.assertRaises(ValidationError):
            RspTable(**{"name": "testTable", "@id": "#test_table_id", "tap:table_index": 1}, columns=[col])

        # A table description with only whitespace should throw an exception.
        with self.assertRaises(ValidationError):
            RspTable(
                **{"name": "testTable", "@id": "#test_table_id", "tap:table_index": 1, "description": "  "},
                columns=[col],
            )

        # A table description of `None` should throw an exception.
        with self.assertRaises(ValidationError):
            RspTable(
                **{"name": "testTable", "@id": "#test_table_id", "tap:table_index": 1, "description": None},
                columns=[col],
            )

        # Missing TAP table index should throw an exception.
        with self.assertRaises(ValidationError):
            RspTable(name="testTable", id="#test_table_id", description="test table", columns=[col])

        # Missing at least one column flagged as TAP principal should throw an
        # exception.
        with self.assertRaises(ValidationError):
            RspTable(
                **{
                    "name": "testTable",
                    "@id": "#test_table_id",
                    "description": "test table",
                    "tap:table_index": 1,
                    "columns": [
                        RspColumn(
                            **{
                                "name": "testColumn",
                                "@id": "#test_col_id",
                                "datatype": "string",
                                "description": "test column",
                            }
                        )
                    ],
                }
            )

        # Creating a valid RSP table should not throw an exception.
        tbl = RspTable(
            **{
                "name": "testTable",
                "@id": "#test_table_id",
                "description": "test table",
                "tap:table_index": 1,
                "columns": [col],
            }
        )

        # Creating an empty RSP table schema throw an exception.
        with self.assertRaises(ValidationError):
            RspSchema(tables=[tbl])

        # Creating a schema with duplicate TAP table indices should throw an
        # exception.
        with self.assertRaises(ValidationError):
            RspSchema(
                **{"name": "testSchema", "@id": "#test_schema_id", "description": "test schema"},
                tables=[
                    RspTable(
                        **{
                            "name": "testTable1",
                            "@id": "#test_table1_id",
                            "description": "test table",
                            "tap:table_index": 1,
                            "columns": [
                                RspColumn(
                                    **{
                                        "name": "testColumn",
                                        "@id": "#test_col1_id",
                                        "datatype": "string",
                                        "description": "test column",
                                        "tap:principal": 1,
                                    }
                                )
                            ],
                        }
                    ),
                    RspTable(
                        **{
                            "name": "testTable2",
                            "@id": "#test_table2_id",
                            "description": "test table",
                            "tap:table_index": 1,
                            "columns": [
                                RspColumn(
                                    **{
                                        "name": "testColumn",
                                        "@id": "#test_col2_id",
                                        "datatype": "string",
                                        "description": "test column",
                                        "tap:principal": 1,
                                    }
                                )
                            ],
                        }
                    ),
                ],
            )

        # Creating a valid schema with multiple tables having unique TAP table
        # indices should not throw a exception.
        RspSchema(
            **{"name": "testSchema", "@id": "#test_schema_id", "description": "test schema"},
            tables=[
                RspTable(
                    **{
                        "name": "testTable",
                        "@id": "#test_table_id",
                        "description": "test table",
                        "tap:table_index": 1,
                        "columns": [
                            RspColumn(
                                **{
                                    "name": "testColumn",
                                    "@id": "#test_col1_id",
                                    "datatype": "string",
                                    "description": "test column",
                                    "tap:principal": 1,
                                }
                            )
                        ],
                    }
                ),
                RspTable(
                    **{
                        "name": "testTable2",
                        "@id": "#test_table2_id",
                        "description": "test table",
                        "tap:table_index": 2,
                        "columns": [
                            RspColumn(
                                **{
                                    "name": "testColumn",
                                    "@id": "#test_col2_id",
                                    "datatype": "string",
                                    "description": "test column",
                                    "tap:principal": 1,
                                }
                            )
                        ],
                    }
                ),
            ],
        )

    def test_get_schema(self) -> None:
        """Test that get_schema() returns the correct schema types."""
        rsp_schema: RspSchema = get_schema("RSP")
        self.assertEqual(rsp_schema.__name__, "RspSchema")

        default_schema: Schema = get_schema("default")
        self.assertEqual(default_schema.__name__, "Schema")

        with self.assertRaises(ValueError):
            get_schema("invalid_schema")
