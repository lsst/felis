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
from collections.abc import Iterator, Mapping, MutableMapping
from typing import Any, Optional

import sqlalchemy
import yaml

from felis import DEFAULT_FRAME
from felis.model import Visitor, VisitorBase

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


def _get_unique_constraint(table: sqlalchemy.schema.Table) -> Optional[sqlalchemy.schema.UniqueConstraint]:
    """Return a unique constraint for a table, raise if table has more than
    one unique constraint.
    """
    uniques = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, sqlalchemy.schema.UniqueConstraint)
    ]
    if len(uniques) > 1:
        raise TypeError(f"More than one constraint defined for table {table}")
    elif not uniques:
        return None
    else:
        return uniques[0]


def _get_indices(table: sqlalchemy.schema.Table) -> Mapping[str, sqlalchemy.schema.Index]:
    """Return mapping of table indices indexed by index name."""
    return {index.name: index for index in table.indexes}


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
    """Tests for both VisitorBase and Visitor classes."""

    schema_obj: MutableMapping[str, Any] = {}

    def setUp(self) -> None:
        """Load data from test file."""
        with open(TEST_YAML) as test_yaml:
            self.schema_obj = yaml.load(test_yaml, Loader=yaml.SafeLoader)
            self.schema_obj.update(DEFAULT_FRAME)

    def test_check(self) -> None:
        """Check YAML consistency using VisitorBase visitor."""
        visitor = VisitorBase()
        visitor.visit_schema(self.schema_obj)

    def test_make_metadata(self) -> None:
        """Generate sqlalchemy metadata using Visitor class"""
        visitor = Visitor()
        schema = visitor.visit_schema(self.schema_obj)
        self.assertIsNotNone(schema)
        self.assertEqual(schema.name, "sdqa")
        self.assertIsNotNone(schema.tables)
        self.assertIsNotNone(schema.graph_index)
        self.assertIsNotNone(schema.metadata)

        table_names = [
            "sdqa_ImageStatus",
            "sdqa_Metric",
            "sdqa_Rating_ForAmpVisit",
            "sdqa_Rating_CcdVisit",
            "sdqa_Threshold",
        ]

        # Look at metadata tables.
        self.assertIsNone(schema.metadata.schema)
        tables = schema.metadata.tables
        self.assertCountEqual(tables.keys(), [f"sdqa.{table}" for table in table_names])

        # Check schema.tables attribute.
        self.assertCountEqual([table.name for table in schema.tables], table_names)

        # Checks tables in graph index.
        for table_name in table_names:
            self.assertIs(schema.graph_index[f"#{table_name}"], tables[f"sdqa.{table_name}"])

        # Details of sdqa_ImageStatus table.
        table = tables["sdqa.sdqa_ImageStatus"]
        self.assertCountEqual(table.columns.keys(), ["sdqa_imageStatusId", "statusName", "definition"])
        self.assertTrue(table.columns["sdqa_imageStatusId"].primary_key)
        self.assertFalse(table.indexes)
        for column in table.columns.values():
            self.assertIsInstance(column.type, sqlalchemy.types.Variant)

        # Details of sdqa_Metric table.
        table = tables["sdqa.sdqa_Metric"]
        self.assertCountEqual(
            table.columns.keys(), ["sdqa_metricId", "metricName", "physicalUnits", "dataType", "definition"]
        )
        self.assertTrue(table.columns["sdqa_metricId"].primary_key)
        self.assertFalse(table.indexes)
        for column in table.columns.values():
            self.assertIsInstance(column.type, sqlalchemy.types.Variant)
        # It defines a unique constraint.
        unique = _get_unique_constraint(table)
        assert unique is not None, "Constraint must be defined"
        self.assertEqual(unique.name, "UQ_sdqaMetric_metricName")
        self.assertCountEqual(unique.columns, [table.columns["metricName"]])

        # Details of sdqa_Rating_ForAmpVisit table.
        table = tables["sdqa.sdqa_Rating_ForAmpVisit"]
        self.assertCountEqual(
            table.columns.keys(),
            [
                "sdqa_ratingId",
                "sdqa_metricId",
                "sdqa_thresholdId",
                "ampVisitId",
                "metricValue",
                "metricSigma",
            ],
        )
        self.assertTrue(table.columns["sdqa_ratingId"].primary_key)
        for column in table.columns.values():
            self.assertIsInstance(column.type, sqlalchemy.types.Variant)
        unique = _get_unique_constraint(table)
        self.assertIsNotNone(unique)
        self.assertEqual(unique.name, "UQ_sdqaRatingForAmpVisit_metricId_ampVisitId")
        self.assertCountEqual(unique.columns, [table.columns["sdqa_metricId"], table.columns["ampVisitId"]])
        # It has a bunch of indices.
        indices = _get_indices(table)
        self.assertCountEqual(
            indices.keys(),
            [
                "IDX_sdqaRatingForAmpVisit_metricId",
                "IDX_sdqaRatingForAmpVisit_thresholdId",
                "IDX_sdqaRatingForAmpVisit_ampVisitId",
            ],
        )
        self.assertCountEqual(
            indices["IDX_sdqaRatingForAmpVisit_metricId"].columns,
            [schema.graph_index["#sdqa_Rating_ForAmpVisit.sdqa_metricId"]],
        )
        # And a foreign key referencing sdqa_Metric table.
        self.assertEqual(len(table.foreign_key_constraints), 1)
        fk = list(table.foreign_key_constraints)[0]
        self.assertEqual(fk.name, "FK_sdqa_Rating_ForAmpVisit_sdqa_Metric")
        self.assertCountEqual(fk.columns, [table.columns["sdqa_metricId"]])
        self.assertIs(fk.referred_table, tables["sdqa.sdqa_Metric"])

    def test_error_schema(self) -> None:
        """Check for errors at schema level."""

        schema = copy.deepcopy(self.schema_obj)

        # Missing @id
        with remove_key(schema, "@id"):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                VisitorBase().visit_schema(schema)

        # Delete tables.
        with remove_key(schema, "tables"):
            with self.assertRaisesRegex(KeyError, "'tables'"):
                VisitorBase().visit_schema(schema)

    def test_error_table(self) -> None:
        """Check for errors at table level."""

        schema = copy.deepcopy(self.schema_obj)
        table = schema["tables"][0]

        # Missing @id
        with remove_key(table, "@id"):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                VisitorBase().visit_schema(schema)

        # Missing name.
        with remove_key(table, "name"):
            with self.assertRaisesRegex(ValueError, "No name for table object"):
                VisitorBase().visit_schema(schema)

        # Missing columns.
        with remove_key(table, "columns"):
            with self.assertRaisesRegex(KeyError, "'columns'"):
                VisitorBase().visit_schema(schema)

        # Duplicate table @id causes warning.
        table2 = schema["tables"][1]
        with replace_key(table, "@id", "#duplicateID"), replace_key(table2, "@id", "#duplicateID"):
            with self.assertLogs(logger="felis", level="WARNING") as cm:
                VisitorBase().visit_schema(schema)
        self.assertEqual(cm.output, ["WARNING:felis:Duplication of @id #duplicateID"])

    def test_error_column(self) -> None:
        """Check for errors at column level."""

        schema = copy.deepcopy(self.schema_obj)
        column = schema["tables"][0]["columns"][0]

        # Missing @id
        with remove_key(column, "@id"):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                VisitorBase().visit_schema(schema)

        # Missing name.
        with remove_key(column, "name"):
            with self.assertRaisesRegex(ValueError, "No name for table object"):
                VisitorBase().visit_schema(schema)

        # Missing datatype.
        with remove_key(column, "datatype"):
            with self.assertRaisesRegex(ValueError, "No datatype defined"):
                VisitorBase().visit_schema(schema)

        # Incorrect datatype.
        with replace_key(column, "datatype", "nibble"):
            with self.assertRaisesRegex(ValueError, "Incorrect Type Name"):
                VisitorBase().visit_schema(schema)

        # Duplicate @id causes warning.
        table2 = schema["tables"][1]
        with replace_key(column, "@id", "#duplicateID"), replace_key(table2, "@id", "#duplicateID"):
            with self.assertLogs(logger="felis", level="WARNING") as cm:
                VisitorBase().visit_schema(schema)
        self.assertEqual(cm.output, ["WARNING:felis:Duplication of @id #duplicateID"])

    def test_error_index(self) -> None:
        """Check for errors at index level."""

        schema = copy.deepcopy(self.schema_obj)
        table = schema["tables"][0]

        # Missing @id
        index = {"name": "IDX_index", "columns": [table["columns"][0]["@id"]]}
        with replace_key(table, "indexes", [index]):
            with self.assertRaisesRegex(ValueError, "No @id defined for object"):
                VisitorBase().visit_schema(schema)

        # Missing name.
        index = {
            "@id": "#IDX_index",
            "columns": [table["columns"][0]["@id"]],
        }
        with replace_key(table, "indexes", [index]):
            with self.assertRaisesRegex(ValueError, "No name for table object"):
                VisitorBase().visit_schema(schema)

        # Both columns and expressions specified.
        index = {
            "@id": "#IDX_index",
            "name": "IDX_index",
            "columns": [table["columns"][0]["@id"]],
            "expressions": ["1+2"],
        }
        with replace_key(table, "indexes", [index]):
            with self.assertRaisesRegex(ValueError, "Defining columns and expressions is not valid"):
                VisitorBase().visit_schema(schema)

        # Duplicate @id causes warning.
        index = {
            "@id": "#duplicateID",
            "name": "IDX_index",
            "columns": [table["columns"][0]["@id"]],
        }
        table2 = schema["tables"][1]
        with replace_key(table, "indexes", [index]), replace_key(table2, "@id", "#duplicateID"):
            with self.assertLogs(logger="felis", level="WARNING") as cm:
                VisitorBase().visit_schema(schema)
        self.assertEqual(cm.output, ["WARNING:felis:Duplication of @id #duplicateID"])


if __name__ == "__main__":
    unittest.main()
