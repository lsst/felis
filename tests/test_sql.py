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
from collections.abc import Mapping, MutableMapping
from typing import Any, cast

import sqlalchemy
import yaml

from felis import DEFAULT_FRAME
from felis.db import sqltypes
from felis.sql import SQLVisitor

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


def _get_unique_constraint(table: sqlalchemy.schema.Table) -> sqlalchemy.schema.UniqueConstraint | None:
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
    return {cast(str, index.name): index for index in table.indexes}


class VisitorTestCase(unittest.TestCase):
    """Tests for both CheckingVisitor and SQLVisitor classes."""

    schema_obj: MutableMapping[str, Any] = {}

    def setUp(self) -> None:
        """Load data from test file."""
        with open(TEST_YAML) as test_yaml:
            self.schema_obj = yaml.load(test_yaml, Loader=yaml.SafeLoader)
            self.schema_obj.update(DEFAULT_FRAME)

    def test_make_metadata(self) -> None:
        """Generate sqlalchemy metadata using SQLVisitor class."""
        visitor = SQLVisitor()
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
        for column, ctype in zip(
            table.columns.values(),
            (sqlalchemy.types.SMALLINT, sqlalchemy.types.VARCHAR, sqlalchemy.types.VARCHAR),
        ):
            self.assertIsInstance(column.type, (ctype, sqlalchemy.types.Variant))

        # Details of sdqa_Metric table.
        table = tables["sdqa.sdqa_Metric"]
        self.assertCountEqual(
            table.columns.keys(), ["sdqa_metricId", "metricName", "physicalUnits", "dataType", "definition"]
        )
        self.assertTrue(table.columns["sdqa_metricId"].primary_key)
        self.assertFalse(table.indexes)
        for column, ctype in zip(
            table.columns.values(),
            (
                sqlalchemy.types.SMALLINT,
                sqlalchemy.types.VARCHAR,
                sqlalchemy.types.VARCHAR,
                sqlalchemy.types.CHAR,
                sqlalchemy.types.VARCHAR,
            ),
        ):
            self.assertIsInstance(column.type, (ctype, sqlalchemy.types.Variant))
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
        for column, ctype in zip(
            table.columns.values(),
            (
                sqlalchemy.types.BIGINT,
                sqlalchemy.types.SMALLINT,
                sqlalchemy.types.SMALLINT,
                sqlalchemy.types.BIGINT,
                sqltypes.DOUBLE,
                sqltypes.DOUBLE,
            ),
        ):
            self.assertIsInstance(column.type, (ctype, sqlalchemy.types.Variant))
        unique = _get_unique_constraint(table)
        self.assertIsNotNone(unique)
        assert unique is not None, "Constraint must be defined"
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


if __name__ == "__main__":
    unittest.main()
