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
from collections.abc import MutableMapping
from typing import Any

import yaml

from felis import DEFAULT_FRAME
from felis.utils import ReorderingVisitor

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class VisitorTestCase(unittest.TestCase):
    """Tests for ReorderingVisitor class."""

    schema_obj: MutableMapping[str, Any] = {}

    def setUp(self) -> None:
        """Load data from test file."""
        with open(TEST_YAML) as test_yaml:
            self.schema_obj = yaml.load(test_yaml, Loader=yaml.SafeLoader)
            self.schema_obj.update(DEFAULT_FRAME)

    def test_reordering(self) -> None:
        """Check for attribute ordering."""
        visitor = ReorderingVisitor()
        schema = visitor.visit_schema(self.schema_obj)
        self.assertEqual(
            list(schema.keys()), ["@context", "name", "@id", "@type", "description", "tables", "version"]
        )

        table = schema["tables"][0]
        self.assertEqual(list(table.keys())[:5], ["name", "@id", "description", "columns", "primaryKey"])

        column = table["columns"][0]
        self.assertEqual(list(column.keys())[:4], ["name", "@id", "description", "datatype"])

    def test_add_type(self) -> None:
        """Check for attribute ordering with add_type."""
        visitor = ReorderingVisitor(add_type=True)
        schema = visitor.visit_schema(self.schema_obj)
        self.assertEqual(
            list(schema.keys()), ["@context", "name", "@id", "@type", "description", "tables", "version"]
        )

        table = schema["tables"][0]
        self.assertEqual(
            list(table.keys())[:6], ["name", "@id", "@type", "description", "columns", "primaryKey"]
        )

        column = table["columns"][0]
        self.assertEqual(list(column.keys())[:5], ["name", "@id", "@type", "description", "datatype"])


if __name__ == "__main__":
    unittest.main()
