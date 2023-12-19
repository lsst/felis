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
import shutil
import tempfile
import unittest
from collections.abc import MutableMapping
from typing import Any

import sqlalchemy
import yaml

from felis import DEFAULT_FRAME
from felis.cli import _normalize
from felis.tap import Tap11Base, TapLoadingVisitor, init_tables

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class VisitorTestCase(unittest.TestCase):
    """Tests for TapLoadingVisitor class."""

    schema_obj: MutableMapping[str, Any] = {}

    def setUp(self) -> None:
        """Load data from test file."""
        with open(TEST_YAML) as test_yaml:
            self.schema_obj = yaml.load(test_yaml, Loader=yaml.SafeLoader)
            self.schema_obj.update(DEFAULT_FRAME)
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_tap(self) -> None:
        """Test for creating tap schema."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"
        engine = sqlalchemy.create_engine(url)
        tap_tables = init_tables()
        Tap11Base.metadata.create_all(engine)

        # This repeats logic from cli.py.
        normalized = _normalize(self.schema_obj, embed="@always")
        if isinstance(normalized["@graph"], dict):
            normalized["@graph"] = [normalized["@graph"]]
        for schema in normalized["@graph"]:
            tap_visitor = TapLoadingVisitor(engine, tap_tables=tap_tables)
            tap_visitor.visit_schema(schema)


if __name__ == "__main__":
    unittest.main()
