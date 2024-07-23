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

import sqlalchemy
import yaml

from felis.datamodel import Schema
from felis.tap import Tap11Base, TapLoadingVisitor, init_tables

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class VisitorTestCase(unittest.TestCase):
    """Test the TAP loading visitor."""

    schema_obj: Schema

    def setUp(self) -> None:
        """Load data from a test file."""
        with open(TEST_YAML) as test_yaml:
            yaml_data = yaml.load(test_yaml, Loader=yaml.SafeLoader)
            self.schema_obj = Schema.model_validate(yaml_data)
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_tap(self) -> None:
        """Test creation of the TAP_SCHEMA metadata using the visitor class."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"
        engine = sqlalchemy.create_engine(url)
        tap_tables = init_tables()
        Tap11Base.metadata.create_all(engine)

        # This repeats logic from cli.py.
        tap_visitor = TapLoadingVisitor(engine, tap_tables=tap_tables)
        tap_visitor.visit_schema(self.schema_obj)


if __name__ == "__main__":
    unittest.main()
