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

import gc
import os
import unittest

import yaml
from sqlalchemy import text
from sqlalchemy.engine import create_engine

from felis.datamodel import Schema
from felis.tap_schema import DataLoader, TableManager

try:
    from testing.postgresql import Postgresql
except ImportError:
    Postgresql = None

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "sales.yaml")


class TestTapSchemaPostgresql(unittest.TestCase):
    """Test TAP_SCHEMA for PostgreSQL"""

    def setUp(self) -> None:
        """Set up a local PostgreSQL database and a test schema."""
        # Skip the test if the testing.postgresql package is not installed.
        if not Postgresql:
            self.skipTest("testing.postgresql not installed")

        # Start a PostgreSQL database and print the URL and version.
        self.postgresql = Postgresql()
        url = self.postgresql.url()
        print("\n\n---- PostgreSQL URL ----")
        print(url)
        self.engine = create_engine(url)
        with self.engine.connect() as conn:
            print("\n---- PostgreSQL Version ----")
            res = conn.execute(text("SELECT version()")).fetchone()
            if res:
                print(res[0])
            print("\n")

        # Setup a test schema.
        data = yaml.safe_load(open(TEST_YAML))
        self.schema = Schema.model_validate(data)

    def test_create_metadata(self) -> None:
        """Test loading of data into a PostgreSQL TAP_SCHEMA."""
        mgr = TableManager()
        mgr.initialize_database(self.engine)

        loader = DataLoader(self.schema, mgr, self.engine, 1)
        loader.load()

    def tearDown(self) -> None:
        """Tear down the test case."""
        gc.collect()
        self.engine.dispose()
