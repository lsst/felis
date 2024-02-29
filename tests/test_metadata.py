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

from felis.datamodel import Schema
from felis.metadata import SchemaMetaData

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class SchemaMetaDataTestCase(unittest.TestCase):
    """Tests for the `SchemaMetaData` class."""

    def setUp(self) -> None:
        os.makedirs(os.path.join(TESTDIR, ".tests"), exist_ok=True)

    def test_dump(self) -> None:
        """Load test file and validate it using the data model."""
        with open(TEST_YAML) as test_yaml:
            data = yaml.safe_load(test_yaml)
            schema_obj = Schema.model_validate(data)
        md = SchemaMetaData(schema_obj)
        with open(os.path.join(TESTDIR, ".tests", "schema_metadata_dump_test.txt"), "w") as dumpfile:
            md.dump(connection_string="mysql://", file=dumpfile)


if __name__ == "__main__":
    unittest.main()
