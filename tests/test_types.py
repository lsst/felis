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

from lsst.felis.types.attributes import DEFAULT_TYPES
from lsst.felis.types.simple import SimpleTypes
from lsst.felis.types.sql import SQLTypes


class TypesTestCase(unittest.TestCase):
    """Test case for simple types."""

    def test_simple_types(self):
        simple_types = SimpleTypes()
        type_names = DEFAULT_TYPES.keys()
        for type_name in type_names:
            simple_t = simple_types.create(type_name)
            self.assertIsNotNone(simple_t.type_string)
            self.assertIsNotNone(simple_t.type_parameters)
            self.assertEqual(len(simple_t.type_parameters), 0)

    def test_sql_types(self):
        sql_types = SQLTypes()
        type_names = DEFAULT_TYPES.keys()
        for type_name in type_names:
            sql_t = sql_types.create(type_name)
            self.assertIsNotNone(sql_t.type_string)
            self.assertIsNotNone(sql_t.type_parameters)
            self.assertIsNotNone(sql_t.type_object)
            self.assertEqual(sql_t.type_object.compile(), sql_t.type_string)
            self.assertEqual(len(sql_t.type_parameters), 0)
