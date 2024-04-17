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

from felis.types.properties import DEFAULT_TYPES
from felis.types.simple import SimpleTypes
from felis.types.sql import MYSQL, SQLTypes


class TypesTestCase(unittest.TestCase):
    """Test case for simple types."""

    def test_simple_types(self) -> None:
        simple_types = SimpleTypes()
        default_length = 64
        for type_name, default_type in DEFAULT_TYPES.items():
            print(f"\nTesting MySQL type: {type_name}")
            if default_type.is_length_required:
                print("  Length required")
                simple_t = simple_types.create(type_name, length=default_length)
            else:
                print("  Length not required")
                simple_t = simple_types.create(type_name)
            self.assertIsNotNone(simple_t.type_string)
            self.assertIsNotNone(simple_t.type_parameters)
            self.assertEqual(len(simple_t.type_parameters), 0)

    def test_sql_types(self) -> None:
        sql_types = SQLTypes()
        default_length = 64
        for type_name, default_type in DEFAULT_TYPES.items():
            print(f"\nTesting SQL type: {type_name}")
            if default_type.is_length_required:
                print("  Length required")
                sql_t = sql_types.create(type_name, length=default_length)
            else:
                print("  Length not required")
                sql_t = sql_types.create(type_name)
            print("  Type string:", sql_t.type_string)
            self.assertIsNotNone(sql_t.type_string)
            print("  Type parameters:", sql_t.type_parameters)
            self.assertIsNotNone(sql_t.type_parameters)
            print("  Type object:", sql_t.type_object)
            self.assertIsNotNone(sql_t.type_object)
            print("  Type object compile:", sql_t.type_object.compile())
            self.assertEqual(sql_t.type_object.compile(), sql_t.type_string)
            print("  Type object compile dialect:", sql_t.type_object.compile(dialect=sql_t.dialect))
            self.assertEqual(len(sql_t.type_parameters), 0)

    def test_mysql_types(self) -> None:
        sql_types = SQLTypes(dialect=MYSQL)
        default_length = 64
        for type_name, default_type in DEFAULT_TYPES.items():
            print(f"\nTesting MySQL type: {type_name}")
            if default_type.is_length_required or type_name in ["string", "unicode"]:
                print("  Length required")
                sql_t = sql_types.create(type_name, length=default_length)
            else:
                print("  Length not required")
                sql_t = sql_types.create(type_name)
            print("  Type string:", sql_t.type_string)
            self.assertIsNotNone(sql_t.type_string)
            print("  Type parameters:", sql_t.type_parameters)
            self.assertIsNotNone(sql_t.type_parameters)
            print("  Type object:", sql_t.type_object)
            self.assertIsNotNone(sql_t.type_object)
            print("  Type object compile dialect:", sql_t.type_object.compile(dialect=sql_t.dialect))
            self.assertEqual(len(sql_t.type_parameters), 0)
