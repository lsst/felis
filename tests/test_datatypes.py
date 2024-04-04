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

from felis.datamodel import Column, Schema


def _col_mysql(datatype, mysql_datatype, length=None) -> dict[str, str]:
    return Column(
        **{
            "name": "test_col",
            "@id": "#test_col_id",
            "datatype": datatype,
            "mysql:datatype": mysql_datatype,
            "length": length,
        }
    )


class RedaundantDatatypesTest(unittest.TestCase):
    """Test validation of redundant datatype definitions."""

    def test_mysql_datatypes(self) -> None:
        """Test that redundant datatype definitions raise an error."""
        Schema.ValidationConfig.check_redundant_datatypes = True
        try:
            # Error: same type
            with self.assertRaises(ValidationError):
                _col_mysql("double", "DOUBLE")

            # Error: same type
            with self.assertRaises(ValidationError):
                _col_mysql("int", "INTEGER")

            # Error: same type
            with self.assertRaises(ValidationError):
                _col_mysql("float", "FLOAT")

            # Error: same type
            with self.assertRaises(ValidationError):
                _col_mysql("char", "CHAR", length=8)

            # Error: same type
            with self.assertRaises(ValidationError):
                _col_mysql("string", "VARCHAR", length=32)

            # Error: same type mapping as default
            with self.assertRaises(ValidationError):
                _col_mysql("byte", "TINYINT")

            # Error: same type mapping as default
            with self.assertRaises(ValidationError):
                _col_mysql("short", "SMALLINT")

            # Error: same type mapping as default
            with self.assertRaises(ValidationError):
                _col_mysql("long", "BIGINT")

            # Okay: These look equivalent but default is actually `BIT(1)`.
            _col_mysql("boolean", "BOOLEAN")

            # TODO:
            # - unicode
            # - text
            # - binary
            # - timestamp

            # Fixme: This should raise an error but MySQL type comes back as
            # 'BIT' and not 'BIT(1)'.
            # with self.assertRaises(ValidationError):
            #    _col_mysql("boolean", "BIT(1)")

            # Error: same type and length
            with self.assertRaises(ValidationError):
                _col_mysql("string", "VARCHAR(128)", length=128)

            # Okay: different types
            _col_mysql("double", "FLOAT")

            # Okay: same base types with different lengths
            _col_mysql("string", "VARCHAR(128)", length=32)

        finally:
            Schema.ValidationConfig.check_redundant_datatypes = False


if __name__ == "__main__":
    unittest.main()
