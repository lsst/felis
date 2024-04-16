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

from felis.datamodel import Column


class ColumnGenerator:
    """Generate column data for testing."""

    def __init__(self, name, id, db_name):
        self.name = name
        self.id = id
        self.db_name = db_name
        self.context = {"check_redundant_datatypes": True}

    def col(self, datatype: str, db_datatype: str, length=None):
        return Column.model_validate(
            {
                "name": self.name,
                "@id": self.id,
                "datatype": datatype,
                f"{self.db_name}:datatype": db_datatype,
                "length": length,
            },
            context=self.context,
        )


class RedundantDatatypesTest(unittest.TestCase):
    """Test validation of redundant datatype definitions."""

    def test_mysql_datatypes(self) -> None:
        """Test that redundant datatype definitions raise an error."""
        coldata = ColumnGenerator("test_col", "#test_col_id", "mysql")

        with self.assertRaises(ValidationError):
            coldata.col("double", "DOUBLE")

        with self.assertRaises(ValidationError):
            coldata.col("int", "INTEGER")

        with self.assertRaises(ValidationError):
            coldata.col("boolean", "BIT(1)")

        with self.assertRaises(ValidationError):
            coldata.col("float", "FLOAT")

        with self.assertRaises(ValidationError):
            coldata.col("char", "CHAR", length=8)

        with self.assertRaises(ValidationError):
            coldata.col("string", "VARCHAR", length=32)

        with self.assertRaises(ValidationError):
            coldata.col("byte", "TINYINT")

        with self.assertRaises(ValidationError):
            coldata.col("short", "SMALLINT")

        with self.assertRaises(ValidationError):
            coldata.col("long", "BIGINT")

        # These look like they should be equivalent, but the default is
        # actually ``BIT(1)`` for MySQL.
        coldata.col("boolean", "BOOLEAN")

        with self.assertRaises(ValidationError):
            coldata.col("unicode", "NVARCHAR", length=32)

        with self.assertRaises(ValidationError):
            coldata.col("timestamp", "TIMESTAMP")

        # DM-42257: Felis does not handle unbounded text types properly.
        # coldata.col("text", "TEXT", length=32)

        with self.assertRaises(ValidationError):
            coldata.col("binary", "LONGBLOB", length=1024)

        with self.assertRaises(ValidationError):
            # Same type and length
            coldata.col("string", "VARCHAR(128)", length=128)

        # Different types, which is okay
        coldata.col("double", "FLOAT")

        # Same base type with different lengths, which is okay
        coldata.col("string", "VARCHAR(128)", length=32)

        # Different string types, which is okay
        coldata.col("string", "CHAR", length=32)
        coldata.col("unicode", "CHAR", length=32)


if __name__ == "__main__":
    unittest.main()
