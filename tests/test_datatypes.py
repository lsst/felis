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
from felis.metadata import get_datatype_with_variants
from felis.datamodel import DIALECTS


class ColumnGenerator:
    """Generate column data for testing."""

    def __init__(self, name, id, db_name=None, check_redundant_datatypes=True):
        self.name = name
        self.id = id
        self.db_name = db_name
        self.context = {"check_redundant_datatypes": check_redundant_datatypes}

    def col(self, datatype: str, db_datatype: str | None = None, length: int | None = None):
        data = {}
        if db_datatype is not None:
            if self.db_name is None:
                raise ValueError("db_datatype must be None if db_name is None")
            data[f"{self.db_name}:datatype"] = db_datatype
        if length is not None:
            data["length"] = length
        return Column.model_validate(
            {"name": self.name, "@id": self.id, "datatype": datatype, **data},
            context=self.context,
        )


class TimestampDatatypesTest(unittest.TestCase):
    """Test timestamp datatype definitions."""

    def test_mysql_datatypes(self) -> None:
        """Test the conversion of timestamp and datetime columns to SQL."""
        for db_name in [None, "sqlite", "mysql", "postgresql"]:
            colgen = ColumnGenerator("test_timestamp", "#test_timestamp", db_name)
            dialect = DIALECTS[db_name] if db_name else None
            for datatype in ["timestamp", "datetime"]:
                for precision in [None, 6]:
                    for timezone in [True, False]:
                        print(f"{db_name}, {datatype}, precision={precision}, tz={timezone}")
                        col = colgen.col(datatype)
                        col.precision = precision
                        col.timezone = timezone
                        sql_datatype = get_datatype_with_variants(col)
                        sql = sql_datatype.compile(dialect)
                        print(sql)

                        if db_name == "mysql":
                            if datatype == "timestamp":
                                if precision is None:
                                    self.assertEqual(sql, "TIMESTAMP")
                                else:
                                    self.assertEqual(sql, f"TIMESTAMP({precision})")
                            elif datatype == "datetime":
                                if precision is None:
                                    self.assertEqual(sql, "DATETIME")
                                else:
                                    self.assertEqual(sql, f"DATETIME({precision})")
                        elif db_name == "postgresql":
                            if timezone:
                                if precision is None:
                                    self.assertEqual(sql, "TIMESTAMP WITH TIME ZONE")
                                else:
                                    self.assertEqual(sql, f"TIMESTAMP({precision}) WITH TIME ZONE")
                            else:
                                if precision is None:
                                    self.assertEqual(sql, "TIMESTAMP WITHOUT TIME ZONE")
                                else:
                                    self.assertEqual(sql, f"TIMESTAMP({precision}) WITHOUT TIME ZONE")
                        elif db_name in [None, "sqlite"]:
                            if datatype == "timestamp":
                                self.assertEqual(sql, "TIMESTAMP")
                            elif datatype == "datetime":
                                self.assertEqual(sql, "DATETIME")
                        print("")


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

        with self.assertRaises(ValidationError):
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

        # Check the old type mapping for MySQL, which is now okay
        coldata.col("boolean", "BIT(1)")

        # Different types, which is okay
        coldata.col("double", "FLOAT")

        # Same base type with different lengths, which is okay
        coldata.col("string", "VARCHAR(128)", length=32)

        # Different string types, which is okay
        coldata.col("string", "CHAR", length=32)
        coldata.col("unicode", "CHAR", length=32)


if __name__ == "__main__":
    unittest.main()
