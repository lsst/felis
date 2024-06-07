import unittest

from felis.db.typeparser import PostgresTypeParser
from sqlalchemy.dialects import postgresql as pg


class TestPostgresTypeParser(unittest.TestCase):
    """Test the SQLTypeParser class."""

    def setUp(self):
        self.parser = PostgresTypeParser()

    def _check_type(self, input_str, expected_type):
        print(f"checking types: {input_str}, {expected_type}")
        parsed = self.parser.parse(input_str)
        print("parsed:", parsed)
        print(f"SQL types: {parsed} and {expected_type}")
        self.assertEqual(type(parsed), type(expected_type))
        if hasattr(expected_type, "length"):
            self.assertEqual(parsed.length, expected_type.length)
        if hasattr(expected_type, "precision"):
            self.assertEqual(parsed.precision, expected_type.precision)
        if hasattr(expected_type, "scale"):
            self.assertEqual(parsed.scale, expected_type.scale)
        if hasattr(expected_type, "timezone"):
            self.assertEqual(parsed.timezone, expected_type.timezone)
        compiled = parsed.compile(self.parser.dialect)
        print(f"compiled: {compiled}\n")

    def test_postgresql_types(self):
        self._check_type("BIGINT", pg.BIGINT())
        self._check_type("BIT", pg.BIT())
        self._check_type("BOOLEAN", pg.BOOLEAN())
        self._check_type("BYTEA", pg.BYTEA())
        self._check_type("CHAR", pg.CHAR())
        self._check_type("CHAR(10)", pg.CHAR(10))
        self._check_type("DATE", pg.DATE())
        self._check_type("DOUBLE PRECISION", pg.DOUBLE_PRECISION())
        self._check_type("INTEGER", pg.INTEGER())
        self._check_type("NUMERIC", pg.NUMERIC())
        self._check_type("NUMERIC(6)", pg.NUMERIC(6))
        self._check_type("NUMERIC(10, 2)", pg.NUMERIC(10, 2))
        self._check_type("REAL", pg.REAL())
        self._check_type("SMALLINT", pg.SMALLINT())
        self._check_type("TEXT", pg.TEXT())
        self._check_type("TIME", pg.TIME())
        self._check_type("TIMESTAMP", pg.TIMESTAMP())
        self._check_type("TIMESTAMP(6)", pg.TIMESTAMP(6))
        self._check_type("TIMESTAMP WITH TIME ZONE", pg.TIMESTAMP(True))
        self._check_type("TIMESTAMP(6) WITH TIME ZONE", pg.TIMESTAMP(True, 6))
        self._check_type("TIMESTAMP WITHOUT TIME ZONE", pg.TIMESTAMP(False))
        self._check_type("TIMESTAMP(6) WITHOUT TIME ZONE", pg.TIMESTAMP(False, 6))
        self._check_type("VARCHAR", pg.VARCHAR())
        self._check_type("VARCHAR(255)", pg.VARCHAR(255))


if __name__ == "__main__":
    unittest.main()
