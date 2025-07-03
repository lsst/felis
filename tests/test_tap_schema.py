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
from typing import Any

from sqlalchemy import create_engine, select

from felis.datamodel import Schema
from felis.tap_schema import DataLoader, TableManager

TEST_DIR = os.path.dirname(__file__)
TEST_SALES = os.path.join(TEST_DIR, "data", "sales.yaml")
TEST_TAP_SCHEMA = os.path.join(TEST_DIR, "data", "test_tap_schema.yaml")
TEST_COMPOSITE_KEYS = os.path.join(TEST_DIR, "data", "test_composite_keys.yaml")


class TableManagerTestCase(unittest.TestCase):
    """Test the `TableManager` class."""

    def setUp(self) -> None:
        """Set up the test case."""
        with open(TEST_SALES) as test_file:
            self.schema = Schema.from_stream(test_file)

    def test_create_table_manager(self) -> None:
        """Test the TAP table manager class."""
        mgr = TableManager()

        schema_name = mgr.schema_name

        # Check the created metadata and tables.
        self.assertNotEqual(len(mgr.metadata.tables), 0)
        self.assertEqual(mgr.metadata.schema, schema_name)
        for table_name in mgr.get_table_names_std():
            mgr[table_name]

        # Make sure that creating a new table manager works when one has
        # already been created.
        mgr = TableManager()

    def test_table_name_postfix(self) -> None:
        """Test the table name postfix."""
        mgr = TableManager(apply_schema_to_metadata=False, table_name_postfix="_test")
        for table_name in mgr.metadata.tables:
            self.assertTrue(table_name.endswith("_test"))


class DataLoaderTestCase(unittest.TestCase):
    """Test the `DataLoader` class."""

    def setUp(self) -> None:
        """Set up the test case."""
        with open(TEST_TAP_SCHEMA) as test_file:
            self.schema = Schema.from_stream(test_file, context={"id_generation": True})

        self.tmpdir = tempfile.mkdtemp(dir=TEST_DIR)

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_sqlite(self) -> None:
        """Test the `DataLoader` using an in-memory SQLite database."""
        engine = create_engine("sqlite:///:memory:")

        mgr = TableManager(apply_schema_to_metadata=False)
        mgr.initialize_database(engine)

        loader = DataLoader(self.schema, mgr, engine)
        loader.load()

    def test_sql_output(self) -> None:
        """Test printing SQL to stdout and writing SQL to a file."""
        engine = create_engine("sqlite:///:memory:")
        mgr = TableManager(apply_schema_to_metadata=False)
        loader = DataLoader(self.schema, mgr, engine, dry_run=True, print_sql=True)
        loader.load()

        sql_path = os.path.join(self.tmpdir, "test_tap_schema_print_sql.sql")
        loader = DataLoader(self.schema, mgr, engine, dry_run=True, print_sql=True, output_path=sql_path)
        loader.load()

        self.assertTrue(os.path.exists(sql_path))
        with open(sql_path) as sql_file:
            sql_data = sql_file.read()
            insert_count = sql_data.count("INSERT INTO")
            self.assertEqual(
                insert_count,
                22,
                f"Expected 22 'INSERT INTO' statements, found {insert_count}",
            )

    def test_unique_keys(self) -> None:
        """Test generation of unique foreign keys."""
        engine = create_engine("sqlite:///:memory:")

        mgr = TableManager(apply_schema_to_metadata=False)
        mgr.initialize_database(engine)

        loader = DataLoader(self.schema, mgr, engine, unique_keys=True)
        loader.load()

        keys_data = mgr.select(engine, "keys")
        self.assertGreaterEqual(len(keys_data), 1)
        for row in keys_data:
            self.assertTrue(row["key_id"].startswith(f"{self.schema.name}_"))

        key_columns_data = mgr.select(engine, "key_columns")
        self.assertGreaterEqual(len(key_columns_data), 1)
        for row in key_columns_data:
            self.assertTrue(row["key_id"].startswith(f"{self.schema.name}_"))

    def test_select_with_filter(self) -> None:
        """Test selecting rows with a filter."""
        engine = create_engine("sqlite:///:memory:")
        mgr = TableManager(apply_schema_to_metadata=False)
        mgr.initialize_database(engine)
        loader = DataLoader(self.schema, mgr, engine, unique_keys=True)
        loader.load()

        rows = mgr.select(engine, "columns", "table_name = 'test_schema.table1'")
        self.assertEqual(len(rows), 16)


def _find_row(rows: list[dict[str, Any]], column_name: str, value: str) -> dict[str, Any]:
    next_row = next(
        (row for row in rows if row[column_name] == value),
        None,
    )
    assert next_row is not None
    assert isinstance(next_row, dict)
    return next_row


class TapSchemaSqliteSetup:
    """Set up the TAP_SCHEMA SQLite database for testing.

    Parameters
    ----------
    test_file_path:
        Path to the TAP_SCHEMA test file.

    context
        Context for the schema. Default is an empty dictionary.
    """

    def __init__(self, test_file_path: str, context: dict = {}) -> None:
        with open(test_file_path) as test_file:
            self._schema = Schema.from_stream(test_file, context=context)

        self._engine = create_engine("sqlite:///:memory:")

        mgr = TableManager(apply_schema_to_metadata=False)
        mgr.initialize_database(self._engine)
        self._mgr = mgr

        loader = DataLoader(self._schema, mgr, self._engine, tap_schema_index=2)
        loader.load()

    @property
    def schema(self) -> Schema:
        """Return the schema."""
        return self._schema

    @property
    def engine(self) -> Any:
        """Return the engine."""
        return self._engine

    @property
    def mgr(self) -> TableManager:
        """Return the table manager."""
        return self._mgr


class TapSchemaDataTest(unittest.TestCase):
    """Test the validity of generated TAP SCHEMA data."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.tap_schema_setup = TapSchemaSqliteSetup(TEST_TAP_SCHEMA, context={"id_generation": True})

    def test_schemas(self) -> None:
        schemas_table = self.tap_schema_setup.mgr["schemas"]
        with self.tap_schema_setup.engine.connect() as connection:
            result = connection.execute(select(schemas_table))
            schema_data = [row._asdict() for row in result]

        self.assertEqual(len(schema_data), 1)

        schema = schema_data[0]
        self.assertEqual(schema["schema_name"], "test_schema")
        self.assertEqual(schema["description"], "Test schema")
        self.assertEqual(schema["utype"], "Schema")
        self.assertEqual(schema["schema_index"], 2)

    def test_tables(self) -> None:
        tables_table = self.tap_schema_setup.mgr["tables"]
        with self.tap_schema_setup.engine.connect() as connection:
            result = connection.execute(select(tables_table))
            table_data = [row._asdict() for row in result]

        self.assertEqual(len(table_data), 2)

        table = table_data[0]
        assert isinstance(table, dict)
        self.assertEqual(table["schema_name"], "test_schema")
        self.assertEqual(table["table_name"], f"{self.tap_schema_setup.schema.name}.table1")
        self.assertEqual(table["table_type"], "table")
        self.assertEqual(table["utype"], "Table")
        self.assertEqual(table["description"], "Test table 1")
        self.assertEqual(table["table_index"], 2)

    def test_columns(self) -> None:
        columns_table = self.tap_schema_setup.mgr["columns"]
        with self.tap_schema_setup.engine.connect() as connection:
            result = connection.execute(select(columns_table))
            column_data = [row._asdict() for row in result]

        table1_rows = [
            row for row in column_data if row["table_name"] == f"{self.tap_schema_setup.schema.name}.table1"
        ]
        self.assertNotEqual(len(table1_rows), 0)

        boolean_col = _find_row(table1_rows, "column_name", "boolean_field")
        self.assertEqual(boolean_col["datatype"], "boolean")
        self.assertEqual(boolean_col["arraysize"], None)

        byte_col = _find_row(table1_rows, "column_name", "byte_field")
        self.assertEqual(byte_col["datatype"], "unsignedByte")
        self.assertEqual(byte_col["arraysize"], None)

        short_col = _find_row(table1_rows, "column_name", "short_field")
        self.assertEqual(short_col["datatype"], "short")
        self.assertEqual(short_col["arraysize"], None)

        int_col = _find_row(table1_rows, "column_name", "int_field")
        self.assertEqual(int_col["datatype"], "int")
        self.assertEqual(int_col["arraysize"], None)

        float_col = _find_row(table1_rows, "column_name", "float_field")
        self.assertEqual(float_col["datatype"], "float")
        self.assertEqual(float_col["arraysize"], None)

        double_col = _find_row(table1_rows, "column_name", "double_field")
        self.assertEqual(double_col["datatype"], "double")
        self.assertEqual(double_col["arraysize"], None)

        long_col = _find_row(table1_rows, "column_name", "long_field")
        self.assertEqual(long_col["datatype"], "long")
        self.assertEqual(long_col["arraysize"], None)

        unicode_col = _find_row(table1_rows, "column_name", "unicode_field")
        self.assertEqual(unicode_col["datatype"], "unicodeChar")
        self.assertEqual(unicode_col["arraysize"], "128*")

        binary_col = _find_row(table1_rows, "column_name", "binary_field")
        self.assertEqual(binary_col["datatype"], "unsignedByte")
        self.assertEqual(binary_col["arraysize"], "1024*")

        ts = _find_row(table1_rows, "column_name", "timestamp_field")
        self.assertEqual(ts["datatype"], "char")
        self.assertEqual(ts["xtype"], "timestamp")
        self.assertEqual(ts["description"], "Timestamp field")
        self.assertEqual(ts["utype"], "Obs:Timestamp")
        self.assertEqual(ts["unit"], "s")
        self.assertEqual(ts["ucd"], "time.epoch")
        self.assertEqual(ts["principal"], 1)
        self.assertEqual(ts["std"], 1)
        self.assertEqual(ts["column_index"], 42)
        self.assertEqual(ts["size"], None)
        self.assertEqual(ts["arraysize"], "*")

        char_col = _find_row(table1_rows, "column_name", "char_field")
        self.assertEqual(char_col["datatype"], "char")
        self.assertEqual(char_col["arraysize"], "64")

        str_col = _find_row(table1_rows, "column_name", "string_field")
        self.assertEqual(str_col["datatype"], "char")
        self.assertEqual(str_col["arraysize"], "256*")

        txt_col = _find_row(table1_rows, "column_name", "text_field")
        self.assertEqual(txt_col["datatype"], "char")
        self.assertEqual(txt_col["arraysize"], "*")

    def test_keys(self) -> None:
        keys_table = self.tap_schema_setup.mgr["keys"]
        with self.tap_schema_setup.engine.connect() as connection:
            result = connection.execute(select(keys_table))
            key_data = [row._asdict() for row in result]

        self.assertEqual(len(key_data), 1)

        key = key_data[0]
        assert isinstance(key, dict)

        self.assertEqual(key["key_id"], "fk_table1_to_table2")
        self.assertEqual(key["from_table"], f"{self.tap_schema_setup.schema.name}.table1")
        self.assertEqual(key["target_table"], f"{self.tap_schema_setup.schema.name}.table2")
        self.assertEqual(key["description"], "Foreign key from table1 to table2")
        self.assertEqual(key["utype"], "ForeignKey")

    def test_key_columns(self) -> None:
        key_columns_table = self.tap_schema_setup.mgr["key_columns"]
        with self.tap_schema_setup.engine.connect() as connection:
            result = connection.execute(select(key_columns_table))
            key_column_data = [row._asdict() for row in result]

        self.assertEqual(len(key_column_data), 1)

        key_column = key_column_data[0]
        assert isinstance(key_column, dict)

        self.assertEqual(key_column["key_id"], "fk_table1_to_table2")
        self.assertEqual(key_column["from_column"], "fk")
        self.assertEqual(key_column["target_column"], "id")

    def test_bad_table_name(self) -> None:
        """Test getting a bad TAP_SCHEMA table name."""
        with self.assertRaises(KeyError):
            self.tap_schema_setup.mgr["bad_table"]


class ForceUnboundArraySizeTest(unittest.TestCase):
    """Test that arraysize for appropriate types is set to '*' when the
    ``force_unboundeded_arraysize`` context flag is set to ``True``.
    """

    def setUp(self) -> None:
        """Set up the test case."""
        self.tap_schema_setup = TapSchemaSqliteSetup(
            TEST_TAP_SCHEMA, context={"id_generation": True, "force_unbounded_arraysize": True}
        )

    def test_force_unbounded_arraysize(self) -> None:
        """Test that unbounded arraysize is set to None."""
        columns_table = self.tap_schema_setup.mgr["columns"]
        with self.tap_schema_setup.engine.connect() as connection:
            result = connection.execute(select(columns_table))
            column_data = [row._asdict() for row in result]

        table1_rows = [
            row for row in column_data if row["table_name"] == f"{self.tap_schema_setup.schema.name}.table1"
        ]
        for row in table1_rows:
            if row["column_name"] in ["string_field", "text_field", "unicode_field", "binary_field"]:
                self.assertEqual(row["arraysize"], "*")


class CompositeKeysTestCase(unittest.TestCase):
    """Test the handling of composite foreign keys."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.tap_schema_setup = TapSchemaSqliteSetup(TEST_COMPOSITE_KEYS, context={"id_generation": True})

        # Fetch the keys and key_columns data from the TAP_SCHEMA tables.
        keys_table = self.tap_schema_setup.mgr["keys"]
        key_columns_table = self.tap_schema_setup.mgr["key_columns"]
        with self.tap_schema_setup.engine.connect() as connection:
            key_columns_result = connection.execute(select(key_columns_table))
            self.key_columns_data = [row._asdict() for row in key_columns_result]

            keys_result = connection.execute(select(keys_table))
            self.keys_data = [row._asdict() for row in keys_result]

    def test_keys(self) -> None:
        """Test that composite keys are handled correctly by inspecting the
        data in the generated TAP_SCHEMA ``keys`` table.
        """
        print(f"\nComposite keys data: {self.keys_data}")

        self.assertEqual(len(self.keys_data), 1)

        self.assertEqual(
            self.keys_data[0],
            {
                "key_id": "fk_composite",
                "from_table": "test_composite_keys.table1",
                "target_table": "test_composite_keys.table2",
                "utype": "ForeignKey",
                "description": "Composite foreign key from table1 to table2",
            },
        )

    def test_key_columns(self) -> None:
        """Test that composite keys are handled correctly by inspecting the
        data in the generated TAP_SCHEMA ``key_columns`` table.
        """
        print(f"\nComposite key columns data: {self.key_columns_data}")

        self.assertEqual(len(self.key_columns_data), 2)

        key_columns_row1 = self.key_columns_data[0]
        self.assertEqual(
            key_columns_row1, {"key_id": "fk_composite", "from_column": "id1", "target_column": "id1"}
        )

        key_columns_row2 = self.key_columns_data[1]
        self.assertEqual(
            key_columns_row2, {"key_id": "fk_composite", "from_column": "id2", "target_column": "id2"}
        )


if __name__ == "__main__":
    unittest.main()
