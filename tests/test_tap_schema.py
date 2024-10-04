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
from typing import Any

from sqlalchemy import Engine, MetaData, create_engine, select

from felis import tap
from felis.datamodel import Schema
from felis.tap_schema import DataLoader, TableManager
from felis.tests.utils import mk_temp_dir, open_test_file, rm_temp_dir


class TableManagerTestCase(unittest.TestCase):
    """Test the `TableManager` class."""

    def setUp(self) -> None:
        """Set up the test case."""
        with open_test_file("sales.yaml") as test_file:
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


class DataLoaderTestCase(unittest.TestCase):
    """Test the `DataLoader` class."""

    def setUp(self) -> None:
        """Set up the test case."""
        with open_test_file("sales.yaml") as test_file:
            self.schema = Schema.from_stream(test_file)

        self.tmpdir = mk_temp_dir()

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        rm_temp_dir(self.tmpdir)

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
                21,
                f"Expected 21 'INSERT INTO' statements, found {insert_count}",
            )


def _find_row(rows: list[dict[str, Any]], column_name: str, value: str) -> dict[str, Any]:
    next_row = next(
        (row for row in rows if row[column_name] == value),
        None,
    )
    assert next_row is not None
    assert isinstance(next_row, dict)
    return next_row


def _fetch_results(_engine: Engine, _metadata: MetaData) -> dict:
    results: dict[str, Any] = {}
    with _engine.connect() as connection:
        for table_name in TableManager.get_table_names_std():
            tap_table = _metadata.tables[table_name]
            primary_key_columns = tap_table.primary_key.columns
            stmt = select(tap_table).order_by(*primary_key_columns)
            result = connection.execute(stmt)
            column_data = [row._asdict() for row in result]
            results[table_name] = column_data
    return results


class TapSchemaDataTest(unittest.TestCase):
    """Test the validity of generated TAP SCHEMA data."""

    def setUp(self) -> None:
        """Set up the test case."""
        with open_test_file("test_tap_schema.yaml") as test_file:
            self.schema = Schema.from_stream(test_file, context={"id_generation": True})

        self.engine = create_engine("sqlite:///:memory:")

        mgr = TableManager(apply_schema_to_metadata=False)
        mgr.initialize_database(self.engine)
        self.mgr = mgr

        loader = DataLoader(self.schema, mgr, self.engine, tap_schema_index=2)
        loader.load()

        self.md = MetaData()
        self.md.reflect(self.engine)

    def test_schemas(self) -> None:
        schemas_table = self.mgr["schemas"]
        with self.engine.connect() as connection:
            result = connection.execute(select(schemas_table))
            schema_data = [row._asdict() for row in result]

        self.assertEqual(len(schema_data), 1)

        schema = schema_data[0]
        self.assertEqual(schema["schema_name"], "test_schema")
        self.assertEqual(schema["description"], "Test schema")
        self.assertEqual(schema["utype"], "Schema")
        self.assertEqual(schema["schema_index"], 2)

    def test_tables(self) -> None:
        tables_table = self.mgr["tables"]
        with self.engine.connect() as connection:
            result = connection.execute(select(tables_table))
            table_data = [row._asdict() for row in result]

        self.assertEqual(len(table_data), 2)

        table = table_data[0]
        assert isinstance(table, dict)
        self.assertEqual(table["schema_name"], "test_schema")
        self.assertEqual(table["table_name"], f"{self.schema.name}.table1")
        self.assertEqual(table["table_type"], "table")
        self.assertEqual(table["utype"], "Table")
        self.assertEqual(table["description"], "Test table 1")
        self.assertEqual(table["table_index"], 2)

    def test_columns(self) -> None:
        columns_table = self.mgr["columns"]
        with self.engine.connect() as connection:
            result = connection.execute(select(columns_table))
            column_data = [row._asdict() for row in result]

        table1_rows = [row for row in column_data if row["table_name"] == f"{self.schema.name}.table1"]
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
        keys_table = self.mgr["keys"]
        with self.engine.connect() as connection:
            result = connection.execute(select(keys_table))
            key_data = [row._asdict() for row in result]

        self.assertEqual(len(key_data), 1)

        key = key_data[0]
        assert isinstance(key, dict)

        self.assertEqual(key["key_id"], "fk_table1_to_table2")
        self.assertEqual(key["from_table"], f"{self.schema.name}.table1")
        self.assertEqual(key["target_table"], f"{self.schema.name}.table2")
        self.assertEqual(key["description"], "Foreign key from table1 to table2")
        self.assertEqual(key["utype"], "ForeignKey")

    def test_key_columns(self) -> None:
        key_columns_table = self.mgr["key_columns"]
        with self.engine.connect() as connection:
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
            self.mgr["bad_table"]

    def test_compare_to_tap(self) -> None:
        """Test that the generated data matches the records generated by the
        ``tap`` module, which ``tap_schema`` is designed to deprecate and
        eventually replace.
        """
        tap_tables = tap.init_tables()

        # Load the TAP_SCHEMA data using the tap module.
        tap_engine = create_engine("sqlite:///:memory:")
        tap.Tap11Base.metadata.create_all(tap_engine)
        visitor = tap.TapLoadingVisitor(
            tap_engine,
            tap_tables=tap_tables,
            tap_schema_index=2,
        )
        visitor.visit_schema(self.schema)

        # Reflect the generated TAP_SCHEMA data generated by the tap module.
        tap_md = MetaData()
        tap_md.reflect(tap_engine)

        # Gather data generated by tap.
        tap_results = _fetch_results(tap_engine, tap_md)

        # Gather data generated by tap_schema.
        tap_schema_results = _fetch_results(self.engine, self.md)

        # Table names should match.
        self.assertSetEqual(set(tap_results.keys()), set(tap_schema_results.keys()))

        # Perform a row-by-row comparison of the data.
        for table_name in tap_results:
            print(f"Comparing {table_name}")
            tap_data = tap_results[table_name]
            tap_schema_data = tap_schema_results[table_name]

            self.assertEqual(len(tap_data), len(tap_schema_data))

            for tap_row, tap_schema_row in zip(tap_data, tap_schema_data):
                print("tap: " + str(tap_row))
                print("tap_schema: " + str(tap_schema_row))
                self.assertDictEqual(tap_row, tap_schema_row)
