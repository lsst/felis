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

from sqlalchemy import select

from felis.datamodel import Schema
from felis.db.database_context import create_database_context
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
        # For SQLite (default), metadata.schema is None but schema_name is set
        expected_metadata_schema = None if not mgr.apply_schema_to_metadata else schema_name
        self.assertEqual(mgr.metadata.schema, expected_metadata_schema)
        self.assertEqual(mgr.schema_name, "TAP_SCHEMA")  # schema_name should always be set
        for table_name in mgr.get_table_names_std():
            mgr[table_name]

        # Make sure that creating a new table manager works when one has
        # already been created.
        mgr = TableManager()

    def test_table_name_postfix(self) -> None:
        """Test the table name postfix."""
        mgr = TableManager(table_name_postfix="_test")
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
        mgr = TableManager()
        with create_database_context("sqlite:///:memory:", mgr.metadata) as db_ctx:
            mgr.initialize_database(db_ctx)

            loader = DataLoader(self.schema, mgr, db_context=db_ctx)
            loader.load()

    def test_sql_output(self) -> None:
        """Test printing SQL to stdout and writing SQL to a file."""
        mgr = TableManager()
        with create_database_context("sqlite:///:memory:", mgr.metadata) as db_ctx:
            loader = DataLoader(self.schema, mgr, db_ctx, dry_run=True, print_sql=True)
            loader.load()

            sql_path = os.path.join(self.tmpdir, "test_tap_schema_print_sql.sql")
            with open(sql_path, "w") as sql_file:
                loader = DataLoader(
                    self.schema, mgr, db_ctx, dry_run=True, print_sql=True, output_file=sql_file
                )
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
        mgr = TableManager()
        with create_database_context("sqlite:///:memory:", mgr.metadata) as db_ctx:
            mgr.initialize_database(db_ctx)

            loader = DataLoader(self.schema, mgr, db_context=db_ctx, unique_keys=True)
            loader.load()

            keys_data = mgr.select(db_ctx, "keys")
            self.assertGreaterEqual(len(keys_data), 1)
            for row in keys_data:
                self.assertTrue(row["key_id"].startswith(f"{self.schema.name}_"))

            key_columns_data = mgr.select(db_ctx, "key_columns")
            self.assertGreaterEqual(len(key_columns_data), 1)
            for row in key_columns_data:
                self.assertTrue(row["key_id"].startswith(f"{self.schema.name}_"))

    def test_select_with_filter(self) -> None:
        """Test selecting rows with a filter."""
        mgr = TableManager()
        with create_database_context("sqlite:///:memory:", mgr.metadata) as db_ctx:
            mgr.initialize_database(db_ctx)
            loader = DataLoader(self.schema, mgr, db_context=db_ctx, unique_keys=True)
            loader.load()

            rows = mgr.select(db_ctx, "columns", "table_name = 'test_schema.table1'")
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

        mgr = TableManager()
        # Create context manager but don't enter it yet - tests will do that
        self._mgr = mgr
        self._metadata = mgr.metadata

    @property
    def schema(self) -> Schema:
        """Return the schema."""
        return self._schema

    @property
    def mgr(self) -> TableManager:
        """Return the table manager."""
        return self._mgr

    @property
    def metadata(self) -> Any:
        """Return the metadata."""
        return self._metadata


class TapSchemaDataTest(unittest.TestCase):
    """Test the validity of generated TAP SCHEMA data."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.tap_schema_setup = TapSchemaSqliteSetup(TEST_TAP_SCHEMA, context={"id_generation": True})

    def test_schemas(self) -> None:
        with create_database_context("sqlite:///:memory:", self.tap_schema_setup.metadata) as db_ctx:
            self.tap_schema_setup.mgr.initialize_database(db_ctx)
            loader = DataLoader(
                self.tap_schema_setup.schema,
                self.tap_schema_setup.mgr,
                db_context=db_ctx,
                tap_schema_index=2,
            )
            loader.load()

            schemas_table = self.tap_schema_setup.mgr["schemas"]
            with db_ctx.engine.connect() as connection:
                result = connection.execute(select(schemas_table))
                schema_data = [row._asdict() for row in result]

        self.assertEqual(len(schema_data), 1)

        schema = schema_data[0]
        self.assertEqual(schema["schema_name"], "test_schema")
        self.assertEqual(schema["description"], "Test schema")
        self.assertEqual(schema["utype"], "Schema")
        self.assertEqual(schema["schema_index"], 2)

    def test_tables(self) -> None:
        with create_database_context("sqlite:///:memory:", self.tap_schema_setup.metadata) as db_ctx:
            self.tap_schema_setup.mgr.initialize_database(db_ctx)
            loader = DataLoader(
                self.tap_schema_setup.schema, self.tap_schema_setup.mgr, db_context=db_ctx, tap_schema_index=2
            )
            loader.load()

            tables_table = self.tap_schema_setup.mgr["tables"]
            with db_ctx.engine.connect() as connection:
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
        with create_database_context("sqlite:///:memory:", self.tap_schema_setup.metadata) as db_ctx:
            self.tap_schema_setup.mgr.initialize_database(db_ctx)
            loader = DataLoader(
                self.tap_schema_setup.schema, self.tap_schema_setup.mgr, db_context=db_ctx, tap_schema_index=2
            )
            loader.load()

            columns_table = self.tap_schema_setup.mgr["columns"]
            with db_ctx.engine.connect() as connection:
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
        with create_database_context("sqlite:///:memory:", self.tap_schema_setup.metadata) as db_ctx:
            self.tap_schema_setup.mgr.initialize_database(db_ctx)
            loader = DataLoader(
                self.tap_schema_setup.schema,
                self.tap_schema_setup.mgr,
                db_context=db_ctx,
                tap_schema_index=2,
            )
            loader.load()

            keys_table = self.tap_schema_setup.mgr["keys"]
            with db_ctx.engine.connect() as connection:
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
        with create_database_context("sqlite:///:memory:", self.tap_schema_setup.metadata) as db_ctx:
            self.tap_schema_setup.mgr.initialize_database(db_ctx)
            loader = DataLoader(
                self.tap_schema_setup.schema,
                self.tap_schema_setup.mgr,
                db_context=db_ctx,
                tap_schema_index=2,
            )
            loader.load()

            key_columns_table = self.tap_schema_setup.mgr["key_columns"]
            with db_ctx.engine.connect() as connection:
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
        with create_database_context("sqlite:///:memory:", self.tap_schema_setup.metadata) as db_ctx:
            self.tap_schema_setup.mgr.initialize_database(db_ctx)
            loader = DataLoader(
                self.tap_schema_setup.schema, self.tap_schema_setup.mgr, db_context=db_ctx, tap_schema_index=2
            )
            loader.load()

            columns_table = self.tap_schema_setup.mgr["columns"]
            with db_ctx.engine.connect() as connection:
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

        # Set up the data in a context manager
        with create_database_context("sqlite:///:memory:", self.tap_schema_setup.metadata) as db_ctx:
            self.tap_schema_setup.mgr.initialize_database(db_ctx)
            loader = DataLoader(
                self.tap_schema_setup.schema, self.tap_schema_setup.mgr, db_context=db_ctx, tap_schema_index=2
            )
            loader.load()

            # Fetch the keys and key_columns data from the TAP_SCHEMA tables.
            keys_table = self.tap_schema_setup.mgr["keys"]
            key_columns_table = self.tap_schema_setup.mgr["key_columns"]
            with db_ctx.engine.connect() as connection:
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


class TableManagerExtensionsTestCase(unittest.TestCase):
    """Test the `TableManager` class with extensions."""

    def setUp(self) -> None:
        """Set up the test case."""
        self.tmpdir = tempfile.mkdtemp(dir=TEST_DIR)

        self.extensions_path = os.path.join(self.tmpdir, "test_extensions.yaml")
        extensions_content = """
name: test_extensions
description: Test TAP_SCHEMA extensions

tables:
  - name: schemas
    description: Extensions to schemas table
    columns:
      - name: owner_id
        datatype: char
        length: 32
        nullable: true
        description: "Owner identifier"
      - name: read_anon
        datatype: int
        nullable: true
        description: "Anon read flag"

  - name: tables
    description: Extensions to tables table
    columns:
      - name: api_created
        datatype: int
        nullable: true
        description: "API created flag"
"""
        with open(self.extensions_path, "w") as f:
            f.write(extensions_content)

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_extensions_applied(self) -> None:
        mgr = TableManager(extensions_path=self.extensions_path)

        schemas_table = mgr["schemas"]
        self.assertIn("owner_id", schemas_table.c)
        self.assertIn("read_anon", schemas_table.c)

        tables_table = mgr["tables"]
        self.assertIn("api_created", tables_table.c)

    def test_extensions_column_count(self) -> None:
        mgr_without = TableManager()
        mgr_with = TableManager(extensions_path=self.extensions_path)

        schemas_before = len(mgr_without["schemas"].c)
        schemas_after = len(mgr_with["schemas"].c)
        self.assertEqual(schemas_after, schemas_before + 2)

        tables_before = len(mgr_without["tables"].c)
        tables_after = len(mgr_with["tables"].c)
        self.assertEqual(tables_after, tables_before + 1)

    def test_extensions_with_data_loader(self) -> None:
        mgr = TableManager(extensions_path=self.extensions_path)
        with create_database_context("sqlite:///:memory:", mgr.metadata) as db_ctx:
            mgr.initialize_database(db_ctx)

            with open(TEST_TAP_SCHEMA) as test_file:
                schema = Schema.from_stream(test_file, context={"id_generation": True})

            loader = DataLoader(schema, mgr, db_context=db_ctx)
            loader.load()

            schemas_table = mgr["schemas"]
            with db_ctx.engine.connect() as connection:
                result = connection.execute(select(schemas_table))
                row = result.fetchone()
                self.assertIn("owner_id", row._fields)
                self.assertIn("read_anon", row._fields)

    def test_invalid_extensions_file(self) -> None:
        invalid_path = os.path.join(self.tmpdir, "nonexistent.yaml")

        with self.assertRaises(ValueError):
            TableManager(extensions_path=invalid_path)

    def test_empty_extensions(self) -> None:
        empty_extensions_path = os.path.join(self.tmpdir, "empty_extensions.yaml")
        with open(empty_extensions_path, "w") as f:
            f.write("name: empty_extensions\ntables: []\n")

        mgr = TableManager(extensions_path=empty_extensions_path)
        self.assertIsNotNone(mgr["schemas"])

    def test_extensions_with_null_table_extensions(self) -> None:
        null_extensions_path = os.path.join(self.tmpdir, "null_extensions.yaml")
        with open(null_extensions_path, "w") as f:
            f.write("""
name: null_extensions
tables:
  - name: schemas
    columns: []
  - name: tables
    columns: []
  - name: columns
    columns:
      - name: test_col
        datatype: int
""")

        mgr = TableManager(extensions_path=null_extensions_path)

        columns_table = mgr["columns"]
        self.assertIn("test_col", columns_table.c)

        schemas_table = mgr["schemas"]
        self.assertNotIn("owner_id", schemas_table.c)

    def test_extensions_invalid_column_missing_name(self) -> None:
        invalid_name_path = os.path.join(self.tmpdir, "invalid_name.yaml")
        with open(invalid_name_path, "w") as f:
            f.write("""
    name: invalid_name
    tables:
      - name: schemas
        columns:
          - datatype: int
            description: "Missing name"
          - name: some_column
            datatype: int
    """)
        with self.assertRaises(KeyError):
            TableManager(extensions_path=invalid_name_path)

    def test_extensions_column_id_auto_generation(self) -> None:
        auto_id_path = os.path.join(self.tmpdir, "auto_id.yaml")
        with open(auto_id_path, "w") as f:
            f.write("""
name: auto_id
tables:
  - name: schemas
    columns:
      - name: auto_id
        datatype: int
        nullable: false
        description: "Column with auto_id"
""")

        mgr = TableManager(extensions_path=auto_id_path)
        schemas_table = mgr["schemas"]
        self.assertIn("auto_id", schemas_table.c)

    def test_extensions_column_id_preserved(self) -> None:
        explicit_id_path = os.path.join(self.tmpdir, "explicit_id.yaml")
        with open(explicit_id_path, "w") as f:
            f.write("""
name: explicit_id
tables:
  - name: schemas
    columns:
      - name: explicit_id
        datatype: int
        "@id": "#custom.id.path"
""")

        mgr = TableManager(extensions_path=explicit_id_path)
        schemas_table = mgr["schemas"]
        self.assertIn("explicit_id", schemas_table.c)

    def test_extensions_multiple_tables_extended(self) -> None:
        multi_table_path = os.path.join(self.tmpdir, "multi_table.yaml")
        with open(multi_table_path, "w") as f:
            f.write("""
name: multi_table
tables:
  - name: schemas
    columns:
      - name: schema_ext1
        datatype: int
      - name: schema_ext2
        datatype: int
  - name: tables
    columns:
      - name: table_ext1
        datatype: int
      - name: table_ext2
        datatype: double
  - name: columns
    columns:
      - name: col_ext1
        datatype: int
  - name: keys
    columns:
      - name: key_ext1
        datatype: char
        length: 128
""")

        mgr = TableManager(extensions_path=multi_table_path)

        schemas_table = mgr["schemas"]
        self.assertIn("schema_ext1", schemas_table.c)
        self.assertIn("schema_ext2", schemas_table.c)

        tables_table = mgr["tables"]
        self.assertIn("table_ext1", tables_table.c)
        self.assertIn("table_ext2", tables_table.c)

        columns_table = mgr["columns"]
        self.assertIn("col_ext1", columns_table.c)

        keys_table = mgr["keys"]
        self.assertIn("key_ext1", keys_table.c)

    def test_extensions_nonexistent_table_skipped(self) -> None:
        nonexistent_table_path = os.path.join(self.tmpdir, "nonexistent_table.yaml")
        with open(nonexistent_table_path, "w") as f:
            f.write("""
name: test_extensions_nonexistent_table
tables:
  - name: schemas
    columns:
      - name: valid_ext
        datatype: int
  - name: nonexistent_table
    columns:
      - name: should_be_ignored
        datatype: int
""")

        mgr = TableManager(extensions_path=nonexistent_table_path)
        schemas_table = mgr["schemas"]
        self.assertIn("valid_ext", schemas_table.c)

    def test_extensions_column_properties_preserved(self) -> None:
        full_props_path = os.path.join(self.tmpdir, "full_props.yaml")
        with open(full_props_path, "w") as f:
            f.write("""
name: full_props
tables:
  - name: schemas
    columns:
      - name: full_property_column
        datatype: char
        length: 64
        nullable: false
        description: "Column with all properties"
        "@id": "#tap_schema.schemas.full_property_column"
""")

        mgr = TableManager(extensions_path=full_props_path)
        schemas_table = mgr["schemas"]
        self.assertIn("full_property_column", schemas_table.c)

    def test_extensions_apply_schema_to_metadata_true(self) -> None:
        mgr = TableManager(
            engine_url="postgresql://user:pass@localhost/db", extensions_path=self.extensions_path
        )
        schemas_table = mgr["schemas"]
        self.assertIn("owner_id", schemas_table.c)

    def test_extensions_apply_schema_to_metadata_false(self) -> None:
        mgr = TableManager(extensions_path=self.extensions_path)

        schemas_table = mgr["schemas"]
        self.assertIn("owner_id", schemas_table.c)
        self.assertIn("read_anon", schemas_table.c)

    def test_extensions_with_table_name_postfix(self) -> None:
        mgr = TableManager(extensions_path=self.extensions_path, table_name_postfix="_custom")

        schemas_table = mgr["schemas"]
        self.assertIn("owner_id", schemas_table.c)

    def test_extensions_metadata_builder_called(self) -> None:
        mgr = TableManager(extensions_path=self.extensions_path)

        self.assertIsNotNone(mgr._metadata)

        table_names = list(mgr.metadata.tables.keys())
        found_schemas = any("schemas" in name for name in table_names)
        found_tables = any("tables" in name and "schemas" not in name for name in table_names)

        self.assertTrue(found_schemas, f"No schemas table found in {table_names}")
        self.assertTrue(found_tables, f"No tables table found in {table_names}")

    def test_extensions_preserve_original_columns(self) -> None:
        mgr = TableManager(extensions_path=self.extensions_path)

        schemas_table = mgr["schemas"]
        column_names = [col.name for col in schemas_table.columns]

        self.assertIn("schema_name", column_names)
        self.assertIn("owner_id", column_names)
        self.assertIn("read_anon", column_names)

    def test_no_extensions_path_provided(self) -> None:
        mgr = TableManager(extensions_path=None)
        schemas_table = mgr["schemas"]
        self.assertNotIn("owner_id", schemas_table.c)

    def test_extensions_path_empty_string(self) -> None:
        mgr = TableManager(extensions_path="")
        schemas_table = mgr["schemas"]
        self.assertNotIn("owner_id", schemas_table.c)

    def test_extensions_file_not_found(self) -> None:
        nonexistent_path = os.path.join(self.tmpdir, "does_not_exist.yaml")
        with self.assertRaises(ValueError):
            TableManager(extensions_path=nonexistent_path)


if __name__ == "__main__":
    unittest.main()
