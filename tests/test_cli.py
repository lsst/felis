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

import yaml
from sqlalchemy import create_engine

import felis.tap_schema as tap_schema
from felis.datamodel import Schema
from felis.db.utils import DatabaseContext
from felis.metadata import MetaDataBuilder
from felis.tests.run_cli import run_cli

TEST_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TEST_DIR, "data", "test.yml")
TEST_SALES_YAML = os.path.join(TEST_DIR, "data", "sales.yaml")


class CliTestCase(unittest.TestCase):
    """Tests for CLI commands."""

    def setUp(self) -> None:
        """Set up a temporary directory for tests."""
        self.tmpdir = tempfile.mkdtemp(dir=TEST_DIR)
        self.sqlite_url = f"sqlite:///{self.tmpdir}/db.sqlite3"
        print(f"Using temporary directory: {self.tmpdir}")

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_invalid_command(self) -> None:
        """Test for invalid command."""
        run_cli(["invalid"], expect_error=True)

    def test_help(self) -> None:
        """Test for help command."""
        run_cli(["--help"], print_output=True)

    def test_create(self) -> None:
        """Test for create command."""
        run_cli(["create", f"--engine-url={self.sqlite_url}", TEST_YAML])

    def test_create_with_dry_run(self) -> None:
        """Test for ``create --dry-run`` command."""
        run_cli(["create", "--schema-name=main", f"--engine-url={self.sqlite_url}", "--dry-run", TEST_YAML])

    def test_create_with_ignore_constraints(self) -> None:
        """Test ``--ignore-constraints`` flag of ``create`` command."""
        run_cli(
            [
                "create",
                "--schema-name=main",
                "--ignore-constraints",
                f"--engine-url={self.sqlite_url}",
                "--dry-run",
                TEST_YAML,
            ]
        )

    def test_validate(self) -> None:
        """Test validate command."""
        run_cli(["validate", TEST_YAML])

    def test_validate_with_id_generation(self) -> None:
        """Test that loading a schema with IDs works if ID generation is
        enabled. This is the default behavior.
        """
        test_yaml = os.path.join(TEST_DIR, "data", "test_id_generation.yaml")
        run_cli(["--id-generation", "validate", test_yaml])

    def test_validate_with_id_generation_error(self) -> None:
        """Test that loading a schema without IDs fails if ID generation is not
        enabled.
        """
        test_yaml = os.path.join(TEST_DIR, "data", "test_id_generation.yaml")
        run_cli(["--no-id-generation", "validate", test_yaml], expect_error=True)

    def test_validate_with_extra_checks(self) -> None:
        """Test schema validation flags."""
        run_cli(
            [
                "validate",
                "--check-description",
                "--check-tap-principal",
                "--check-tap-table-indexes",
                TEST_YAML,
            ]
        )

    def test_create_with_initialize_and_drop_error(self) -> None:
        """Test that initialize and drop can't be used together."""
        run_cli(["create", "--initialize", "--drop", TEST_YAML], expect_error=True)

    def test_load_tap_schema(self) -> None:
        """Test load-tap-schema command."""
        # Create the TAP_SCHEMA database.
        tap_schema_path = tap_schema.TableManager.get_tap_schema_std_path()
        run_cli(["--id-generation", "create", f"--engine-url={self.sqlite_url}", tap_schema_path])

        # Load the TAP_SCHEMA data.
        run_cli(["load-tap-schema", f"--engine-url={self.sqlite_url}", TEST_YAML])

    def test_init_tap_schema(self) -> None:
        """Test init-tap-schema command."""
        run_cli(["init-tap-schema", f"--engine-url={self.sqlite_url}"])

    def test_init_tap_schema_mock(self) -> None:
        """Test init-tap-schema command with a mock URL, which should throw
        an error, as this is not supported.
        """
        run_cli(["init-tap-schema", "sqlite://"], expect_error=True)

    def test_init_tap_schema_with_extensions(self) -> None:
        """Test init-tap-schema command with default extensions."""
        run_cli(
            [
                "init-tap-schema",
                f"--engine-url={self.sqlite_url}",
                "--extensions",
                "resource://felis/config/tap_schema/tap_schema_extensions.yaml",
            ]
        )

    def test_init_tap_schema_with_custom_extensions(self) -> None:
        """Test init-tap-schema command with custom extensions file."""
        extensions_file = os.path.join(self.tmpdir, "custom_extensions.yaml")
        extensions_content = """
    name: TAP_SCHEMA
    tables:
      - name: schemas
        columns:
          - name: field1
            datatype: char
            length: 64
            description: A custom field
    """
        with open(extensions_file, "w") as f:
            f.write(extensions_content)

        run_cli(["init-tap-schema", f"--engine-url={self.sqlite_url}", "--extensions", extensions_file])

    def test_diff(self) -> None:
        """Test for ``diff`` command."""
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TEST_DIR, "data", "test_diff2.yaml")

        run_cli(["diff", test_diff1, test_diff2])

    def test_diff_database(self) -> None:
        """Test for ``diff`` command with database."""
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TEST_DIR, "data", "test_diff2.yaml")

        engine = create_engine(self.sqlite_url)
        metadata_db = MetaDataBuilder(
            Schema.from_uri(test_diff1, context={"id_generation": True}), apply_schema_to_metadata=False
        ).build()
        metadata_db.create_all(engine)

        run_cli(["diff", f"--engine-url={self.sqlite_url}", test_diff2])

    def test_diff_database_with_table_filter(self) -> None:
        """Test for ``diff`` command with database and table filter. This
        should fail as table filters are not supported for this comparator.
        """
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TEST_DIR, "data", "test_diff2.yaml")

        db_url = f"sqlite:///{self.tmpdir}/tap_schema.sqlite3"
        engine = create_engine(db_url)
        metadata_db = MetaDataBuilder(
            Schema.from_uri(test_diff1, context={"id_generation": True}), apply_schema_to_metadata=False
        ).build()
        metadata_db.create_all(engine)

        run_cli(
            ["diff", f"--engine-url={db_url}", "--table", "table1", test_diff2],
            expect_error=True,
        )

    def test_diff_alembic(self) -> None:
        """Test for ``diff`` command with ``--alembic`` comparator option."""
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TEST_DIR, "data", "test_diff2.yaml")
        run_cli(["diff", "--comparator", "alembic", test_diff1, test_diff2], print_output=True)

    def test_diff_alembic_with_table_filter(self) -> None:
        """Test for ``diff`` command with ``--alembic`` option and a table
        filter. This should fail as table filters are not supported for this
        comparator.
        """
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TEST_DIR, "data", "test_diff2.yaml")

        run_cli(
            ["diff", "--comparator", "alembic", "--table", "table1", test_diff1, test_diff2],
            expect_error=True,
        )

    def test_diff_error(self) -> None:
        """Test for ``diff`` command with bad arguments."""
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        run_cli(["diff", test_diff1], expect_error=True)

    def test_diff_error_on_change(self) -> None:
        """Test for ``diff`` command with ``--error-on-change`` flag."""
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TEST_DIR, "data", "test_diff2.yaml")
        run_cli(["diff", "--error-on-change", test_diff1, test_diff2], expect_error=True, print_output=True)

    def test_diff_with_table_filter(self) -> None:
        """Test for ``diff`` command and a table filter."""
        test_diff1 = os.path.join(TEST_DIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TEST_DIR, "data", "test_diff2.yaml")

        run_cli(["diff", "--table", "table1", test_diff1, test_diff2], print_output=True)

    def test_dump_yaml(self) -> None:
        """Test for ``dump`` command with YAML output."""
        with tempfile.NamedTemporaryFile(delete=True, suffix=".yaml") as temp_file:
            run_cli(["dump", TEST_YAML, temp_file.name], print_output=True)

    @classmethod
    def _check_strip_ids(cls, obj: Any) -> None:
        """
        Recursively check that a dict/list structure has no attributes with key
        '@id'. Raises a ValueError if any '@id' key is found. This is used to
        check the output of the `--strip-ids` option in the `dump` command.
        """
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "@id":
                    raise ValueError("Found forbidden key '@id'")
                cls._check_strip_ids(v)
        elif isinstance(obj, list):
            for item in obj:
                cls._check_strip_ids(item)

    def test_dump_yaml_with_strip_ids(self) -> None:
        """Test for ``dump`` command with YAML output and stripped IDs."""
        with tempfile.NamedTemporaryFile(delete=True, suffix=".yaml") as temp_file:
            run_cli(["dump", "--strip-ids", TEST_YAML, temp_file.name], print_output=True)
            dumped_data = temp_file.read().decode("utf-8")
            try:
                # Load the dumped YAML data to check for '@id' keys.
                data = yaml.safe_load(dumped_data)
                self._check_strip_ids(data)
            except ValueError:
                self.fail("Dumped YAML contains forbidden key '@id'")

    def test_dump_json(self) -> None:
        """Test for ``dump`` command with JSON output."""
        with tempfile.NamedTemporaryFile(delete=True, suffix=".json") as temp_file:
            run_cli(["dump", TEST_YAML, temp_file.name], print_output=True)

    def test_dump_json_with_strip_ids(self) -> None:
        """Test for ``dump`` command with JSON output."""
        with tempfile.NamedTemporaryFile(delete=True, suffix=".json") as temp_file:
            run_cli(["dump", "--strip-ids", TEST_YAML, temp_file.name], print_output=True)
            dumped_data = temp_file.read().decode("utf-8")
            try:
                # Load the dumped YAML data to check for '@id' keys.
                data = yaml.safe_load(dumped_data)
                self._check_strip_ids(data)
            except ValueError:
                self.fail("Dumped YAML contains forbidden key '@id'")

    def test_dump_with_invalid_file_extension_error(self) -> None:
        """Test for ``dump`` command with JSON output."""
        run_cli(["dump", TEST_YAML, "out.bad"], expect_error=True)

    def test_create_and_drop_indexes(self) -> None:
        """Test creating and dropping indexes using CLI commands with
        SQLite.
        """
        # Load the schema to get expected indexes
        schema = Schema.from_uri(TEST_SALES_YAML)
        md_with_indexes = MetaDataBuilder(schema, skip_indexes=False).build()

        engine = create_engine(self.sqlite_url)

        def check_indexes_exist(should_exist: bool, message: str) -> None:
            """Check if indexes exist or don't exist in the database."""
            with engine.connect() as conn:
                for table in md_with_indexes.tables.values():
                    for index in table.indexes:
                        if index.name is not None:
                            exists = DatabaseContext._index_exists(conn, table, index)
                            if should_exist:
                                self.assertTrue(
                                    exists,
                                    f"Index '{index.name}' {message}",
                                )
                            else:
                                self.assertFalse(
                                    exists,
                                    f"Index '{index.name}' {message}",
                                )

        # Create database without indexes
        run_cli(["create", "--skip-indexes", f"--engine-url={self.sqlite_url}", TEST_SALES_YAML])

        # Check that indexes don't exist yet
        check_indexes_exist(False, "should not exist after create with --skip-indexes")

        # Create the indexes using CLI
        run_cli(
            ["create-indexes", f"--engine-url={self.sqlite_url}", TEST_SALES_YAML, "--schema-name", "main"]
        )

        # Check that indexes now exist
        check_indexes_exist(True, "should exist after create-indexes")

        # Create the indexes again; should not cause an error
        run_cli(
            ["create-indexes", f"--engine-url={self.sqlite_url}", TEST_SALES_YAML, "--schema-name", "main"]
        )

        # Drop the indexes using CLI
        run_cli(["drop-indexes", f"--engine-url={self.sqlite_url}", TEST_SALES_YAML, "--schema-name", "main"])

        # Check that indexes were dropped
        check_indexes_exist(False, "should not exist after drop-indexes")


if __name__ == "__main__":
    unittest.main()
