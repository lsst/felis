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

from click.testing import CliRunner
from sqlalchemy import create_engine

import felis.tap_schema as tap_schema
from felis.cli import cli
from felis.datamodel import Schema
from felis.metadata import MetaDataBuilder

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")


class CliTestCase(unittest.TestCase):
    """Tests for CLI commands."""

    def setUp(self) -> None:
        """Set up a temporary directory for tests."""
        self.tmpdir = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_all(self) -> None:
        """Test for create command."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["create", "--schema-name=main", f"--engine-url={url}", TEST_YAML],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)

    def test_create_all_dry_run(self) -> None:
        """Test for ``create --dry-run`` command."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["create", "--schema-name=main", f"--engine-url={url}", "--dry-run", TEST_YAML],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)

    def test_ignore_constraints(self) -> None:
        """Test ``--ignore-constraints`` flag of ``create`` command."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "create",
                "--schema-name=main",
                "--ignore-constraints",
                f"--engine-url={url}",
                "--dry-run",
                TEST_YAML,
            ],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)

    def test_validate_default(self) -> None:
        """Test validate command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", TEST_YAML], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_id_generation(self) -> None:
        """Test the ``--id-generation`` flag."""
        test_yaml = os.path.join(TESTDIR, "data", "test_id_generation.yaml")
        runner = CliRunner()
        result = runner.invoke(cli, ["--id-generation", "validate", test_yaml], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_no_id_generation(self) -> None:
        """Test that loading a schema without IDs fails if ID generation is not
        enabled.
        """
        test_yaml = os.path.join(TESTDIR, "data", "test_id_generation.yaml")
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", test_yaml], catch_exceptions=False)
        self.assertNotEqual(result.exit_code, 0)

    def test_validation_flags(self) -> None:
        """Test schema validation flags."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "validate",
                "--check-description",
                "--check-tap-principal",
                "--check-tap-table-indexes",
                TEST_YAML,
            ],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)

    def test_initialize_and_drop(self) -> None:
        """Test that initialize and drop can't be used together."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["create", "--initialize", "--drop", TEST_YAML],
            catch_exceptions=False,
        )
        self.assertTrue(result.exit_code != 0)

    def test_load_tap_schema(self) -> None:
        """Test load-tap-schema command."""
        # Create the TAP_SCHEMA database.
        url = f"sqlite:///{self.tmpdir}/load_tap_schema.sqlite3"
        runner = CliRunner()
        tap_schema_path = tap_schema.TableManager.get_tap_schema_std_path()
        result = runner.invoke(
            cli,
            ["--id-generation", "create", f"--engine-url={url}", tap_schema_path],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)

        # Load the TAP_SCHEMA data.
        runner = CliRunner()
        result = runner.invoke(
            cli, ["load-tap-schema", f"--engine-url={url}", TEST_YAML], catch_exceptions=False
        )
        self.assertEqual(result.exit_code, 0)

    def test_init_tap_schema(self) -> None:
        """Test init-tap-schema command."""
        url = f"sqlite:///{self.tmpdir}/init_tap_schema.sqlite3"
        runner = CliRunner()
        result = runner.invoke(cli, ["init-tap-schema", f"--engine-url={url}"], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_init_tap_schema_mock(self) -> None:
        """Test init-tap-schema command with a mock URL, which should throw
        an error, as this is not supported.
        """
        runner = CliRunner()
        result = runner.invoke(cli, ["init-tap-schema", "sqlite://"], catch_exceptions=False)
        self.assertNotEqual(result.exit_code, 0)

    def test_diff(self) -> None:
        """Test for ``diff`` command."""
        test_diff1 = os.path.join(TESTDIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TESTDIR, "data", "test_diff2.yaml")

        runner = CliRunner()
        result = runner.invoke(cli, ["diff", test_diff1, test_diff2], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_diff_database(self) -> None:
        """Test for ``diff`` command with database."""
        test_diff1 = os.path.join(TESTDIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TESTDIR, "data", "test_diff2.yaml")
        db_url = f"sqlite:///{self.tmpdir}/tap_schema.sqlite3"

        engine = create_engine(db_url)
        metadata_db = MetaDataBuilder(Schema.from_uri(test_diff1), apply_schema_to_metadata=False).build()
        metadata_db.create_all(engine)

        runner = CliRunner()
        result = runner.invoke(cli, ["diff", f"--engine-url={db_url}", test_diff2], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_diff_alembic(self) -> None:
        """Test for ``diff`` command with ``--alembic`` comparator option."""
        test_diff1 = os.path.join(TESTDIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TESTDIR, "data", "test_diff2.yaml")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["diff", "--comparator", "alembic", test_diff1, test_diff2], catch_exceptions=False
        )
        print(result.output)
        self.assertEqual(result.exit_code, 0)

    def test_diff_error(self) -> None:
        """Test for ``diff`` command with bad arguments."""
        test_diff1 = os.path.join(TESTDIR, "data", "test_diff1.yaml")
        runner = CliRunner()
        result = runner.invoke(cli, ["diff", test_diff1], catch_exceptions=False)
        self.assertNotEqual(result.exit_code, 0)

    def test_diff_error_on_change(self) -> None:
        """Test for ``diff`` command with ``--error-on-change`` flag."""
        test_diff1 = os.path.join(TESTDIR, "data", "test_diff1.yaml")
        test_diff2 = os.path.join(TESTDIR, "data", "test_diff2.yaml")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["diff", "--error-on-change", test_diff1, test_diff2], catch_exceptions=False
        )
        print(result.output)
        self.assertNotEqual(result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
