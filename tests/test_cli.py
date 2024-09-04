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

import felis.tap_schema as tap_schema
from felis.cli import cli
from felis.db.dialects import get_supported_dialects

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

    def test_init_tap(self) -> None:
        """Test for ``init-tap`` command."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"
        runner = CliRunner()
        result = runner.invoke(cli, ["init-tap", url], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_load_tap(self) -> None:
        """Test for ``load-tap`` command."""
        # Cannot use the same url for both init-tap and load-tap in the same
        # process.
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"

        # Need to run init-tap first.
        runner = CliRunner()
        result = runner.invoke(cli, ["init-tap", url])
        self.assertEqual(result.exit_code, 0)

        result = runner.invoke(cli, ["load-tap", f"--engine-url={url}", TEST_YAML], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_load_tap_mock(self) -> None:
        """Test ``load-tap --dry-run`` command on supported dialects."""
        urls = [f"{dialect_name}://" for dialect_name in get_supported_dialects().keys()]

        for url in urls:
            runner = CliRunner()
            result = runner.invoke(
                cli, ["load-tap", f"--engine-url={url}", "--dry-run", TEST_YAML], catch_exceptions=False
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
        """Test for ``load-tap-schema`` command."""
        # Create the TAP_SCHEMA database.
        url = f"sqlite:///{self.tmpdir}/tap_schema.sqlite3"
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


if __name__ == "__main__":
    unittest.main()
