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

from felis.cli import cli

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TEST_YAML = os.path.join(TESTDIR, "data", "test.yml")
TEST_MERGE_YAML = os.path.join(TESTDIR, "data", "test-merge.yml")


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
        """Test for create --dry-run command."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["create", "--schema-name=main", f"--engine-url={url}", "--dry-run", TEST_YAML],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)

    def test_init_tap(self) -> None:
        """Test for init-tap command."""
        url = f"sqlite:///{self.tmpdir}/tap.sqlite3"
        runner = CliRunner()
        result = runner.invoke(cli, ["init-tap", url], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

    def test_load_tap(self) -> None:
        """Test for load-tap command."""
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
        """Test for load-tap --dry-run command."""
        url = "postgresql+psycopg2://"

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


if __name__ == "__main__":
    unittest.main()
