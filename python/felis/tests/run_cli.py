"""Test utility for running cli commands."""

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

import logging

from click.testing import CliRunner

from felis.cli import cli

__all__ = ["run_cli"]


def run_cli(
    cmd: list[str],
    log_level: int = logging.WARNING,
    catch_exceptions: bool = False,
    expect_error: bool = False,
    print_cmd: bool = False,
    print_output: bool = False,
    id_generation: bool = False,
) -> None:
    """Run a CLI command and check the exit code.

    Parameters
    ----------
    cmd : list[str]
        The command to run.
    log_level : int
        The logging level to use, by default logging.WARNING.
    catch_exceptions : bool
        Whether to catch exceptions, by default False.
    expect_error : bool
        Whether to expect an error, by default False.
    print_cmd : bool
        Whether to print the command, by default False.
    print_output : bool
        Whether to print the output, by default False.
    id_generation : bool
        Whether to enable id generation, by default False.
    """
    if not cmd:
        raise ValueError("No command provided.")
    full_cmd = ["--log-level", logging.getLevelName(log_level)] + cmd
    if id_generation:
        full_cmd = ["--id-generation"] + full_cmd
    if print_cmd:
        print(f"Running command: felis {' '.join(full_cmd)}")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        full_cmd,
        catch_exceptions=catch_exceptions,
    )
    if print_output:
        print(result.output)
    if expect_error:
        assert result.exit_code != 0
    else:
        assert result.exit_code == 0
