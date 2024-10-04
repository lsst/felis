"""Test utility functions."""

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
import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from typing import IO

__all__ = ["open_test_file", "mk_temp_dir", "rm_temp_dir"]

TEST_DATA_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "tests", "data"))
"""The directory containing test data files."""

TEST_TMP_DIR = os.path.normpath(os.path.join(TEST_DATA_DIR, ".."))
"""The directory for temporary files."""

__all__ = ["open_test_file", "mk_temp_dir", "rm_temp_dir"]

logger = logging.getLogger(__name__)


def get_test_file_path(file_name: str) -> str:
    """Return the path to a test file.

    Parameters
    ----------
    file_name
        The name of the test file.

    Returns
    -------
    str
        The path to the test file.

    Raises
    ------
    FileNotFoundError
        Raised if the file does not exist.
    """
    file_path = os.path.join(TEST_DATA_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    return file_path


@contextmanager
def open_test_file(file_name: str) -> Iterator[IO[str]]:
    """Return a file object for a test file using a context manager.

    Parameters
    ----------
    file_name
        The name of the test file.

    Returns
    -------
    `Iterator` [ `IO` [ `str` ] ]
        A file object for the test file.

    Raises
    ------
    FileNotFoundError
        Raised if the file does not exist.
    """
    logger.debug("Opening test file: %s", file_name)
    file_path = get_test_file_path(file_name)
    file = open(file_path)
    try:
        yield file
    finally:
        file.close()


def mk_temp_dir(parent_dir: str = TEST_TMP_DIR) -> str:
    """Create a temporary directory for testing.

    Parameters
    ----------
    parent_dir
        The parent directory for the temporary directory.

    Returns
    -------
    str
        The path to the temporary directory.
    """
    return tempfile.mkdtemp(dir=parent_dir)


def rm_temp_dir(temp_dir: str) -> None:
    """Remove a temporary directory.

    Parameters
    ----------
    temp_dir
        The path to the temporary directory.
    """
    logger.debug("Removing temporary directory: %s", temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)
