"""Provides a temporary Postgresql instance for testing."""

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

import gc
import unittest
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, create_engine

try:
    from testing.postgresql import Postgresql  # type: ignore
except ImportError:
    Postgresql = None

__all__ = ["TemporaryPostgresInstance", "setup_postgres_test_db"]


class TemporaryPostgresInstance:
    """Wrapper for a temporary Postgres database.

    Parameters
    ----------
    server
        The ``testing.postgresql.Postgresql`` instance.
    engine
        The SQLAlchemy engine for the temporary database server.

    Notes
    -----
    This class was copied and modified from
    ``lsst.daf.butler.tests.postgresql``.
    """

    def __init__(self, server: Postgresql, engine: Engine) -> None:
        """Initialize the temporary Postgres database instance."""
        self._server = server
        self._engine = engine

    @property
    def url(self) -> str:
        """Return connection URL for the temporary database server.

        Returns
        -------
        str
            The connection URL.
        """
        return self._server.url()

    @property
    def engine(self) -> Engine:
        """Return the SQLAlchemy engine for the temporary database server.

        Returns
        -------
        `~sqlalchemy.engine.Engine`
            The SQLAlchemy engine.
        """
        return self._engine

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        """Return a SQLAlchemy connection to the test database.

        Returns
        -------
        `~sqlalchemy.engine.Connection`
            The SQLAlchemy connection.
        """
        with self._engine.begin() as connection:
            yield connection

    def print_info(self) -> None:
        """Print information about the temporary database server."""
        print("\n\n---- PostgreSQL URL ----")
        print(self.url)
        self._engine = create_engine(self.url)
        with self.begin() as conn:
            print("\n---- PostgreSQL Version ----")
            res = conn.execute(text("SELECT version()")).fetchone()
            if res:
                print(res[0])
            print("\n")


@contextmanager
def setup_postgres_test_db() -> Iterator[TemporaryPostgresInstance]:
    """Set up a temporary Postgres database instance that can be used for
    testing.

    Returns
    -------
    TemporaryPostgresInstance
        The temporary Postgres database instance.

    Raises
    ------
    unittest.SkipTest
        Raised if the ``testing.postgresql`` module is not available.
    """
    if Postgresql is None:
        raise unittest.SkipTest("testing.postgresql module not available.")

    with Postgresql() as server:
        engine = create_engine(server.url())
        instance = TemporaryPostgresInstance(server, engine)
        yield instance

        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        engine.dispose()
