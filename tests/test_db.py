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
import tempfile
from unittest import TestCase

from sqlalchemy import MetaData, create_engine

from felis import Schema
from felis.db.schema import create_database
from felis.db.utils import DatabaseContext

TESTDIR = os.path.abspath(os.path.dirname(__file__))
TESTFILE = os.path.join(TESTDIR, "data", "sales.yaml")


class TestCreateDatabase(TestCase):
    """Test the creation of a database from a schema using the
    ``create_database`` utility function.
    """

    def check_tables(self, db: DatabaseContext, schema: Schema) -> None:
        metadata = MetaData()
        metadata.reflect(db.engine)
        for table in schema.tables:
            self.assertIn(table.name, metadata.tables)

    def check_db(self, db: DatabaseContext, expected_dialect: str = "sqlite") -> None:
        self.assertIsNotNone(db.engine)
        self.assertIsNotNone(db.metadata)
        self.assertIsNotNone(db.connection)
        self.assertEqual(db.dialect_name, expected_dialect)

    def test_sqlite_engine_str(self) -> None:
        """Test creating a database with a SQLite engine using a URI string."""
        with tempfile.NamedTemporaryFile(delete=True, suffix=".sqlite3") as tmpfile:
            schema = Schema.from_uri(TESTFILE)
            db = create_database(schema, f"sqlite:///{tmpfile.name}")
            self.check_db(db)
            self.check_tables(db, schema)

    def test_sqlite_in_memory(self) -> None:
        """Test creating a database with a SQLite engine using an in-memory
        database.
        """
        schema = Schema.from_uri(TESTFILE)
        db = create_database(schema)
        self.check_db(db)
        self.check_tables(db, schema)

    def test_sqlite_engine(self) -> None:
        """Test creating a database with a SQLite engine using an SQLAlchemy
        engine object.
        """
        with tempfile.NamedTemporaryFile(delete=True, suffix=".sqlite3") as tmpfile:
            schema = Schema.from_uri(TESTFILE)
            engine = create_engine(f"sqlite:///{tmpfile.name}")
            db = create_database(schema, engine)
            self.check_db(db)
            self.check_tables(db, schema)
