"""Database utilities for Felis schemas."""

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

from sqlalchemy import Engine, create_engine

from ..datamodel import Schema
from ..metadata import MetaDataBuilder
from .utils import DatabaseContext

__all__ = ["create_database"]


def create_database(schema: Schema, engine_or_url_str: Engine | str | None = None) -> DatabaseContext:
    """
    Create a database from the specified `Schema`.

    Parameters
    ----------
    schema
        The schema to create.
    engine_or_url_str
        The SQLAlchemy engine or URL to use for database creation.
        If None, an in-memory SQLite database will be created.

    Returns
    -------
    `DatabaseContext`
        The database context object.
    """
    if engine_or_url_str is not None:
        engine = (
            engine_or_url_str if isinstance(engine_or_url_str, Engine) else create_engine(engine_or_url_str)
        )
    else:
        engine = create_engine("sqlite:///:memory:")
    metadata = MetaDataBuilder(
        schema, apply_schema_to_metadata=False if engine.url.drivername == "sqlite" else True
    ).build()
    ctx = DatabaseContext(metadata, engine)
    ctx.initialize()
    ctx.create_all()
    return ctx
