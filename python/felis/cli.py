"""Click command line interface."""

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

from __future__ import annotations

import io
import logging
from collections.abc import Iterable
from typing import IO

import click
import yaml
from pydantic import ValidationError
from sqlalchemy.engine import Engine, create_engine, make_url
from sqlalchemy.engine.mock import MockConnection

from . import __version__
from .datamodel import Schema
from .db.utils import DatabaseContext
from .metadata import MetaDataBuilder
from .tap import Tap11Base, TapLoadingVisitor, init_tables

__all__ = ["cli"]

logger = logging.getLogger("felis")

loglevel_choices = ["CRITICAL", "FATAL", "ERROR", "WARNING", "INFO", "DEBUG"]


@click.group()
@click.version_option(__version__)
@click.option(
    "--log-level",
    type=click.Choice(loglevel_choices),
    envvar="FELIS_LOGLEVEL",
    help="Felis log level",
    default=logging.getLevelName(logging.INFO),
)
@click.option(
    "--log-file",
    type=click.Path(),
    envvar="FELIS_LOGFILE",
    help="Felis log file path",
)
def cli(log_level: str, log_file: str | None) -> None:
    """Felis command line tools"""
    if log_file:
        logging.basicConfig(filename=log_file, level=log_level)
    else:
        logging.basicConfig(level=log_level)


@cli.command("create", help="Create database objects from the Felis file")
@click.option("--engine-url", envvar="ENGINE_URL", help="SQLAlchemy Engine URL", default="sqlite://")
@click.option("--schema-name", help="Alternate schema name to override Felis file")
@click.option("--initialize", is_flag=True, help="Create the schema in the database if it does not exist")
@click.option(
    "--drop", is_flag=True, help="Drop schema if it already exists in the database (implies --initialize)"
)
@click.option("--echo", is_flag=True, help="Echo database commands as they are executed")
@click.option("--dry-run", is_flag=True, help="Dry run only to print out commands instead of executing")
@click.option(
    "--output-file", "-o", type=click.File(mode="w"), help="Write SQL commands to a file instead of executing"
)
@click.argument("file", type=click.File())
def create(
    engine_url: str,
    schema_name: str | None,
    initialize: bool,
    drop: bool,
    echo: bool,
    dry_run: bool,
    output_file: IO[str] | None,
    file: IO,
) -> None:
    """Create database objects from the Felis file.

    Parameters
    ----------
    engine_url
        SQLAlchemy Engine URL.
    schema_name
        Alternate schema name to override Felis file.
    initialize
        Create the schema in the database if it does not exist.
    drop
        Drop schema if it already exists in the database.
    echo
        Echo database commands as they are executed.
    dry_run
        Dry run only to print out commands instead of executing.
    output_file
        Write SQL commands to a file instead of executing.
    file
        Felis file to read.

    Notes
    -----
    This command creates database objects from the Felis file. The
    ``--initialize`` or ``--drop`` flags can be used to create a new MySQL
    database or PostgreSQL schema if it does not exist already. These options
    are mutually exclusive and an error will be raised if they are used
    together, because ``--drop`` always implies ``--initialize``.
    """
    try:
        yaml_data = yaml.safe_load(file)
        schema = Schema.model_validate(yaml_data)
        url = make_url(engine_url)
        if schema_name:
            logger.info(f"Overriding schema name with: {schema_name}")
            schema.name = schema_name
        elif url.drivername == "sqlite":
            logger.info("Overriding schema name for sqlite with: main")
            schema.name = "main"
        if not url.host and not url.drivername == "sqlite":
            dry_run = True
            logger.info("Forcing dry run for non-sqlite engine URL with no host")

        metadata = MetaDataBuilder(schema).build()
        logger.debug(f"Created metadata with schema name: {metadata.schema}")

        engine: Engine | MockConnection
        if not dry_run and not output_file:
            engine = create_engine(url, echo=echo)
        else:
            if dry_run:
                logger.info("Dry run will be executed")
            engine = DatabaseContext.create_mock_engine(url, output_file)
            if output_file:
                logger.info("Writing SQL output to: " + output_file.name)

        context = DatabaseContext(metadata, engine)

        if drop and initialize:
            raise ValueError("Cannot drop and initialize schema at the same time")

        if drop:
            logger.debug("Dropping schema if it exists")
            context.drop()
            initialize = True  # If schema is dropped, it needs to be recreated.

        if initialize:
            logger.debug("Creating schema if not exists")
            context.initialize()

        context.create_all()
    except Exception as e:
        logger.error(e)
        raise click.exceptions.Exit(1)


@cli.command("init-tap", help="Initialize TAP_SCHEMA objects in the database")
@click.option("--tap-schema-name", help="Alternate database schema name for 'TAP_SCHEMA'")
@click.option("--tap-schemas-table", help="Alternate table name for 'schemas'")
@click.option("--tap-tables-table", help="Alternate table name for 'tables'")
@click.option("--tap-columns-table", help="Alternate table name for 'columns'")
@click.option("--tap-keys-table", help="Alternate table name for 'keys'")
@click.option("--tap-key-columns-table", help="Alternate table name for 'key_columns'")
@click.argument("engine-url")
def init_tap(
    engine_url: str,
    tap_schema_name: str,
    tap_schemas_table: str,
    tap_tables_table: str,
    tap_columns_table: str,
    tap_keys_table: str,
    tap_key_columns_table: str,
) -> None:
    """Initialize TAP_SCHEMA objects in the database.

    Parameters
    ----------
    engine_url
        SQLAlchemy Engine URL. The target PostgreSQL schema or MySQL database
        must already exist and be referenced in the URL.
    tap_schema_name
        Alterate name for the database schema ``TAP_SCHEMA``.
    tap_schemas_table
        Alterate table name for ``schemas``.
    tap_tables_table
        Alterate table name for ``tables``.
    tap_columns_table
        Alterate table name for ``columns``.
    tap_keys_table
        Alterate table name for ``keys``.
    tap_key_columns_table
        Alterate table name for ``key_columns``.

    Notes
    -----
    The supported version of TAP_SCHEMA in the SQLAlchemy metadata is 1.1. The
    tables are created in the database schema specified by the engine URL,
    which must be a PostgreSQL schema or MySQL database that already exists.
    """
    engine = create_engine(engine_url, echo=True)
    init_tables(
        tap_schema_name,
        tap_schemas_table,
        tap_tables_table,
        tap_columns_table,
        tap_keys_table,
        tap_key_columns_table,
    )
    Tap11Base.metadata.create_all(engine)


@cli.command("load-tap", help="Load metadata from a Felis file into a TAP_SCHEMA database")
@click.option("--engine-url", envvar="ENGINE_URL", help="SQLAlchemy Engine URL to catalog")
@click.option("--schema-name", help="Alternate Schema Name for Felis file")
@click.option("--catalog-name", help="Catalog Name for Schema")
@click.option("--dry-run", is_flag=True, help="Dry Run Only. Prints out the DDL that would be executed")
@click.option("--tap-schema-name", help="Alternate schema name for 'TAP_SCHEMA'")
@click.option("--tap-tables-postfix", help="Postfix for TAP_SCHEMA table names")
@click.option("--tap-schemas-table", help="Alternate table name for 'schemas'")
@click.option("--tap-tables-table", help="Alternate table name for 'tables'")
@click.option("--tap-columns-table", help="Alternate table name for 'columns'")
@click.option("--tap-keys-table", help="Alternate table name for 'keys'")
@click.option("--tap-key-columns-table", help="Alternate table name for 'key_columns'")
@click.option("--tap-schema-index", type=int, help="TAP_SCHEMA index of the schema in this environment")
@click.argument("file", type=click.File())
def load_tap(
    engine_url: str,
    schema_name: str,
    catalog_name: str,
    dry_run: bool,
    tap_schema_name: str,
    tap_tables_postfix: str,
    tap_schemas_table: str,
    tap_tables_table: str,
    tap_columns_table: str,
    tap_keys_table: str,
    tap_key_columns_table: str,
    tap_schema_index: int,
    file: io.TextIOBase,
) -> None:
    """Load TAP metadata from a Felis file.

    This command loads the associated TAP metadata from a Felis YAML file
    into the TAP_SCHEMA tables.

    Parameters
    ----------
    engine_url
        SQLAlchemy Engine URL to catalog.
    schema_name
        Alternate schema name. This overrides the schema name in the
        ``catalog`` field of the Felis file.
    catalog_name
        Catalog name for the schema. This possibly duplicates the
        ``tap_schema_name`` argument (DM-44870).
    dry_run
        Dry run only to print out commands instead of executing.
    tap_schema_name
        Alternate name for the schema of TAP_SCHEMA in the database.
    tap_tables_postfix
        Postfix for TAP table names that will be automatically appended.
    tap_schemas_table
        Alternate table name for ``schemas``.
    tap_tables_table
        Alternate table name for ``tables``.
    tap_columns_table
        Alternate table name for ``columns``.
    tap_keys_table
        Alternate table name for ``keys``.
    tap_key_columns_table
        Alternate table name for ``key_columns``.
    tap_schema_index
        TAP_SCHEMA index of the schema in this TAP environment.
    file
        Felis file to read.

    Notes
    -----
    The data will be loaded into the TAP_SCHEMA from the engine URL. The
    tables must have already been initialized or an error will occur.
    """
    yaml_data = yaml.load(file, Loader=yaml.SafeLoader)
    schema = Schema.model_validate(yaml_data)

    tap_tables = init_tables(
        tap_schema_name,
        tap_tables_postfix,
        tap_schemas_table,
        tap_tables_table,
        tap_columns_table,
        tap_keys_table,
        tap_key_columns_table,
    )

    if not dry_run:
        engine = create_engine(engine_url)

        if engine_url == "sqlite://" and not dry_run:
            # In Memory SQLite - Mostly used to test
            Tap11Base.metadata.create_all(engine)

        tap_visitor = TapLoadingVisitor(
            engine,
            catalog_name=catalog_name,
            schema_name=schema_name,
            tap_tables=tap_tables,
            tap_schema_index=tap_schema_index,
        )
        tap_visitor.visit_schema(schema)
    else:
        conn = DatabaseContext.create_mock_engine(engine_url)

        tap_visitor = TapLoadingVisitor.from_mock_connection(
            conn,
            catalog_name=catalog_name,
            schema_name=schema_name,
            tap_tables=tap_tables,
            tap_schema_index=tap_schema_index,
        )
        tap_visitor.visit_schema(schema)


@cli.command("validate", help="Validate one or more Felis YAML files")
@click.option(
    "--check-description", is_flag=True, help="Check that all objects have a description", default=False
)
@click.option(
    "--check-redundant-datatypes", is_flag=True, help="Check for redundant datatype overrides", default=False
)
@click.option(
    "--check-tap-table-indexes",
    is_flag=True,
    help="Check that every table has a unique TAP table index",
    default=False,
)
@click.option(
    "--check-tap-principal",
    is_flag=True,
    help="Check that at least one column per table is flagged as TAP principal",
    default=False,
)
@click.argument("files", nargs=-1, type=click.File())
def validate(
    check_description: bool,
    check_redundant_datatypes: bool,
    check_tap_table_indexes: bool,
    check_tap_principal: bool,
    files: Iterable[io.TextIOBase],
) -> None:
    """Validate one or more felis YAML files.

    Parameters
    ----------
    check_description
        Check that all objects have a valid description.
    check_redundant_datatypes
        Check for redundant type overrides.
    check_tap_table_indexes
        Check that every table has a unique TAP table index.
    check_tap_principal
        Check that at least one column per table is flagged as TAP principal.
    files
        The Felis YAML files to validate.

    Raises
    ------
    click.exceptions.Exit
        If any validation errors are found. The ``ValidationError`` which is
        thrown when a schema fails to validate will be logged as an error
        message.

    Notes
    -----
    All of the ``check`` flags are turned off by default and represent
    optional validations controlled by the Pydantic context.
    """
    rc = 0
    for file in files:
        file_name = getattr(file, "name", None)
        logger.info(f"Validating {file_name}")
        try:
            data = yaml.load(file, Loader=yaml.SafeLoader)
            Schema.model_validate(
                data,
                context={
                    "check_description": check_description,
                    "check_redundant_datatypes": check_redundant_datatypes,
                    "check_tap_table_indexes": check_tap_table_indexes,
                    "check_tap_principal": check_tap_principal,
                },
            )
        except ValidationError as e:
            logger.error(e)
            rc = 1
    if rc:
        raise click.exceptions.Exit(rc)


if __name__ == "__main__":
    cli()
