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
    """Felis Command Line Tools.

    Parameters
    ----------
    log_level : `str`
        Felis log level.
    log_file : `str`, optional
        Felis log file path.

    Notes
    -----
    These options are used to configure the logging level and file for the
    command line tools.
    """
    if log_file:
        logging.basicConfig(filename=log_file, level=log_level)
    else:
        logging.basicConfig(level=log_level)


@cli.command("create")
@click.option("--engine-url", envvar="ENGINE_URL", help="SQLAlchemy Engine URL", default="sqlite://")
@click.option("--schema-name", help="Alternate schema name to override Felis file")
@click.option(
    "--create-if-not-exists", is_flag=True, help="Create the schema in the database if it does not exist"
)
@click.option("--drop-if-exists", is_flag=True, help="Drop schema if it already exists in the database")
@click.option("--echo", is_flag=True, help="Echo database commands as they are executed")
@click.option("--dry-run", is_flag=True, help="Dry run only to print out commands instead of executing")
@click.option(
    "--output-file", "-o", type=click.File(mode="w"), help="Write SQL commands to a file instead of executing"
)
@click.argument("file", type=click.File())
def create(
    engine_url: str,
    schema_name: str | None,
    create_if_not_exists: bool,
    drop_if_exists: bool,
    echo: bool,
    dry_run: bool,
    output_file: IO[str] | None,
    file: IO,
) -> None:
    """Create database objects from the Felis file.

    Parameters
    ----------
    engine_url : `str`
        SQLAlchemy Engine URL.
    schema_name : `str`, optional
        Alternate schema name to override Felis file.
    create_if_not_exists : bool
        Create the schema in the database if it does not exist.
    drop_if_exists : bool
        Drop schema if it already exists in the database.
    echo : bool
        Echo database commands as they are executed.
    dry_run : bool
        Dry run only to print out commands instead of executing.
    output_file : IO[str], optional
        Write SQL commands to a file instead of executing.
    file : IO
        Felis file to read.

    Notes
    -----
    This command creates database objects from the Felis file. The
    ``create_if_not_exists`` and ``drop_if_exists`` options can be used to
    create a new MySQL database or PostgreSQL schema if it does not exist
    already.
    """
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

    if drop_if_exists:
        logger.debug("Dropping schema if it exists")
        context.drop_if_exists()
        create_if_not_exists = True  # If schema is dropped, it needs to be recreated.

    if create_if_not_exists:
        logger.debug("Creating schema if not exists")
        context.create_if_not_exists()

    context.create_all()


@cli.command("init-tap")
@click.option("--tap-schema-name", help="Alt Schema Name for TAP_SCHEMA")
@click.option("--tap-schemas-table", help="Alt Table Name for TAP_SCHEMA.schemas")
@click.option("--tap-tables-table", help="Alt Table Name for TAP_SCHEMA.tables")
@click.option("--tap-columns-table", help="Alt Table Name for TAP_SCHEMA.columns")
@click.option("--tap-keys-table", help="Alt Table Name for TAP_SCHEMA.keys")
@click.option("--tap-key-columns-table", help="Alt Table Name for TAP_SCHEMA.key_columns")
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
    engine_url : `str`
        SQLAlchemy Engine URL. The target PostgreSQL schema or MySQL database
        must already exist and be referenced in the URL.
    tap_schema_name : `str`
        Alterate name for the database schema representing `TAP_SCHEMA`.
    tap_schemas_table : `str`
        Alterate name for `TAP_SCHEMA.schemas` table.
    tap_tables_table : `str`
        Alterate name for `TAP_SCHEMA.tables` table.
    tap_columns_table : `str`
        Alterate name for `TAP_SCHEMA.columns` table.
    tap_keys_table : `str`
        Alterate table name for `TAP_SCHEMA.keys` table.
    tap_key_columns_table : `str`
        Alterate table name for `TAP_SCHEMA.key_columns` table.

    Returns
    -------
    `None`

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


@cli.command("load-tap")
@click.option("--engine-url", envvar="ENGINE_URL", help="SQLAlchemy Engine URL to catalog")
@click.option("--schema-name", help="Alternate Schema Name for Felis file")
@click.option("--catalog-name", help="Catalog Name for Schema")
@click.option("--dry-run", is_flag=True, help="Dry Run Only. Prints out the DDL that would be executed")
@click.option("--tap-schema-name", help="Alt Schema Name for TAP_SCHEMA")
@click.option("--tap-tables-postfix", help="Postfix for TAP table names")
@click.option("--tap-schemas-table", help="Alt Table Name for TAP_SCHEMA.schemas")
@click.option("--tap-tables-table", help="Alt Table Name for TAP_SCHEMA.tables")
@click.option("--tap-columns-table", help="Alt Table Name for TAP_SCHEMA.columns")
@click.option("--tap-keys-table", help="Alt Table Name for TAP_SCHEMA.keys")
@click.option("--tap-key-columns-table", help="Alt Table Name for TAP_SCHEMA.key_columns")
@click.option("--tap-schema-index", type=int, help="TAP_SCHEMA index of the schema")
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
    """Load TAP metadata from a Felis FILE.

    This command loads the associated TAP metadata from a Felis YAML file
    into the TAP_SCHEMA tables.

    Parameters
    ----------
    engine_url : `str`
        SQLAlchemy Engine URL to catalog.
    schema_name : `str`
        Alternate schema name. This overrides the schema name in the `catalog`
        field of the Felis file.
    catalog_name : `str`
        Catalog name for the schema. This possibly duplicates the `schema_name`
        argument (DM-44870).
    dry_run : `bool`
        Dry run only to print out commands instead of executing.
    tap_schema_name : `str`
        Alternate name for the schema of TAP_SCHEMA in the database.
    tap_tables_postfix : `str`
        Postfix for TAP table names that will be automatically appended.
    tap_schemas_table : `str`
        Alternate table name for `TAP_SCHEMA.schemas`.
    tap_tables_table : `str`
        Alternate table name for `TAP_SCHEMA.tables`.
    tap_columns_table : `str`
        Alternate table name for `TAP_SCHEMA.columns`.
    tap_keys_table : `str`
        Alternate table name for `TAP_SCHEMA.keys`.
    tap_key_columns_table : `str`
        Alternate table name for `TAP_SCHEMA.key_columns`.
    tap_schema_index : `int`
        TAP_SCHEMA index of the schema, which is transient because the value
        depends on a particular environment.
    file:
        Felis file to read.

    Notes
    -----
    The data will be loaded into the TAP_SCHEMA from the engine URL. The
    TAP_SCHEMA tables must already exist.
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


@cli.command("validate")
@click.option("--check-description", is_flag=True, help="Require description for all objects", default=False)
@click.option(
    "--check-redundant-datatypes", is_flag=True, help="Check for redundant datatypes", default=False
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
    check_description : `bool`
        Require a valid description for all objects.
    check_redundant_datatypes : `bool`
        Check for redundant type overrides.
    check_tap_table_indexes : `bool`
        Check that every table has a unique TAP table index.
    check_tap_principal : `bool`
        Check that at least one column per table is flagged as TAP principal.
    files : `Iterable[io.TextIOBase]`
        The files to validate.

    Raises
    ------
    click.exceptions.Exit
        If any validation errors are found. The `ValidationError` which is
        thrown when a schema fails to validate will be logged as an error
        message.

    Notes
    -----
    All of the "check" flags are turned off by default.
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
