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

import logging
from collections.abc import Iterable
from typing import IO

import click
from pydantic import ValidationError
from sqlalchemy.engine import Engine, create_engine, make_url
from sqlalchemy.engine.mock import MockConnection, create_mock_engine

from . import __version__
from .datamodel import Schema
from .db.schema import create_database
from .db.utils import DatabaseContext, is_mock_url
from .diff import DatabaseDiff, FormattedSchemaDiff, SchemaDiff
from .metadata import MetaDataBuilder
from .tap_schema import DataLoader, TableManager

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
@click.option(
    "--id-generation", is_flag=True, help="Generate IDs for all objects that do not have them", default=False
)
@click.pass_context
def cli(ctx: click.Context, log_level: str, log_file: str | None, id_generation: bool) -> None:
    """Felis command line tools"""
    ctx.ensure_object(dict)
    ctx.obj["id_generation"] = id_generation
    if ctx.obj["id_generation"]:
        logger.info("ID generation is enabled")
    if log_file:
        logging.basicConfig(filename=log_file, level=log_level)
    else:
        logging.basicConfig(level=log_level)


@cli.command("create", help="Create database objects from the Felis file")
@click.option("--engine-url", envvar="FELIS_ENGINE_URL", help="SQLAlchemy Engine URL", default="sqlite://")
@click.option("--schema-name", help="Alternate schema name to override Felis file")
@click.option(
    "--initialize",
    is_flag=True,
    help="Create the schema in the database if it does not exist (error if already exists)",
)
@click.option(
    "--drop", is_flag=True, help="Drop schema if it already exists in the database (implies --initialize)"
)
@click.option("--echo", is_flag=True, help="Echo database commands as they are executed")
@click.option("--dry-run", is_flag=True, help="Dry run only to print out commands instead of executing")
@click.option(
    "--output-file", "-o", type=click.File(mode="w"), help="Write SQL commands to a file instead of executing"
)
@click.option("--ignore-constraints", is_flag=True, help="Ignore constraints when creating tables")
@click.argument("file", type=click.File())
@click.pass_context
def create(
    ctx: click.Context,
    engine_url: str,
    schema_name: str | None,
    initialize: bool,
    drop: bool,
    echo: bool,
    dry_run: bool,
    output_file: IO[str] | None,
    ignore_constraints: bool,
    file: IO[str],
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
    ignore_constraints
        Ignore constraints when creating tables.
    file
        Felis file to read.
    """
    try:
        schema = Schema.from_stream(file, context={"id_generation": ctx.obj["id_generation"]})
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

        metadata = MetaDataBuilder(schema, ignore_constraints=ignore_constraints).build()
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
        logger.exception(e)
        raise click.ClickException(str(e))


@cli.command("load-tap-schema", help="Load metadata from a Felis file into a TAP_SCHEMA database")
@click.option("--engine-url", envvar="FELIS_ENGINE_URL", help="SQLAlchemy Engine URL")
@click.option("--tap-schema-name", help="Name of the TAP_SCHEMA schema in the database (default: TAP_SCHEMA)")
@click.option(
    "--tap-tables-postfix", help="Postfix which is applied to standard TAP_SCHEMA table names", default=""
)
@click.option("--tap-schema-index", type=int, help="TAP_SCHEMA index of the schema in this environment")
@click.option("--dry-run", is_flag=True, help="Execute dry run only. Does not insert any data.")
@click.option("--echo", is_flag=True, help="Print out the generated insert statements to stdout")
@click.option("--output-file", type=click.Path(), help="Write SQL commands to a file")
@click.argument("file", type=click.File())
@click.pass_context
def load_tap_schema(
    ctx: click.Context,
    engine_url: str,
    tap_schema_name: str,
    tap_tables_postfix: str,
    tap_schema_index: int,
    dry_run: bool,
    echo: bool,
    output_file: str | None,
    file: IO[str],
) -> None:
    """Load TAP metadata from a Felis file.

    Parameters
    ----------
    engine_url
        SQLAlchemy Engine URL.
    tap_tables_postfix
        Postfix which is applied to standard TAP_SCHEMA table names.
    tap_schema_index
        TAP_SCHEMA index of the schema in this environment.
    dry_run
        Execute dry run only. Does not insert any data.
    echo
        Print out the generated insert statements to stdout.
    output_file
        Output file for writing generated SQL.
    file
        Felis file to read.

    Notes
    -----
    The TAP_SCHEMA database must already exist or the command will fail. This
    command will not initialize the TAP_SCHEMA tables.
    """
    url = make_url(engine_url)
    engine: Engine | MockConnection
    if dry_run or is_mock_url(url):
        engine = create_mock_engine(url, executor=None)
    else:
        engine = create_engine(engine_url)
    mgr = TableManager(
        engine=engine,
        apply_schema_to_metadata=False if engine.dialect.name == "sqlite" else True,
        schema_name=tap_schema_name,
        table_name_postfix=tap_tables_postfix,
    )

    schema = Schema.from_stream(file, context={"id_generation": ctx.obj["id_generation"]})

    DataLoader(
        schema,
        mgr,
        engine,
        tap_schema_index=tap_schema_index,
        dry_run=dry_run,
        print_sql=echo,
        output_path=output_file,
    ).load()


@cli.command("init-tap-schema", help="Initialize a standard TAP_SCHEMA database")
@click.option("--engine-url", envvar="FELIS_ENGINE_URL", help="SQLAlchemy Engine URL")
@click.option("--tap-schema-name", help="Name of the TAP_SCHEMA schema in the database")
@click.option(
    "--tap-tables-postfix", help="Postfix which is applied to standard TAP_SCHEMA table names", default=""
)
@click.pass_context
def init_tap_schema(
    ctx: click.Context, engine_url: str, tap_schema_name: str, tap_tables_postfix: str
) -> None:
    """Initialize a standard TAP_SCHEMA database.

    Parameters
    ----------
    engine_url
        SQLAlchemy Engine URL.
    tap_schema_name
        Name of the TAP_SCHEMA schema in the database.
    tap_tables_postfix
        Postfix which is applied to standard TAP_SCHEMA table names.
    """
    url = make_url(engine_url)
    engine: Engine | MockConnection
    if is_mock_url(url):
        raise click.ClickException("Mock engine URL is not supported for this command")
    engine = create_engine(engine_url)
    mgr = TableManager(
        apply_schema_to_metadata=False if engine.dialect.name == "sqlite" else True,
        schema_name=tap_schema_name,
        table_name_postfix=tap_tables_postfix,
    )
    mgr.initialize_database(engine)


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
@click.pass_context
def validate(
    ctx: click.Context,
    check_description: bool,
    check_redundant_datatypes: bool,
    check_tap_table_indexes: bool,
    check_tap_principal: bool,
    files: Iterable[IO[str]],
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
        Raised if any validation errors are found. The ``ValidationError``
        which is thrown when a schema fails to validate will be logged as an
        error message.

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
            Schema.from_stream(
                file,
                context={
                    "check_description": check_description,
                    "check_redundant_datatypes": check_redundant_datatypes,
                    "check_tap_table_indexes": check_tap_table_indexes,
                    "check_tap_principal": check_tap_principal,
                    "id_generation": ctx.obj["id_generation"],
                },
            )
        except ValidationError as e:
            logger.error(e)
            rc = 1
    if rc:
        raise click.exceptions.Exit(rc)


@cli.command(
    "diff",
    help="""
    Compare two schemas or a schema and a database for changes

    Examples:

      felis diff schema1.yaml schema2.yaml

      felis diff -c alembic schema1.yaml schema2.yaml

      felis diff --engine-url sqlite:///test.db schema.yaml
    """,
)
@click.option("--engine-url", envvar="FELIS_ENGINE_URL", help="SQLAlchemy Engine URL")
@click.option(
    "-c",
    "--comparator",
    type=click.Choice(["alembic", "deepdiff"], case_sensitive=False),
    help="Comparator to use for schema comparison",
    default="deepdiff",
)
@click.option("-E", "--error-on-change", is_flag=True, help="Exit with error code if schemas are different")
@click.argument("files", nargs=-1, type=click.File())
@click.pass_context
def diff(
    ctx: click.Context,
    engine_url: str | None,
    comparator: str,
    error_on_change: bool,
    files: Iterable[IO[str]],
) -> None:
    schemas = [
        Schema.from_stream(file, context={"id_generation": ctx.obj["id_generation"]}) for file in files
    ]

    diff: SchemaDiff
    if len(schemas) == 2 and engine_url is None:
        if comparator == "alembic":
            db_context = create_database(schemas[0])
            assert isinstance(db_context.engine, Engine)
            diff = DatabaseDiff(schemas[1], db_context.engine)
        else:
            diff = FormattedSchemaDiff(schemas[0], schemas[1])
    elif len(schemas) == 1 and engine_url is not None:
        engine = create_engine(engine_url)
        diff = DatabaseDiff(schemas[0], engine)
    else:
        raise click.ClickException(
            "Invalid arguments - provide two schemas or a schema and a database engine URL"
        )

    diff.print()

    if diff.has_changes and error_on_change:
        raise click.ClickException("Schema was changed")


if __name__ == "__main__":
    cli()
