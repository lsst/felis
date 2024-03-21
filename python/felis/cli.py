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
import json
import logging
import os
import sys
from collections.abc import Iterable, Mapping, MutableMapping
from enum import Enum
from typing import Any

import click
import yaml
from pydantic import ValidationError
from pyld import jsonld
from sqlalchemy.engine import Engine, create_engine, create_mock_engine, make_url
from sqlalchemy.engine.mock import MockConnection

from . import DEFAULT_CONTEXT, DEFAULT_FRAME, __version__
from .check import CheckingVisitor
from .datamodel import Schema
from .sql import SQLVisitor
from .tap import Tap11Base, TapLoadingVisitor, init_tables
from .utils import ReorderingVisitor
from .validation import get_schema

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
    """Felis Command Line Tools."""
    if log_file:
        logging.basicConfig(filename=log_file, level=log_level)
    else:
        logging.basicConfig(level=log_level)


@cli.command("create-all")
@click.option("--engine-url", envvar="ENGINE_URL", help="SQLAlchemy Engine URL")
@click.option("--schema-name", help="Alternate Schema Name for Felis File")
@click.option("--dry-run", is_flag=True, help="Dry Run Only. Prints out the DDL that would be executed")
@click.argument("file", type=click.File())
def create_all(engine_url: str, schema_name: str, dry_run: bool, file: io.TextIOBase) -> None:
    """Create schema objects from the Felis FILE."""
    schema_obj = yaml.load(file, Loader=yaml.SafeLoader)
    visitor = SQLVisitor(schema_name=schema_name)
    schema = visitor.visit_schema(schema_obj)

    metadata = schema.metadata

    engine: Engine | MockConnection
    if not dry_run:
        engine = create_engine(engine_url)
    else:
        _insert_dump = InsertDump()
        engine = create_mock_engine(make_url(engine_url), executor=_insert_dump.dump)
        _insert_dump.dialect = engine.dialect
    metadata.create_all(engine)


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
    """Initialize TAP 1.1 TAP_SCHEMA objects.

    Please verify the schema/catalog you are executing this in in your
    engine URL.
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
    file: io.TextIOBase,
) -> None:
    """Load TAP metadata from a Felis FILE.

    This command loads the associated TAP metadata from a Felis FILE
    to the TAP_SCHEMA tables.
    """
    top_level_object = yaml.load(file, Loader=yaml.SafeLoader)
    schema_obj: dict
    if isinstance(top_level_object, dict):
        schema_obj = top_level_object
        if "@graph" not in schema_obj:
            schema_obj["@type"] = "felis:Schema"
        schema_obj["@context"] = DEFAULT_CONTEXT
    elif isinstance(top_level_object, list):
        schema_obj = {"@context": DEFAULT_CONTEXT, "@graph": top_level_object}
    else:
        logger.error("Schema object not of recognizable type")
        raise click.exceptions.Exit(1)

    normalized = _normalize(schema_obj, embed="@always")
    if len(normalized["@graph"]) > 1 and (schema_name or catalog_name):
        logger.error("--schema-name and --catalog-name incompatible with multiple schemas")
        raise click.exceptions.Exit(1)

    # Force normalized["@graph"] to a list, which is what happens when there's
    # multiple schemas
    if isinstance(normalized["@graph"], dict):
        normalized["@graph"] = [normalized["@graph"]]

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

        for schema in normalized["@graph"]:
            tap_visitor = TapLoadingVisitor(
                engine,
                catalog_name=catalog_name,
                schema_name=schema_name,
                tap_tables=tap_tables,
            )
            tap_visitor.visit_schema(schema)
    else:
        _insert_dump = InsertDump()
        conn = create_mock_engine(make_url(engine_url), executor=_insert_dump.dump, paramstyle="pyformat")
        # After the engine is created, update the executor with the dialect
        _insert_dump.dialect = conn.dialect

        for schema in normalized["@graph"]:
            tap_visitor = TapLoadingVisitor.from_mock_connection(
                conn,
                catalog_name=catalog_name,
                schema_name=schema_name,
                tap_tables=tap_tables,
            )
            tap_visitor.visit_schema(schema)


@cli.command("modify-tap")
@click.option("--start-schema-at", type=int, help="Rewrite index for tap:schema_index", default=0)
@click.argument("files", nargs=-1, type=click.File())
def modify_tap(start_schema_at: int, files: Iterable[io.TextIOBase]) -> None:
    """Modify TAP information in Felis schema FILES.

    This command has some utilities to aid in rewriting felis FILES
    in specific ways. It will write out a merged version of these files.
    """
    count = 0
    graph = []
    for file in files:
        schema_obj = yaml.load(file, Loader=yaml.SafeLoader)
        if "@graph" not in schema_obj:
            schema_obj["@type"] = "felis:Schema"
        schema_obj["@context"] = DEFAULT_CONTEXT
        schema_index = schema_obj.get("tap:schema_index")
        if not schema_index or (schema_index and schema_index > start_schema_at):
            schema_index = start_schema_at + count
            count += 1
        schema_obj["tap:schema_index"] = schema_index
        graph.extend(jsonld.flatten(schema_obj))
    merged = {"@context": DEFAULT_CONTEXT, "@graph": graph}
    normalized = _normalize(merged, embed="@always")
    _dump(normalized)


@cli.command("basic-check")
@click.argument("file", type=click.File())
def basic_check(file: io.TextIOBase) -> None:
    """Perform a basic check on a felis FILE.

    This performs a very check to ensure required fields are
    populated and basic semantics are okay. It does not ensure semantics
    are valid for other commands like create-all or load-tap.
    """
    schema_obj = yaml.load(file, Loader=yaml.SafeLoader)
    schema_obj["@type"] = "felis:Schema"
    # Force Context and Schema Type
    schema_obj["@context"] = DEFAULT_CONTEXT
    check_visitor = CheckingVisitor()
    check_visitor.visit_schema(schema_obj)


@cli.command("normalize")
@click.argument("file", type=click.File())
def normalize(file: io.TextIOBase) -> None:
    """Normalize a Felis FILE.

    Takes a felis schema FILE, expands it (resolving the full URLs),
    then compacts it, and finally produces output in the canonical
    format.

    (This is most useful in some debugging scenarios)

    See Also :

        https://json-ld.org/spec/latest/json-ld/#expanded-document-form
        https://json-ld.org/spec/latest/json-ld/#compacted-document-form
    """
    schema_obj = yaml.load(file, Loader=yaml.SafeLoader)
    schema_obj["@type"] = "felis:Schema"
    # Force Context and Schema Type
    schema_obj["@context"] = DEFAULT_CONTEXT
    expanded = jsonld.expand(schema_obj)
    normalized = _normalize(expanded, embed="@always")
    _dump(normalized)


@cli.command("merge")
@click.argument("files", nargs=-1, type=click.File())
def merge(files: Iterable[io.TextIOBase]) -> None:
    """Merge a set of Felis FILES.

    This will expand out the felis FILES so that it is easy to
    override values (using @Id), then normalize to a single
    output.
    """
    graph = []
    for file in files:
        schema_obj = yaml.load(file, Loader=yaml.SafeLoader)
        if "@graph" not in schema_obj:
            schema_obj["@type"] = "felis:Schema"
        schema_obj["@context"] = DEFAULT_CONTEXT
        graph.extend(jsonld.flatten(schema_obj))
    updated_map: MutableMapping[str, Any] = {}
    for item in graph:
        _id = item["@id"]
        item_to_update = updated_map.get(_id, item)
        if item_to_update and item_to_update != item:
            logger.debug(f"Overwriting {_id}")
        item_to_update.update(item)
        updated_map[_id] = item_to_update
    merged = {"@context": DEFAULT_CONTEXT, "@graph": list(updated_map.values())}
    normalized = _normalize(merged, embed="@always")
    _dump(normalized)


_IGNORE_INPUT_KEYS = ["columns", "tables", "constraints", "indexes", "primaryKey"]


class ValidationErrorFormat(str, Enum):
    """Format options for displaying validation errors."""

    json = "json"
    """Display the error in JSON format."""

    compact = "compact"
    """Display the error in a compact, single-line format."""

    verbose = "verbose"
    """Display the error using Pydantic's built-in formatting."""


def _display_validation_error(
    filename: str | None,
    validation_error: ValidationError,
    format: ValidationErrorFormat = ValidationErrorFormat.json,
    ignore_input_keys: list[str] = _IGNORE_INPUT_KEYS,
) -> list[Any]:
    """Display a Pydantic validation error with formatting options.

    Parameters
    ----------
    filename: `str`
        The filename of the schema file being validated
    validation_error : `ValidationError
        The validation erro`r to display.
    format : `ValidationErrorFormat`
        The format to display the error in
    ignore_input_keys : `list` of `str`
        A set of keys in the input to ignore when displaying the error

    Returns
    -------
    display_str : str
        The formatted error string
    """

    def truncate(s: str, length: int) -> str:
        """Truncate a string to a specified amount of characters."""
        return s[:length] + "..." if len(s) > length else s

    lines: list[Any] = []
    file_path = os.path.abspath(filename) if filename else None
    for error in validation_error.errors():
        loc = ".".join(str(loc) for loc in error["loc"])
        msg = error["msg"]
        if error["input"]:
            if isinstance(error["input"], dict):
                input_dict = {}
                for k, v in error["input"].items():
                    if k not in ignore_input_keys:
                        if k == "description":
                            v = truncate(v, 20)
                        input_dict[k] = v
                input = str(input_dict)
            else:
                input = error["input"]
        if format == ValidationErrorFormat.json:
            error_dict = {}
            if file_path:
                error_dict["file"] = file_path
            error_dict.update({"location": loc, "error": msg, "input": input})
            lines.append(error_dict)
        elif format == ValidationErrorFormat.compact:
            if file_path:
                lines.append(f"{file_path}:{loc}: {msg} [{input}]")
            else:
                lines.append(f"{loc}: {msg} {input}")
    return lines


def _validate_files(
    include_filename: bool,
    output_file: io.TextIOBase,
    format: ValidationErrorFormat,
    files: list[io.TextIOBase],
    schema_class: type[Schema],
) -> int:
    """Validate a list of files using the given schema class and options.

    Parameters
    ----------
    include_filename : `bool`
        Whether to include the filename in each error message
    output_file : `io.TextIOBase`
        The file to write the validation errors to
    format : `ValidationErrorFormat`
        The format to display the error in
    files : `list` of `io.TextIOBase`
        The list of files to validate
    schema_class : `type` of `Schema`
        The schema class to use for validation
    """
    if output_file:
        logger.info("Validation errors will be written to: " + getattr(output_file, "name", ""))

    if include_filename and format == ValidationErrorFormat.verbose:
        logger.warning("Ignoring '--include-filename' when using verbose format")

    return_code = 0
    file_lines: list[str] | None = None
    if output_file and format == ValidationErrorFormat.json and len(files) > 1:
        file_lines = []
        include_filename = True
        logger.info("Input file name will be included automatically for JSON file output")
    for file in files:
        input_file_name = getattr(file, "name", "")
        logger.info(f"Validating: {input_file_name}")
        try:
            schema_class.model_validate(yaml.load(file, Loader=yaml.SafeLoader))
            logger.info(f"Validation PASSED: {input_file_name}")
        except ValidationError as validation_error:
            return_code = 1
            logger.error(f"{len(validation_error.errors())} validation errors: {input_file_name}")
            error_display: str
            if format != ValidationErrorFormat.verbose:
                error_lines = _display_validation_error(
                    input_file_name if include_filename else None,
                    validation_error,
                    format=ValidationErrorFormat[format],
                )
            if format == ValidationErrorFormat.json and not output_file:
                error_display = json.dumps(error_lines, indent=4)
            elif format == ValidationErrorFormat.compact:
                error_display = "\n".join(error_lines)
            else:
                error_display = str(validation_error)

            if output_file:
                if format != ValidationErrorFormat.json:
                    output_file.write(error_display)
            else:
                logger.error("\n" + error_display)

            logger.info(f"Validation FAILED: {input_file_name}")

            if file_lines and format == ValidationErrorFormat.json and output_file:
                file_lines.extend(error_lines)

    if file_lines:
        # Dump all lines to an output JSON file.
        output_file.write(json.dumps(file_lines, indent=4))
    return return_code


@cli.command("validate")
@click.option(
    "-s",
    "--schema-name",
    help="Schema name for validation",
    type=click.Choice(["RSP", "default"]),
    default="default",
)
@click.option(
    "-d", "--require-description", is_flag=True, help="Require description for all objects", default=False
)
@click.option(
    "-i", "--include-filename", is_flag=True, help="Include the filename in each error message", default=False
)
@click.option("--output-file", "-o", type=click.File("w"), help="Output file for validation error messages")
@click.option(
    "--format",
    "-f",
    type=click.Choice([format.value for format in ValidationErrorFormat]),
    default=ValidationErrorFormat.json.value,
)
@click.argument("files", nargs=-1, type=click.File())
def validate(
    schema_name: str,
    require_description: bool,
    include_filename: bool,
    output_file: io.TextIOBase,
    format: str,
    files: Iterable[io.TextIOBase],
) -> None:
    """Validate one or more felis YAML files from the command line."""
    schema_class = get_schema(schema_name)
    logger.debug(f"Using schema type: {schema_class.__name__}")

    schema_class.require_description(require_description)
    logger.debug(f"Description required: {require_description}")

    logger.debug(f"Validating {len(list(files))} file(s)")
    return_code = _validate_files(
        include_filename, output_file, ValidationErrorFormat[format], list(files), schema_class
    )
    logger.debug("Done validating file(s)")

    if return_code:
        raise click.exceptions.Exit(return_code)


@cli.command("dump-json")
@click.option("-x", "--expanded", is_flag=True, help="Extended schema before dumping.")
@click.option("-f", "--framed", is_flag=True, help="Frame schema before dumping.")
@click.option("-c", "--compacted", is_flag=True, help="Compact schema before dumping.")
@click.option("-g", "--graph", is_flag=True, help="Pass graph option to compact.")
@click.argument("file", type=click.File())
def dump_json(
    file: io.TextIOBase,
    expanded: bool = False,
    compacted: bool = False,
    framed: bool = False,
    graph: bool = False,
) -> None:
    """Dump JSON representation using various JSON-LD options."""
    schema_obj = yaml.load(file, Loader=yaml.SafeLoader)
    schema_obj["@type"] = "felis:Schema"
    # Force Context and Schema Type
    schema_obj["@context"] = DEFAULT_CONTEXT

    if expanded:
        schema_obj = jsonld.expand(schema_obj)
    if framed:
        schema_obj = jsonld.frame(schema_obj, DEFAULT_FRAME)
    if compacted:
        options = {}
        if graph:
            options["graph"] = True
        schema_obj = jsonld.compact(schema_obj, DEFAULT_CONTEXT, options=options)
    json.dump(schema_obj, sys.stdout, indent=4)


def _dump(obj: Mapping[str, Any]) -> None:
    class OrderedDumper(yaml.Dumper):
        pass

    def _dict_representer(dumper: yaml.Dumper, data: Any) -> Any:
        return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())

    OrderedDumper.add_representer(dict, _dict_representer)
    print(yaml.dump(obj, Dumper=OrderedDumper, default_flow_style=False))


def _normalize(schema_obj: Mapping[str, Any], embed: str = "@last") -> MutableMapping[str, Any]:
    framed = jsonld.frame(schema_obj, DEFAULT_FRAME, options=dict(embed=embed))
    compacted = jsonld.compact(framed, DEFAULT_CONTEXT, options=dict(graph=True))
    graph = compacted["@graph"]
    graph = [ReorderingVisitor(add_type=True).visit_schema(schema_obj) for schema_obj in graph]
    compacted["@graph"] = graph if len(graph) > 1 else graph[0]
    return compacted


class InsertDump:
    """An Insert Dumper for SQL statements."""

    dialect: Any = None

    def dump(self, sql: Any, *multiparams: Any, **params: Any) -> None:
        compiled = sql.compile(dialect=self.dialect)
        sql_str = str(compiled) + ";"
        params_list = [compiled.params]
        for params in params_list:
            if not params:
                print(sql_str)
                continue
            new_params = {}
            for key, value in params.items():
                if isinstance(value, str):
                    new_params[key] = f"'{value}'"
                elif value is None:
                    new_params[key] = "null"
                else:
                    new_params[key] = value

            print(sql_str % new_params)


if __name__ == "__main__":
    cli()
