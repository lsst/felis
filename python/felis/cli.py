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

import io
import json
import logging
import sys
from collections.abc import Iterable, Mapping, MutableMapping
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

logger = logging.getLogger("felis")


@click.group()
@click.version_option(__version__)
def cli() -> None:
    """Felis Command Line Tools."""
    logging.basicConfig(level=logging.INFO)


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
@click.option("--start-schema-at", type=int, help="Rewrite index for tap:schema_index")
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


@cli.command("validate")
@click.argument("files", nargs=-1, type=click.File())
def validate(files: Iterable[io.TextIOBase]) -> None:
    """Validate one or more felis YAML files."""
    rc = 0
    for file in files:
        file_name = getattr(file, "name", None)
        logger.info(f"Validating {file_name}")
        try:
            Schema.model_validate(yaml.load(file, Loader=yaml.SafeLoader))
        except ValidationError as e:
            logger.error(e)
            rc = 1
    if rc:
        raise click.exceptions.Exit(rc)


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
