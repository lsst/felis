# LSST Data Management System
# Copyright 2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
import json

import click
import yaml
from pyld import jsonld
from sqlalchemy import create_engine

from .model import Visitor
from .tap import TapLoadingVisitor, Tap11Base
from .utils import ReorderingVisitor
from . import __version__, DEFAULT_CONTEXT, DEFAULT_FRAME


@click.group()
@click.version_option(__version__)
def cli():
    """Felis Command Line Tools"""
    ...


@cli.command("create-all")
@click.option('--engine-url', envvar="ENGINE_URL", help="SQLAlchemy Engine URL")
@click.option('--schema-name', help="Alternate Schema Name for Felis File")
@click.option('--dry-run', is_flag=True, help="Dry Run Only. Prints out the DDL that would be "
                                              "executed")
@click.argument('file', type=click.File())
def create_all(engine_url, schema_name, dry_run, file):
    """Create schema objects from the Felis FILE."""

    schema_obj = yaml.load(file)
    visitor = Visitor(schema_name=schema_name)
    schema = visitor.visit_schema(schema_obj)

    metadata = schema.metadata

    def metadata_dump(sql, *multiparams, **params):
        # print or write to log or file etc
        print(sql.compile(dialect=engine.dialect))

    if not dry_run:
        engine = create_engine(engine_url)
    else:
        _insert_dump = InsertDump()
        engine = create_engine(engine_url, strategy="mock", executor=_insert_dump.dump)
        _insert_dump.dialect = engine.dialect
    metadata.create_all(engine)


@cli.command("init-tap")
@click.argument('engine-url')
def init_tap(engine_url):
    """Initialize TAP 1.1 TAP_SCHEMA objects.
    Please verify the schema/catalog you are executing this in in your
    engine URL."""
    engine = create_engine(engine_url, echo=True)
    Tap11Base.metadata.create_all(engine)


@cli.command("load-tap")
@click.option('--engine-url', envvar="ENGINE_URL", help="SQLAlchemy Engine URL to catalog")
@click.option('--schema-name', help="Alternate Schema Name for Felis file")
@click.option('--catalog-name', help="Catalog Name for Schema")
@click.option('--dry-run', is_flag=True, help="Dry Run Only. Prints out the DDL that would be "
                                              "executed")
@click.argument('file', type=click.File())
def load_tap(engine_url, schema_name, catalog_name, dry_run, file):
    """Load TAP metadata from a Felis FILE.
    This command loads the associated TAP metadata from a Felis FILE
    to the TAP_SCHEMA tables."""
    schema_obj = yaml.load(file)

    if not dry_run:
        engine = create_engine(engine_url)
    else:
        _insert_dump = InsertDump()
        engine = create_engine(engine_url, strategy="mock", executor=_insert_dump.dump,
                               paramstyle="pyformat")
        # After the engine is created, update the executor with the dialect
        _insert_dump.dialect = engine.dialect

    if engine_url == "sqlite://" and not dry_run:
        # In Memory SQLite - Mostly used to test
        Tap11Base.metadata.create_all(engine)

    tap_visitor = TapLoadingVisitor(engine, catalog_name=catalog_name, schema_name=schema_name,
                                    mock=dry_run)
    tap_visitor.visit_schema(schema_obj)


@cli.command("normalize")
@click.argument('file', type=click.File())
def normalize(file):
    """Normalize a Felis FILE."""
    schema_obj = yaml.load(file)
    schema_obj["@type"] = "felis:Schema"
    # Force Context and Schema Type
    schema_obj["@context"] = DEFAULT_CONTEXT
    expanded = jsonld.expand(schema_obj)
    normalized = _normalize(expanded)
    _dump(normalized)


@cli.command("merge")
@click.argument('files', nargs=-1, type=click.File())
def merge(files):
    """Merge a set of Feils FILES"""
    graph = []
    for file in files:
        schema_obj = yaml.load(file)
        schema_obj["@type"] = "felis:Schema"
        # Force Context and Schema Type
        schema_obj["@context"] = DEFAULT_CONTEXT
        expanded = jsonld.expand(schema_obj)
        graph.extend(expanded)

    merged = {"@context": DEFAULT_CONTEXT,
              "@graph": graph}
    normalized = _normalize(merged)
    _dump(normalized)


def _dump(obj):
    class OrderedDumper(yaml.Dumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items())

    OrderedDumper.add_representer(dict, _dict_representer)
    print(yaml.dump(obj, Dumper=OrderedDumper, default_flow_style=False))


def _normalize(schema_obj):
    framed = jsonld.frame(schema_obj, DEFAULT_FRAME)
    compacted = jsonld.compact(framed, DEFAULT_CONTEXT, options=dict(graph=True))
    graph = compacted["@graph"]
    graph = [ReorderingVisitor(add_type=True).visit_schema(schema_obj) for schema_obj in graph]
    compacted["@graph"] = graph if len(graph) > 1 else graph[0]
    return compacted


class InsertDump:
    """An Insert Dumper for SQL statements"""

    def dump(self, sql, *multiparams, **params):
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
                    new_params[key] = 'null'
                else:
                    new_params[key] = value

            print(sql_str % new_params)


if __name__ == '__main__':
    cli()
