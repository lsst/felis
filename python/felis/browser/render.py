"""Browser rendering utilities for generating static HTML pages from
schemas.
"""
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

import json
import logging
import os
import re
import shutil
from collections.abc import Iterable
from importlib import resources
from pathlib import Path
from typing import Any

from jinja2 import Environment, PackageLoader, pass_context, select_autoescape
from jinja2.runtime import Context

from felis.datamodel import BaseObject, Column, Schema, SchemaVersion, Table

logger = logging.getLogger(__name__)

_ASSETS = ("style.css", "sidebar.js")
"""Static files copied into the site's ``assets`` directory."""

_NAV_DATA = "nav-data.js"
"""Generated script defining ``window.FELIS_NAV``, the sidebar navigation data
shared by all pages.
"""

_SUMMARY_FIELDS = frozenset({"@id", "name", "datatype", "ivoa:ucd", "ivoa:unit", "fits:tunit", "description"})
"""Column fields shown in the columns table, and therefore left out of the
column details.
"""


def _slugify(value: str) -> str:
    """Convert a string to a URL-safe slug.

    Parameters
    ----------
    value
        The string to convert.

    Returns
    -------
    str
        A slug with lowercase alphanumeric characters and hyphens, defaulting
        to "schema" if empty.
    """
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "schema"


def _relative_href(current_output_path: str, target_output_path: str) -> str:
    """Build a POSIX relative href from one output HTML path to another."""
    current_dir = Path(current_output_path).parent
    return Path(os.path.relpath(target_output_path, start=current_dir)).as_posix()


def _format_field_value(value: object) -> str:
    """Format a field value for display in HTML.

    Parameters
    ----------
    value
        The value to format.

    Returns
    -------
    str
        "true" or "false" for booleans, comma-separated values for lists,
        and ``str(value)`` otherwise.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def _column_details(column: Column) -> list[tuple[str, str]]:
    """Return the fields shown in a column's details, as ``(key, value)``
    pairs, leaving out empty fields and those in the columns table.
    """
    return [
        (key, _format_field_value(value))
        for key, value in column.model_dump(by_alias=True).items()
        if key not in _SUMMARY_FIELDS and value is not None
    ]


def _schema_version(schema: Schema) -> str | None:
    """Return the schema version for display, or `None` if it has none."""
    if isinstance(schema.version, SchemaVersion):
        return schema.version.current
    return schema.version


def _resolve_column(schema: Schema, ref: str) -> Column | str:
    """Resolve a column reference from a primary key, index or constraint.

    Parameters
    ----------
    schema
        The schema containing the reference.
    ref
        The column reference.

    Returns
    -------
    `Column` or `str`
        The referenced column, or ``ref`` itself if it cannot be resolved.

    Notes
    -----
    This is the only place the browser looks up columns by ID. References
    are currently column IDs; replace this with the name-based column lookup
    from DM-51789 once that is merged, as ID lookups may be removed.
    """
    if ref in schema:
        obj = schema[ref]
        if isinstance(obj, Column):
            return obj
    return ref


class _UniqueNames:
    """Hand out unique names, appending ``-2``, ``-3``, ... to repeats."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}

    def claim(self, base: str) -> str:
        """Claim a unique name derived from ``base``.

        Parameters
        ----------
        base
            The preferred name.

        Returns
        -------
        str
            ``base`` the first time it is claimed and ``base-N`` for the Nth
            repeat.
        """
        count = self._counts.get(base, 0) + 1
        self._counts[base] = count
        return base if count == 1 else f"{base}-{count}"


class SiteMap:
    """Output paths and element IDs of all objects shown in the site.

    Objects are keyed by ``id()`` because the felis models are not hashable;
    the schemas must outlive the site map.

    Parameters
    ----------
    schemas
        The schemas in the site. Paths are assigned in this order, so the
        order decides which of two same-named schemas gets a ``-2`` suffix.
    """

    def __init__(self, schemas: Iterable[Schema]) -> None:
        self._paths: dict[int, str] = {}
        self._anchors: dict[int, str] = {}
        self._tables: dict[int, Table] = {}

        schema_dirs = _UniqueNames()
        schemas = list(schemas)
        for schema in schemas:
            logger.debug("Mapping schema: %s", schema.name)
            schema_dir = f"schemas/{schema_dirs.claim(_slugify(schema.name))}"
            self._paths[id(schema)] = f"{schema_dir}/index.html"
            table_files = _UniqueNames()
            for table in schema.tables:
                self._paths[id(table)] = f"{schema_dir}/{table_files.claim(_slugify(table.name))}.html"
                self._anchors[id(table)] = f"table-{_slugify(table.name)}"
                self._add(table, "column", table.columns, _UniqueNames())
                self._add(table, "index", table.indexes, _UniqueNames())
                self._add(table, "constraint", table.constraints, _UniqueNames())

        self.schemas = sorted(schemas, key=lambda schema: schema.name.lower())
        """The schemas in display order."""

    def _add(self, table: Table, prefix: str, objects: Iterable[BaseObject], names: _UniqueNames) -> None:
        """Record the table and a unique anchor for objects shown on a table's
        page.
        """
        for obj in objects:
            self._anchors[id(obj)] = names.claim(f"{prefix}-{_slugify(obj.name)}")
            self._tables[id(obj)] = table

    def path(self, obj: BaseObject) -> str:
        """Return the location of ``obj`` within the site.

        Parameters
        ----------
        obj
            A schema or an object in one.

        Returns
        -------
        str
            The page of a schema or table, otherwise the object's anchor on
            its table's page, relative to the site root.
        """
        if id(obj) in self._paths:
            return self._paths[id(obj)]
        return f"{self._paths[id(self.table_of(obj))]}#{self.anchor(obj)}"

    def anchor(self, obj: BaseObject) -> str:
        """Return the element ID of ``obj`` on its table's page.

        Parameters
        ----------
        obj
            A schema or an object in one.

        Returns
        -------
        str
            The element ID.
        """
        return self._anchors[id(obj)]

    def table_of(self, obj: BaseObject) -> Table:
        """Return the table containing ``obj``.

        Parameters
        ----------
        obj
            A schema or an object in one.

        Returns
        -------
        `Table`
            The table whose page shows ``obj``.
        """
        return self._tables[id(obj)]

    def nav_data(self) -> dict[str, Any]:
        """Build the sidebar navigation data shared by all pages.

        Returns
        -------
        dict
            Schemas with their tables, and each table's columns, indexes and
            constraints. Paths are relative to the site root, and
            descriptions are omitted when empty.
        """

        def item(obj: BaseObject, **fields: Any) -> dict[str, Any]:
            data = {"name": obj.name, **fields}
            if obj.description:
                data["description"] = obj.description
            return data

        def anchored(objects: Iterable[BaseObject]) -> list[dict[str, Any]]:
            return [item(obj, anchor=self.anchor(obj)) for obj in objects]

        return {
            "schemas": [
                item(
                    schema,
                    path=self.path(schema),
                    tables=[
                        item(
                            table,
                            path=self.path(table),
                            anchor=self.anchor(table),
                            columns=anchored(table.columns),
                            indexes=anchored(table.indexes),
                            constraints=anchored(table.constraints),
                        )
                        for table in schema.tables
                    ],
                )
                for schema in self.schemas
            ]
        }


def _environment(site: SiteMap) -> Environment:
    """Create the Jinja environment, with filters giving templates access to
    the site map.

    Parameters
    ----------
    site
        The site being rendered.

    Returns
    -------
    `jinja2.Environment`
        The environment used to render all pages.
    """
    env = Environment(
        loader=PackageLoader("felis.browser", "templates"),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
        finalize=lambda value: "" if value is None else value,
    )

    @pass_context
    def href(context: Context, obj: BaseObject) -> str:
        # Link to obj from the page being rendered.
        return _relative_href(context["page_path"], site.path(obj))

    @pass_context
    def columns(context: Context, refs: str | list[str]) -> list[Column | str]:
        # Resolve one or more column references in the current schema.
        return [
            _resolve_column(context["schema"], ref) for ref in ([refs] if isinstance(refs, str) else refs)
        ]

    env.filters.update(
        href=href,
        path=site.path,
        anchor=site.anchor,
        table_of=site.table_of,
        columns=columns,
        details=_column_details,
        version=_schema_version,
    )
    return env


def _write_page(
    env: Environment,
    output_dir: Path,
    template_name: str,
    page_path: str,
    **context: object,
) -> None:
    """Render a template and write it to ``page_path`` under ``output_dir``.

    Parameters
    ----------
    env
        The Jinja environment used to load the template.
    output_dir
        Root directory of the generated site.
    template_name
        Name of the template to render.
    page_path
        Output path of the page, relative to ``output_dir``.
    **context
        Additional template context.
    """
    target_path = output_dir / page_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        env.get_template(template_name).render(
            page_path=page_path,
            site_root=Path(os.path.relpath(".", start=Path(page_path).parent)).as_posix(),
            **context,
        ),
        encoding="utf-8",
    )


def render_static_site(schemas: Iterable[Schema], output_dir: Path, stylesheets: Iterable[Path] = ()) -> None:
    """Render a static browser site for browsing schemas.

    The generated pages include schema, table, and column navigation.

    Parameters
    ----------
    schemas
        Schemas to render.
    output_dir
        Directory where the rendered HTML files will be saved.
    stylesheets
        Additional stylesheets, applied in order after the built-in one, so
        later stylesheets take precedence. Each is copied into the site's
        ``assets`` directory as ``custom-<N>-<name>``.
    """
    site = SiteMap(schemas)
    env = _environment(site)
    custom_stylesheets = {
        f"custom-{number}-{path.name}": path for number, path in enumerate(stylesheets, start=1)
    }
    env.globals["custom_stylesheets"] = list(custom_stylesheets)

    _write_page(env, output_dir, "index.html.j2", "index.html", schemas=site.schemas)
    for schema in site.schemas:
        _write_page(env, output_dir, "schema.html.j2", site.path(schema), schema=schema)
        for table in schema.tables:
            _write_page(env, output_dir, "table.html.j2", site.path(table), schema=schema, table=table)

    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    for asset_name in _ASSETS:
        source = resources.files("felis.browser").joinpath(f"static/{asset_name}").read_text(encoding="utf-8")
        (assets_dir / asset_name).write_text(source, encoding="utf-8")
    for asset_name, path in custom_stylesheets.items():
        shutil.copyfile(path, assets_dir / asset_name)
    nav_json = json.dumps(site.nav_data(), separators=(",", ":"))
    (assets_dir / _NAV_DATA).write_text(f"window.FELIS_NAV = {nav_json};\n", encoding="utf-8")
