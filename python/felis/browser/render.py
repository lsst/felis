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
from collections.abc import Iterable
from enum import StrEnum
from importlib import resources
from pathlib import Path
from typing import Any, cast

from jinja2 import Environment, PackageLoader, pass_context, select_autoescape
from jinja2.runtime import Context

from felis.datamodel import (
    CheckConstraint,
    ForeignKeyConstraint,
    Schema,
    SchemaVersion,
    UniqueConstraint,
)

logger = logging.getLogger(__name__)

_ASSETS = {"stylesheet_href": "style.css", "script_href": "sidebar.js"}
"""Static assets copied into the site's ``assets`` directory, keyed by the
template variable holding each asset's href.
"""

_NAV_DATA = "nav-data.js"
"""Generated script defining ``window.FELIS_NAV``, the sidebar navigation data
shared by all pages.
"""


class AnchorToken(StrEnum):
    """Anchor tokens used in generated HTML element IDs."""

    TABLE = "table"
    COLUMN = "column"
    INDEX = "index"
    CONSTRAINT = "constraint"
    DETAILS = "details"


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


def _schema_name(schema: Schema, source_path: str) -> str:
    """Get the display name for a schema.

    Parameters
    ----------
    schema
        The schema object.
    source_path
        The path to the schema source file, used as fallback for name.

    Returns
    -------
    str
        The schema name or the stem of the source file path if name is not set.
    """
    if schema.name:
        return schema.name
    return Path(source_path).stem


def _column_label(column_id: str) -> str:
    """Extract the column name from a column ID.

    Parameters
    ----------
    column_id
        The full column ID (e.g., "table.column" or "#column").

    Returns
    -------
    str
        The column name without leading '#' and without the table prefix.
    """
    return column_id.rsplit(".", 1)[-1].lstrip("#")


def _table_label(column_id: str) -> str:
    """Extract the table name from a column ID.

    Parameters
    ----------
    column_id
        The full column ID (e.g., "table.column" or "#table.column").

    Returns
    -------
    str
        The table name without leading '#' and without the column suffix.
    """
    return column_id.lstrip("#").split(".", 1)[0]


def _format_field_value(value: object) -> str:
    """Format a field value for display in HTML.

    Parameters
    ----------
    value
        The value to format.

    Returns
    -------
    str
        A string representation suitable for HTML display:
        - None becomes empty string
        - bool becomes "true" or "false"
        - list becomes comma-separated values
        - dict becomes string representation
        - other types are converted to string
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    if isinstance(value, dict):
        return str(value)
    return str(value)


def _relative_href(current_output_path: str, target_output_path: str) -> str:
    """Build a POSIX relative href from one output HTML path to another."""
    current_dir = Path(current_output_path).parent
    return Path(os.path.relpath(target_output_path, start=current_dir)).as_posix()


def _anchor(token: AnchorToken, name: str) -> str:
    """Build an HTML element ID from an anchor token and a name."""
    return f"{token.value}-{_slugify(name)}"


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


def _column_entries(
    column_ids: Iterable[object],
    column_targets: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    """Resolve column IDs to entries describing where each column is
    rendered, falling back to labels parsed from the ID for columns that are
    not in the schema.
    """
    entries: list[dict[str, str]] = []
    for column_id in map(str, column_ids):
        if column_id in column_targets:
            entries.append(column_targets[column_id])
        else:
            entries.append(
                {
                    "id": column_id,
                    "name": _column_label(column_id),
                    "anchor": "",
                    "table_name": _table_label(column_id),
                    "table_output_path": "",
                }
            )
    return entries


@pass_context
def _href(context: Context, target_output_path: str) -> str:
    """Jinja filter resolving a site-relative output path against the page
    being rendered, which is given by the ``page_path`` context variable.
    """
    return _relative_href(context["page_path"], target_output_path)


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
            home_href=_relative_href(page_path, "index.html"),
            nav_data_href=_relative_href(page_path, f"assets/{_NAV_DATA}"),
            **{key: _relative_href(page_path, f"assets/{name}") for key, name in _ASSETS.items()},
            **context,
        ),
        encoding="utf-8",
    )


def _nav_data(schema_pages: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the sidebar navigation data shared by all pages.

    Parameters
    ----------
    schema_pages
        The schema page entries, in display order.

    Returns
    -------
    dict
        Schemas with their tables, and each table's columns, indexes and
        constraints. Paths are relative to the site root, and descriptions
        are omitted when empty.
    """

    def item(entry: dict[str, Any], **fields: Any) -> dict[str, Any]:
        data = {"name": entry["name"], **fields}
        if entry.get("description"):
            data["description"] = entry["description"]
        return data

    def anchored(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [item(entry, anchor=entry["anchor"]) for entry in entries]

    return {
        "schemas": [
            item(
                schema,
                path=schema["output_path"],
                tables=[
                    item(
                        table,
                        path=table["output_path"],
                        anchor=table["anchor"],
                        columns=anchored(table["columns"]),
                        indexes=anchored(table["indexes"]),
                        constraints=anchored(table["constraints"]),
                    )
                    for table in schema["tables"]
                ],
            )
            for schema in schema_pages
        ]
    }


def render_static_site(
    schemas: list[Schema],
    source_paths: list[str],
    output_dir: Path,
) -> None:
    """Render a minimal static browser site for browsing schemas.

    The generated pages include schema, table, and column navigation.

    Parameters
    ----------
    schemas
        List of schemas to render.
    source_paths
        List of source paths corresponding to the schemas, used for display
        purposes.
    output_dir
        Directory where the rendered HTML files will be saved.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    env = Environment(
        loader=PackageLoader("felis.browser", "templates"),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["href"] = _href

    schema_pages: list[dict[str, object]] = []
    schema_dirs = _UniqueNames()

    for schema, source_path in zip(schemas, source_paths, strict=True):
        name = _schema_name(schema, source_path)
        logger.debug("Building schema: %s", name)
        schema_dir = f"schemas/{schema_dirs.claim(_slugify(name))}"
        schema_output_path = f"{schema_dir}/index.html"

        # First pass: build each table's columns, recording where every column
        # is rendered so that references from any table in the schema can be
        # resolved in the second pass.
        table_entries: list[dict[str, object]] = []
        column_targets: dict[str, dict[str, str]] = {}
        table_filenames = _UniqueNames()
        for table in schema.tables:
            logger.debug("Building table: %s.%s", name, table.name)
            table_output_path = f"{schema_dir}/{table_filenames.claim(_slugify(table.name))}.html"

            columns: list[dict[str, object]] = []
            for column in table.columns:
                logger.debug("Building column: %s.%s.%s", name, table.name, column.name)
                details_fields: list[dict[str, str]] = []
                summary_aliases = {
                    "name",
                    "datatype",
                    "ivoa:ucd",
                    "ivoa:unit",
                    "fits:tunit",
                    "description",
                }

                column_data = column.model_dump(by_alias=True, exclude_none=False, exclude_defaults=False)
                for key, value in column_data.items():
                    if key == "@id":
                        continue
                    if key in summary_aliases:
                        continue
                    if value is None:
                        continue
                    details_fields.append({"key": key, "display_value": _format_field_value(value)})

                has_details = len(details_fields) > 0
                column_anchor = _anchor(AnchorToken.COLUMN, column.name)
                columns.append(
                    {
                        "id": column.id,
                        "name": column.name,
                        "datatype": str(column.datatype),
                        "ucd": column.ivoa_ucd or "",
                        "description": column.description or "",
                        "unit": column.ivoa_unit or column.fits_tunit or "",
                        "anchor": column_anchor,
                        "details_anchor": (
                            f"{column_anchor}-{AnchorToken.DETAILS.value}" if has_details else ""
                        ),
                        "has_details": has_details,
                        "details_fields": details_fields,
                    }
                )
                column_targets[column.id] = {
                    "id": column.id,
                    "name": column.name,
                    "anchor": column_anchor,
                    "table_name": table.name,
                    "table_output_path": table_output_path,
                }

            table_entries.append(
                {
                    "name": table.name,
                    "description": table.description or "",
                    "anchor": _anchor(AnchorToken.TABLE, table.name),
                    "output_path": table_output_path,
                    "columns": columns,
                }
            )

        # Second pass: resolve the column references of primary keys, indexes
        # and constraints.
        for table, table_entry in zip(schema.tables, table_entries, strict=True):
            if table.primary_key is None:
                primary_key_ids = []
            elif isinstance(table.primary_key, list):
                primary_key_ids = table.primary_key
            else:
                primary_key_ids = [table.primary_key]
            table_entry["primary_key"] = _column_entries(primary_key_ids, column_targets)

            index_entries: list[dict[str, object]] = []
            index_anchors = _UniqueNames()
            for index in table.indexes:
                definition_values: list[dict[str, str]] | list[str]
                if index.columns is not None:
                    definition_kind = "columns"
                    definition_values = _column_entries(index.columns, column_targets)
                else:
                    definition_kind = "expressions"
                    definition_values = list(index.expressions or [])
                index_entries.append(
                    {
                        "name": index.name,
                        "description": index.description or "",
                        "anchor": index_anchors.claim(_anchor(AnchorToken.INDEX, index.name)),
                        "definition_kind": definition_kind,
                        "definition_values": definition_values,
                    }
                )
            table_entry["indexes"] = index_entries

            constraint_entries: list[dict[str, object]] = []
            constraint_anchors = _UniqueNames()
            for constraint in table.constraints:
                entry: dict[str, object] = {
                    "name": constraint.name,
                    "type": constraint.type,
                    "description": constraint.description or "",
                    "anchor": constraint_anchors.claim(_anchor(AnchorToken.CONSTRAINT, constraint.name)),
                    "deferrable": constraint.deferrable,
                    "initially": constraint.initially or "",
                }

                if isinstance(constraint, CheckConstraint):
                    entry["expression"] = constraint.expression
                elif isinstance(constraint, UniqueConstraint):
                    entry["columns"] = _column_entries(constraint.columns, column_targets)
                elif isinstance(constraint, ForeignKeyConstraint):
                    entry["columns"] = _column_entries(constraint.columns, column_targets)
                    entry["referenced_columns"] = _column_entries(
                        constraint.referenced_columns, column_targets
                    )
                    entry["on_delete"] = constraint.on_delete or ""
                    entry["on_update"] = constraint.on_update or ""

                constraint_entries.append(entry)
            table_entry["constraints"] = constraint_entries

        if isinstance(schema.version, str):
            version = schema.version
        elif isinstance(schema.version, SchemaVersion):
            version = schema.version.current
        else:
            version = "None"

        schema_pages.append(
            {
                "name": name,
                "description": schema.description or "",
                "version": version,
                "source_path": source_path,
                "output_path": schema_output_path,
                "tables": table_entries,
            }
        )

    schema_pages.sort(key=lambda page: str(page["name"]).lower())

    _write_page(env, output_dir, "index.html.j2", "index.html", schemas=schema_pages)

    for page in schema_pages:
        _write_page(
            env,
            output_dir,
            "schema.html.j2",
            str(page["output_path"]),
            current_schema=page,
        )
        for table_page in cast(list[dict[str, object]], page["tables"]):
            _write_page(
                env,
                output_dir,
                "table.html.j2",
                str(table_page["output_path"]),
                current_schema=page,
                current_table=table_page,
            )

    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    for asset_name in _ASSETS.values():
        source = resources.files("felis.browser").joinpath(f"static/{asset_name}").read_text(encoding="utf-8")
        (assets_dir / asset_name).write_text(source, encoding="utf-8")
    nav_json = json.dumps(_nav_data(schema_pages), separators=(",", ":"))
    (assets_dir / _NAV_DATA).write_text(f"window.FELIS_NAV = {nav_json};\n", encoding="utf-8")
