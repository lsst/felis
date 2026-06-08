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

import logging
import os
import re
from enum import StrEnum
from importlib import resources
from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from felis.datamodel import (
    CheckConstraint,
    ForeignKeyConstraint,
    Schema,
    SchemaVersion,
    UniqueConstraint,
)

logger = logging.getLogger(__name__)


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


def _column_entry(
    column_id: str,
    schema_column_targets: dict[str, dict[str, str]],
    column_anchors: dict[str, str],
) -> dict[str, str]:
    """Build a column entry dict, resolving cross-schema references when
    available.
    """
    if column_id in schema_column_targets:
        return dict(schema_column_targets[column_id])
    return {
        "id": column_id,
        "name": _column_label(column_id),
        "anchor": column_anchors.get(column_id, ""),
        "table_name": _table_label(column_id),
        "table_page_filename": "",
    }


def _build_page_context(
    schema_pages: list[dict[str, object]],
    output_path: str,
    current_page_output_path: str,
) -> tuple[list[dict[str, object]], dict[str, object] | None]:
    """Build schema context for a page with relative hrefs.

    Parameters
    ----------
    schema_pages
        List of all schema page dicts to build context from.
    output_path
        The output path of the current page (e.g., "index.html").
    current_page_output_path
        The output path of the page being rendered (to identify current
        context).

    Returns
    -------
    tuple
        (context_pages list, current_schema_context dict or None)
    """
    context_pages: list[dict[str, object]] = []
    current_schema_ctx: dict[str, object] | None = None

    for schema in schema_pages:
        schema_copy = dict(schema)
        schema_copy["filename"] = _relative_href(output_path, str(schema["output_path"]))
        table_copies: list[dict[str, object]] = []
        for table in schema["tables"]:
            table_copy = dict(table)
            table_copy["page_filename"] = _relative_href(output_path, str(table["output_path"]))
            table_copies.append(table_copy)
        schema_copy["tables"] = table_copies
        context_pages.append(schema_copy)
        if schema["output_path"] == current_page_output_path:
            current_schema_ctx = schema_copy

    return context_pages, current_schema_ctx


def _build_table_page_context(
    schema_pages: list[dict[str, object]],
    output_path: str,
    current_page_output_path: str,
    current_table_output_path: str,
) -> tuple[list[dict[str, object]], dict[str, object] | None, dict[str, object] | None]:
    """Build table page context with current schema and table contexts.

    Parameters
    ----------
    schema_pages
        List of all schema page dicts to build context from.
    output_path
        The output path of the current page.
    current_page_output_path
        The output path of the schema being rendered.
    current_table_output_path
        The output path of the table being rendered.

    Returns
    -------
    tuple
        (context_pages list, current_schema_context dict or None,
        current_table_context dict or None)
    """
    context_pages: list[dict[str, object]] = []
    current_schema_ctx: dict[str, object] | None = None
    current_table_ctx: dict[str, object] | None = None

    for schema in schema_pages:
        schema_copy = dict(schema)
        schema_copy["filename"] = _relative_href(output_path, str(schema["output_path"]))
        table_copies: list[dict[str, object]] = []
        for table_entry in schema["tables"]:
            table_copy = dict(table_entry)
            table_copy["page_filename"] = _relative_href(output_path, str(table_entry["output_path"]))
            table_copies.append(table_copy)
            if table_entry["output_path"] == current_table_output_path:
                current_table_ctx = table_copy
        schema_copy["tables"] = table_copies
        context_pages.append(schema_copy)
        if schema["output_path"] == current_page_output_path:
            current_schema_ctx = schema_copy

    return context_pages, current_schema_ctx, current_table_ctx


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
    (output_dir / "assets").mkdir(parents=True, exist_ok=True)

    env = Environment(
        loader=PackageLoader("felis.browser", "templates"),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    schema_pages: list[dict[str, object]] = []
    schema_dir_counts: dict[str, int] = {}

    for schema, source_path in zip(schemas, source_paths, strict=True):
        name = _schema_name(schema, source_path)
        logger.debug("Building schema: %s", name)
        schema_slug = _slugify(name)
        count = schema_dir_counts.get(schema_slug, 0) + 1
        schema_dir_counts[schema_slug] = count
        schema_dir = f"schemas/{schema_slug}" if count == 1 else f"schemas/{schema_slug}-{count}"
        schema_output_path = f"{schema_dir}/index.html"

        prepared_tables: list[dict[str, object]] = []
        table_filename_counts: dict[str, int] = {}
        for table in schema.tables:
            logger.debug("Building table: %s.%s", name, table.name)
            table_anchor = f"{AnchorToken.TABLE.value}-{_slugify(table.name)}"
            table_base = _slugify(table.name)
            table_count = table_filename_counts.get(table_base, 0) + 1
            table_filename_counts[table_base] = table_count
            table_filename = f"{table_base}.html" if table_count == 1 else f"{table_base}-{table_count}.html"
            table_output_path = f"{schema_dir}/{table_filename}"

            columns: list[dict[str, object]] = []
            for column in table.columns:
                logger.debug("Building column: %s.%s.%s", name, table.name, column.name)
                col_slug = _slugify(column.name)
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
                columns.append(
                    {
                        "id": column.id,
                        "name": column.name,
                        "datatype": str(column.datatype),
                        "ucd": column.ivoa_ucd or "",
                        "description": column.description or "",
                        "unit": column.ivoa_unit or column.fits_tunit or "",
                        "anchor": f"{AnchorToken.COLUMN.value}-{col_slug}",
                        "details_anchor": (
                            f"{AnchorToken.COLUMN.value}-{col_slug}-{AnchorToken.DETAILS.value}"
                            if has_details
                            else ""
                        ),
                        "has_details": has_details,
                        "details_fields": details_fields,
                    }
                )

            prepared_tables.append(
                {
                    "table": table,
                    "anchor": table_anchor,
                    "output_path": table_output_path,
                    "columns": columns,
                }
            )

        schema_column_targets: dict[str, dict[str, str]] = {}
        for prepared in prepared_tables:
            table_name = str(getattr(prepared["table"], "name"))
            table_page_filename = str(prepared["output_path"])
            for column in prepared["columns"]:
                column_id = str(column["id"])
                schema_column_targets[column_id] = {
                    "id": column_id,
                    "name": str(column["name"]),
                    "anchor": str(column["anchor"]),
                    "table_name": table_name,
                    "table_page_filename": table_page_filename,
                }

        table_entries: list[dict[str, object]] = []
        for prepared in prepared_tables:
            table = prepared["table"]
            table_anchor = str(prepared["anchor"])
            table_filename = str(prepared["output_path"])
            columns: list[dict[str, object]] = prepared["columns"]
            column_anchors = {str(column["id"]): str(column["anchor"]) for column in columns}

            primary_key_entries: list[dict[str, str]] = []
            if table.primary_key is not None:
                primary_key_columns = (
                    table.primary_key if isinstance(table.primary_key, list) else [table.primary_key]
                )
                for column_id in primary_key_columns:
                    column_id_str = str(column_id)
                    primary_key_entries.append(
                        _column_entry(column_id_str, schema_column_targets, column_anchors)
                    )

            index_entries: list[dict[str, object]] = []
            index_filename_counts: dict[str, int] = {}
            for index in table.indexes:
                index_base = f"{AnchorToken.INDEX.value}-{_slugify(index.name)}"
                index_count = index_filename_counts.get(index_base, 0) + 1
                index_filename_counts[index_base] = index_count
                index_anchor = f"{index_base}" if index_count == 1 else f"{index_base}-{index_count}"
                if index.columns is not None:
                    definition_kind = "columns"
                    definition_values = [
                        _column_entry(str(column_id), schema_column_targets, column_anchors)
                        for column_id in index.columns
                    ]
                else:
                    definition_kind = "expressions"
                    definition_values = list(index.expressions or [])
                index_entries.append(
                    {
                        "name": index.name,
                        "description": index.description or "",
                        "anchor": index_anchor,
                        "definition_kind": definition_kind,
                        "definition_values": definition_values,
                    }
                )

            constraint_entries: list[dict[str, object]] = []
            constraint_filename_counts: dict[str, int] = {}
            for constraint in table.constraints:
                constraint_base = f"{AnchorToken.CONSTRAINT.value}-{_slugify(constraint.name)}"
                constraint_count = constraint_filename_counts.get(constraint_base, 0) + 1
                constraint_filename_counts[constraint_base] = constraint_count
                constraint_anchor = (
                    f"{constraint_base}" if constraint_count == 1 else f"{constraint_base}-{constraint_count}"
                )
                entry: dict[str, object] = {
                    "name": constraint.name,
                    "type": constraint.type,
                    "description": constraint.description or "",
                    "anchor": constraint_anchor,
                    "deferrable": constraint.deferrable,
                    "initially": constraint.initially or "",
                }

                if isinstance(constraint, CheckConstraint):
                    entry["expression"] = constraint.expression
                elif isinstance(constraint, UniqueConstraint):
                    entry["columns"] = [
                        _column_entry(str(column_id), schema_column_targets, column_anchors)
                        for column_id in constraint.columns
                    ]
                elif isinstance(constraint, ForeignKeyConstraint):
                    entry["columns"] = [
                        _column_entry(str(column_id), schema_column_targets, column_anchors)
                        for column_id in constraint.columns
                    ]
                    entry["referenced_columns"] = [
                        _column_entry(str(column_id), schema_column_targets, column_anchors)
                        for column_id in constraint.referenced_columns
                    ]
                    entry["on_delete"] = constraint.on_delete or ""
                    entry["on_update"] = constraint.on_update or ""

                constraint_entries.append(entry)

            table_entries.append(
                {
                    "name": table.name,
                    "description": table.description or "",
                    "anchor": table_anchor,
                    "output_path": table_filename,
                    "primary_key": primary_key_entries,
                    "columns": columns,
                    "indexes": index_entries,
                    "constraints": constraint_entries,
                }
            )

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

    index_template = env.get_template("index.html.j2")
    schema_template = env.get_template("schema.html.j2")
    table_template = env.get_template("table.html.j2")

    index_output_path = "index.html"
    index_schemas, _ = _build_page_context(schema_pages, index_output_path, index_output_path)

    (output_dir / index_output_path).write_text(
        index_template.render(
            schemas=index_schemas,
            stylesheet_href="assets/style.css",
            script_href="assets/sidebar.js",
        ),
        encoding="utf-8",
    )

    for page in schema_pages:
        schema_output = str(page["output_path"])
        schema_context_pages, current_schema_ctx = _build_page_context(
            schema_pages, schema_output, schema_output
        )

        if current_schema_ctx is None:
            raise ValueError(f"Schema context not found for output path: {schema_output}")

        schema_target_path = output_dir / schema_output
        schema_target_path.parent.mkdir(parents=True, exist_ok=True)
        schema_target_path.write_text(
            schema_template.render(
                current_schema=current_schema_ctx,
                schemas=schema_context_pages,
                home_href=_relative_href(schema_output, "index.html"),
                stylesheet_href=_relative_href(schema_output, "assets/style.css"),
                script_href=_relative_href(schema_output, "assets/sidebar.js"),
            ),
            encoding="utf-8",
        )

        for table in page["tables"]:
            table_output = str(table["output_path"])
            table_context_pages, current_schema_table_ctx, current_table_ctx = _build_table_page_context(
                schema_pages, table_output, str(page["output_path"]), table_output
            )

            if current_schema_table_ctx is None:
                raise ValueError(f"Schema context not found for table output path: {table_output}")
            if current_table_ctx is None:
                raise ValueError(f"Table context not found for table output path: {table_output}")

            table_target_path = output_dir / table_output
            table_target_path.parent.mkdir(parents=True, exist_ok=True)
            table_target_path.write_text(
                table_template.render(
                    current_schema=current_schema_table_ctx,
                    current_table=current_table_ctx,
                    schemas=table_context_pages,
                    home_href=_relative_href(table_output, "index.html"),
                    stylesheet_href=_relative_href(table_output, "assets/style.css"),
                    script_href=_relative_href(table_output, "assets/sidebar.js"),
                ),
                encoding="utf-8",
            )

    css_source = resources.files("felis.browser").joinpath("static/style.css").read_text(encoding="utf-8")
    (output_dir / "assets" / "style.css").write_text(css_source, encoding="utf-8")
    js_source = resources.files("felis.browser").joinpath("static/sidebar.js").read_text(encoding="utf-8")
    (output_dir / "assets" / "sidebar.js").write_text(js_source, encoding="utf-8")
