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
import shutil
from collections.abc import Iterable
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any

from jinja2 import Environment, PackageLoader, pass_context
from jinja2.runtime import Context
from lsst.utils.iteration import ensure_iterable

from felis.datamodel import BaseObject, Column, Schema

from .columns import _column_field, _column_tags, _table_column_fields
from .config import SiteConfig
from .diagram import _RelationshipDiagram
from .navdata import _NavData
from .schemas import _resolve_column, _schema_version, _version_order
from .sitemap import SiteMap, _relative_href

_ASSETS = ("style.css", "sidebar.js", "page.js", "search.js")
"""Static files copied into the site's ``assets`` directory."""


_NAV_DATA = "nav-data.js"
"""Generated script defining ``window.FELIS_NAV``, the sidebar navigation data
shared by all pages.
"""


_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
"""Month names for the build date in the footer, in English whatever the
locale, which ``%b`` would follow.
"""


class _TemplateFilters:
    """The template filters giving the pages access to the site map."""

    def __init__(self, site: SiteMap) -> None:
        self._site = site

    @pass_context
    def href(self, context: Context, obj: BaseObject) -> str:
        return _relative_href(context["page_path"], self._site.path(obj))

    @pass_context
    def columns(self, context: Context, refs: str | list[str]) -> list[Column]:
        return [_resolve_column(context["schema"], ref) for ref in ensure_iterable(refs)]

    def relationship_diagram(self, schema: Schema) -> str | None:
        return _RelationshipDiagram(schema).text()

    def all(self) -> dict[str, Any]:
        return {
            "href": self.href,
            "path": self._site.path,
            "anchor": self._site.anchor,
            "table_of": self._site.table_of,
            "columns": self.columns,
            "relationship_diagram": self.relationship_diagram,
            "field": _column_field,
            "column_fields": _table_column_fields,
            "version": _schema_version,
            "version_order": _version_order,
        }


def _none_as_empty(value: object) -> object:
    """Show `None` in a template as nothing, rather than "None"."""
    return "" if value is None else value


def _environment(site: SiteMap) -> Environment:
    """Return the Jinja environment, with filters giving templates the site
    map.
    """
    env = Environment(
        loader=PackageLoader("felis.browser", "templates"),
        # Every template is HTML, including page_macros.j2, whose macros
        # are compiled with its own setting, not their callers'.
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
        finalize=_none_as_empty,
    )
    env.filters.update(_TemplateFilters(site).all())
    return env


def _write_page(
    env: Environment,
    output_dir: Path,
    template_name: str,
    page_path: str,
    **context: object,
) -> None:
    """Render a template and write it to ``page_path`` under ``output_dir``."""
    target_path = output_dir / page_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    # The site root, relative to the page, such as "../.." for a table page.
    site_root = _relative_href(page_path, ".")
    html = env.get_template(template_name).render(page_path=page_path, site_root=site_root, **context)
    target_path.write_text(html, encoding="utf-8")


def _stylesheet_names(config: SiteConfig) -> dict[str, Path]:
    """Return the configured stylesheets by their numbered names in the site's
    assets.
    """
    return {f"custom-{number}-{path.name}": path for number, path in enumerate(config.stylesheets, start=1)}


def _icon_name(config: SiteConfig) -> str | None:
    return f"site-icon-{config.icon.name}" if config.icon else None


def _favicon_name(config: SiteConfig) -> str | None:
    return f"favicon{config.favicon.suffix}" if config.favicon else None


def _page_globals(config: SiteConfig) -> dict[str, Any]:
    """Return the template variables shared by every page."""
    today = datetime.now(UTC)
    return {
        "site_title": config.title,
        "site_description": config.description,
        "custom_stylesheets": list(_stylesheet_names(config)),
        "site_icon": _icon_name(config),
        "site_favicon": _favicon_name(config),
        "diagrams": config.diagrams,
        "show_empty_columns": config.show_empty_columns,
        "maintainer": config.maintainer,
        "build_date": f"{_MONTHS[today.month - 1]} {today.day}, {today.year}",
    }


def _write_assets(assets_dir: Path, site: SiteMap, config: SiteConfig) -> None:
    """Write the files shared by the pages into the site's ``assets``
    directory.
    """
    assets_dir.mkdir(parents=True, exist_ok=True)
    # The built-in files, from the package.
    for asset_name in _ASSETS:
        source = resources.files("felis.browser").joinpath(f"static/{asset_name}").read_text(encoding="utf-8")
        (assets_dir / asset_name).write_text(source, encoding="utf-8")
    # The files named in the site configuration, by their names in the site.
    config_files = _stylesheet_names(config)
    icon_name = _icon_name(config)
    if config.icon and icon_name:
        config_files[icon_name] = config.icon
    favicon_name = _favicon_name(config)
    if config.favicon and favicon_name:
        config_files[favicon_name] = config.favicon
    for asset_name, path in config_files.items():
        shutil.copyfile(path, assets_dir / asset_name)
    nav_json = json.dumps(_NavData(site).data(), separators=(",", ":"))
    (assets_dir / _NAV_DATA).write_text(f"window.FELIS_NAV = {nav_json};\n", encoding="utf-8")


def render_static_site(schemas: Iterable[Schema], output_dir: Path, config: SiteConfig | None = None) -> None:
    """Render a static browser site for browsing schemas.

    The generated pages include schema, table, and column navigation.

    Parameters
    ----------
    schemas
        Schemas to render.
    output_dir
        Directory where the rendered HTML files will be saved.
    config
        How the site looks; the defaults if not given. Its stylesheets are
        copied into the site's ``assets`` directory as ``custom-<N>-<name>``.
    """
    config = config or SiteConfig()
    site = SiteMap(schemas)
    env = _environment(site)
    env.globals.update(_page_globals(config))

    _write_page(env, output_dir, "index.html.j2", "index.html", schemas=site.schemas)
    for schema in site.schemas:
        _write_page(env, output_dir, "schema.html.j2", site.path(schema), schema=schema)
        for table in schema.tables:
            tags = _column_tags(schema, table)
            _write_page(
                env, output_dir, "table.html.j2", site.path(table), schema=schema, table=table, tags=tags
            )
    _write_assets(output_dir / "assets", site, config)
