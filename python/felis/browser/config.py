"""Site configuration for the schema browser."""

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

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, FilePath, HttpUrl

__all__ = ["Maintainer", "SiteConfig"]


class Maintainer(BaseModel):
    """The organization maintaining the schemas, named in the page footer."""

    model_config = ConfigDict(extra="forbid")

    name: str
    """Name of the organization."""

    url: HttpUrl | None = None
    """Website of the organization, linked from its name; none by default."""


class SiteConfig(BaseModel):
    """Configuration of how a generated schema browser site looks.

    Read from a YAML file with `from_file`; every setting is optional.
    """

    model_config = ConfigDict(extra="forbid")

    title: str = "Home"
    """Site title: the first breadcrumb item, and the start of every page
    title.
    """

    stylesheets: list[FilePath] = []
    """Stylesheets applied after the built-in one, in order, so later ones
    take precedence. Each is copied into the site's ``assets`` directory.
    """

    icon: FilePath | None = None
    """Site icon (an image file), shown at the top left of every page and
    linking to the home page; none by default. It is copied into the site's
    ``assets`` directory.
    """

    repository: HttpUrl | None = None
    """URL of the repository the schemas come from, linked from the header of
    every page; no link by default.
    """

    maintainer: Maintainer | None = None
    """Organization maintaining the schemas, shown as "Maintained by" in the
    footer of every page; none by default.
    """

    show_empty_columns: bool = False
    """Show every column field in each table's columns table, including
    fields that none of the table's columns set; by default those are left
    out (``name``, ``datatype``, ``description``, ``unit`` and ``nullable``
    are always shown).
    """

    diagrams: bool = False
    """Show a relationship diagram, drawn from the foreign keys, on the page
    of each schema that has them. The diagrams are drawn in the browser by
    Mermaid, loaded from a CDN.
    """

    @classmethod
    def from_file(cls, path: Path) -> SiteConfig:
        """Read a site configuration from a YAML file.

        Parameters
        ----------
        path
            The configuration file. Relative stylesheet and icon paths in it
            are relative to the file's directory.

        Returns
        -------
        `SiteConfig`
            The configuration.

        Raises
        ------
        pydantic.ValidationError
            Raised if the file has unknown settings, values of the wrong type,
            or stylesheets or an icon that don't exist.
        """
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(data, dict) and isinstance(data.get("stylesheets"), list):
            data["stylesheets"] = [path.parent / str(stylesheet) for stylesheet in data["stylesheets"]]
        if isinstance(data, dict) and isinstance(data.get("icon"), str):
            data["icon"] = path.parent / data["icon"]
        return cls.model_validate(data)
