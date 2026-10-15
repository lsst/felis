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

import json
import os
import tempfile
import unittest
from pathlib import Path

from bs4 import BeautifulSoup
from bs4.element import Tag
from pydantic import ValidationError

from felis import __version__
from felis.browser import Maintainer, SiteConfig, render_static_site
from felis.datamodel import Schema

TEST_DIR = os.path.abspath(os.path.dirname(__file__))


def render_browser_site(
    output_dir: str,
    files: list[str],
    config: SiteConfig | None = None,
) -> None:
    """Render browser pages from schema files using renderer directly."""
    paths = sorted(files)
    for path in paths:
        if os.path.splitext(path)[1].lower() not in {".yaml", ".yml"}:
            raise ValueError(f"Input file must be .yaml or .yml: {path}")

    schemas = [
        Schema.from_uri(path, context={"id_generation": True, "column_ref_index_increment": None})
        for path in paths
    ]
    render_static_site(
        schemas=schemas,
        output_dir=Path(output_dir),
        config=config,
    )


def parse_html(content: str) -> BeautifulSoup:
    """Parse rendered HTML for structural assertions in tests."""
    return BeautifulSoup(content, "html.parser")


def get_page_html_by_title(output_dir: str, title: str) -> str:
    """Return rendered page HTML whose title exactly matches ``title``."""
    for root, _, files in os.walk(output_dir):
        for name in files:
            if not name.endswith(".html"):
                continue
            page_path = os.path.join(root, name)
            with open(page_path, encoding="utf-8") as f:
                page_html = f.read()
            page_soup = parse_html(page_html)
            page_title = page_soup.find("title")
            if page_title is not None and page_title.get_text(strip=True) == title:
                return page_html
    raise AssertionError(f"Rendered page with title '{title}' was not found")


def get_page_soup_by_title(output_dir: str, title: str) -> BeautifulSoup:
    """Return a parsed HTML tree for a rendered page title."""
    return parse_html(get_page_html_by_title(output_dir, title))


def load_nav(output_dir: str) -> dict:
    """Load the sidebar navigation data from the generated ``nav-data.js``."""
    with open(os.path.join(output_dir, "assets", "nav-data.js"), encoding="utf-8") as f:
        script = f.read()
    prefix = "window.FELIS_NAV = "
    if not script.startswith(prefix) or not script.endswith(";\n"):
        raise AssertionError("Unexpected nav-data.js format")
    return json.loads(script[len(prefix) : -2])


def find_link(container: BeautifulSoup | Tag, href: str, title: str | None = None) -> Tag | None:
    """Find a link by href and optional title."""
    attrs: dict[str, str] = {"href": href}
    if title is not None:
        attrs["title"] = title
    return container.find("a", attrs=attrs)


class BrowserTestCase(unittest.TestCase):
    """Tests for the static browser command."""

    def test_browser_renders_table_features(self) -> None:
        """Render table/browser features and validate key structure/content.

        This test covers navigation controls, section wiring, column fields,
        and index/constraint rendering (including FK references).
        """
        schema_yaml = """
name: demo
"@id": "#demo"
tables:
  - name: ParentTable
    "@id": "#ParentTable"
    description: Parent rows.
    columns:
      - name: id
        "@id": "#ParentTable.id"
        datatype: int
        description: Parent key.
    primaryKey: "#ParentTable.id"

  - name: ChildTable
    "@id": "#ChildTable"
    description: Child rows.
    columns:
      - name: id
        "@id": "#ChildTable.id"
        datatype: int
        description: Child key.
      - name: code
        "@id": "#ChildTable.code"
        datatype: int
        "ivoa:ucd": "meta.code"
        "fits:tunit": "deg"
        "tap:std": 0
        description: Business code.
      - name: parent_id
        "@id": "#ChildTable.parent_id"
        datatype: int
        description: Parent link.
      - name: amount
        "@id": "#ChildTable.amount"
        datatype: int
        "ivoa:unit": "m"
        description: Positive amount.
    primaryKey: "#ChildTable.id"
    indexes:
      - name: IDX_ChildTable_code
        "@id": "#IDX_ChildTable_code"
        description: Index on code.
        columns:
          - "#ChildTable.code"
    constraints:
      - name: CK_ChildTable_amount_positive
        "@id": "#CK_ChildTable_amount_positive"
        "@type": Check
        description: Amount must be positive.
        expression: "amount > 0"
      - name: UQ_ChildTable_code_parent
        "@id": "#UQ_ChildTable_code_parent"
        "@type": Unique
        description: Unique code per parent.
        columns:
          - "#ChildTable.code"
          - "#ChildTable.parent_id"
      - name: FK_ChildTable_ParentTable
        "@id": "#FK_ChildTable_ParentTable"
        "@type": ForeignKey
        description: Child must reference a parent.
        columns:
          - "#ChildTable.parent_id"
        referencedColumns:
          - "#ParentTable.id"
        on_delete: CASCADE
        on_update: NO ACTION
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "constraints.yaml")
            output_dir = os.path.join(tmpdir, "browser")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)

            render_browser_site(output_dir=output_dir, files=[schema_path])

            self.assertTrue(os.path.exists(os.path.join(output_dir, "index.html")))
            schema_soup = get_page_soup_by_title(output_dir, "Home - demo")
            child_table_soup = get_page_soup_by_title(output_dir, "Home - demo - ChildTable")

            # Validate base breadcrumb navigation.
            self.assertIsNotNone(find_link(schema_soup, "../../index.html"))
            self.assertIsNotNone(find_link(child_table_soup, "../../index.html"))

            # Validate the sidebar navigation data shared by all pages.
            nav = load_nav(output_dir)
            self.assertEqual([schema["name"] for schema in nav["schemas"]], ["demo"])
            demo = nav["schemas"][0]
            self.assertEqual(demo["path"], "schemas/demo/index.html")
            self.assertNotIn("description", demo)
            parent, child = demo["tables"]
            self.assertEqual(
                parent,
                {
                    "name": "ParentTable",
                    "description": "Parent rows.",
                    "path": "schemas/demo/parenttable.html",
                    "anchor": "table-parenttable",
                    "columns": [{"name": "id", "description": "Parent key.", "anchor": "column-id"}],
                    "indexes": [],
                    "constraints": [],
                },
            )
            self.assertEqual(child["path"], "schemas/demo/childtable.html")
            self.assertEqual(child["description"], "Child rows.")
            self.assertEqual(
                [(column["name"], column["anchor"]) for column in child["columns"]],
                [
                    ("id", "column-id"),
                    ("code", "column-code"),
                    ("parent_id", "column-parent-id"),
                    ("amount", "column-amount"),
                ],
            )
            self.assertEqual(child["columns"][1]["description"], "Business code.")
            self.assertEqual(
                child["indexes"],
                [
                    {
                        "name": "IDX_ChildTable_code",
                        "description": "Index on code.",
                        "anchor": "index-idx-childtable-code",
                    }
                ],
            )
            self.assertEqual(
                [(constraint["name"], constraint["anchor"]) for constraint in child["constraints"]],
                [
                    ("CK_ChildTable_amount_positive", "constraint-ck-childtable-amount-positive"),
                    ("UQ_ChildTable_code_parent", "constraint-uq-childtable-code-parent"),
                    ("FK_ChildTable_ParentTable", "constraint-fk-childtable-parenttable"),
                ],
            )
            self.assertEqual(child["constraints"][1]["description"], "Unique code per parent.")

            # Validate the information each page gives the sidebar script.
            index_soup = get_page_soup_by_title(output_dir, "Home")
            for soup, site_root, schema_path, table_path in (
                (index_soup, ".", None, None),
                (schema_soup, "../..", "schemas/demo/index.html", None),
                (child_table_soup, "../..", "schemas/demo/index.html", "schemas/demo/childtable.html"),
            ):
                body = soup.find("body")
                assert body is not None
                self.assertEqual(body.get("data-site-root"), site_root)
                self.assertEqual(body.get("data-schema-path"), schema_path)
                self.assertEqual(body.get("data-table-path"), table_path)
                self.assertIsNotNone(soup.find("nav", id="sidebar-tree"))
                breadcrumb = soup.find("nav", class_="breadcrumb")
                assert breadcrumb is not None
                home = breadcrumb.find(["a", "span"])
                assert home is not None
                self.assertEqual(home.get_text(strip=True), "Home")
                if table_path or schema_path:
                    self.assertEqual(home.get("href"), "../../index.html")
                self.assertIsNone(soup.find(class_="site-icon"))
                self.assertIsNone(soup.find(class_="repository-link"))
                footer = soup.find("footer", class_="site-footer")
                assert footer is not None
                self.assertIn(f"Felis version: {__version__}", footer.get_text())
                self.assertNotIn("Maintained by", footer.get_text())
                top_button = soup.find("button", id="scroll-to-top")
                self.assertIsNotNone(top_button)
                assert top_button is not None
                self.assertEqual(top_button.get_text(strip=True), "Top")
                scripts = [str(script.get("src")).rsplit("/", 1)[-1] for script in soup.find_all("script")]
                # DataTables on table pages (with Buttons for CSV export) and
                # schema pages (sorting only), but not the home page.
                datatables = ["dataTables.min.js"] if schema_path else []
                if table_path:
                    datatables.append("dataTables.buttons.min.js")
                self.assertEqual(scripts, [*datatables, "nav-data.js", "sidebar.js"])
                for script in soup.find_all("script", src=lambda src: "cdn.datatables.net" in src):
                    self.assertTrue(str(script.get("integrity")).startswith("sha384-"))
                    self.assertEqual(script.get("crossorigin"), "anonymous")

            # A schema without a version or description shows neither.
            self.assertIsNone(schema_soup.find(class_="schema-version"))
            self.assertIsNone(schema_soup.find(class_="schema-description"))

            # The indexes table has no Type column.
            indexes_heading = child_table_soup.find(id="table-childtable-indexes")
            assert indexes_heading is not None
            indexes_table = indexes_heading.find_next("table")
            assert indexes_table is not None
            self.assertEqual(
                [th.get_text(strip=True) for th in indexes_table.find_all("th")],
                ["Index", "Definition", "Description"],
            )

            # Validate the columns table content.
            # Columns that are part of the primary key, a foreign key or an
            # index are tagged after their name (no Primary Key section).
            self.assertIsNone(child_table_soup.find(id="table-childtable-primary-key"))
            expected_tags = {
                "column-id": [("primary-key", "primary key", "Part of the table's primary key")],
                "column-code": [("index", "indexed", "In index IDX_ChildTable_code")],
                "column-parent-id": [("foreign-key", "foreign key", "References ParentTable.id")],
                "column-amount": [],
            }
            for column_id, tags in expected_tags.items():
                row = child_table_soup.find("tr", id=column_id)
                assert row is not None
                self.assertEqual(
                    [
                        (
                            next(c for c in tag["class"] if c.startswith("column-tag-"))[
                                len("column-tag-") :
                            ],
                            tag.get_text(strip=True),
                            tag.get("title"),
                        )
                        for tag in row.find_all(class_="column-tag")
                    ],
                    tags,
                )
            self.assertIsNotNone(child_table_soup.find(string="IDX_ChildTable_code"))
            self.assertIsNotNone(child_table_soup.find("td", string="meta.code"))

            # The column fields set by any of the table's columns are table
            # columns, in a fixed order, blank where a column has no value;
            # name, datatype, description, unit and nullable are always shown.
            columns_table = child_table_soup.find("table", class_="columns-table")
            assert columns_table is not None
            headers = [th.get_text(strip=True) for th in columns_table.find_all("th")]
            self.assertEqual(
                headers,
                [
                    "name",
                    "datatype",
                    "description",
                    "unit",
                    "nullable",
                    "ivoa:ucd",
                    "tap:principal",
                    "tap:std",
                ],
            )
            # DataTables sorts and exports the name cells by their plain name,
            # leaving out the primary key tag, and names the CSV file after
            # the table.
            self.assertEqual(columns_table.get("data-export-name"), "demo.ChildTable")
            id_cell = child_table_soup.find("tr", id="column-id").find("td", attrs={"data-field": "name"})
            self.assertEqual(id_cell.get("data-order"), "id")
            self.assertIsNotNone(id_cell.find(class_="column-tag-primary-key"))

            code_row = child_table_soup.find("tr", id="column-code")
            assert code_row is not None
            cells = dict(
                zip(headers, (td.get_text(strip=True) for td in code_row.find_all("td")), strict=True)
            )
            # The name cell holds the name, followed by any tags.
            self.assertEqual(cells["name"], "code" + "indexed")
            name_cell = code_row.find("td", attrs={"data-field": "name"})
            assert name_cell is not None
            self.assertEqual(str(name_cell.contents[0]).strip(), "code")
            self.assertEqual(cells["datatype"], "int")
            self.assertEqual(cells["ivoa:ucd"], "meta.code")
            # The unit column shows either ivoa:unit or fits:tunit.
            self.assertEqual(cells["unit"], "deg")
            amount_row = child_table_soup.find("tr", id="column-amount")
            assert amount_row is not None
            amount_unit = amount_row.find("td", attrs={"data-field": "unit"})
            assert amount_unit is not None
            self.assertEqual(amount_unit.get_text(strip=True), "m")
            self.assertEqual(cells["tap:std"], "0")
            self.assertEqual(cells["nullable"], "true")

            # There are no expandable details rows or controls.
            self.assertIsNone(child_table_soup.find(class_="column-details-row"))
            self.assertIsNone(child_table_soup.find(attrs={"data-column-details-action": True}))

            self.assertIsNotNone(find_link(child_table_soup, "#column-code"))
            self.assertIsNotNone(find_link(child_table_soup, "#column-parent-id"))
            self.assertIsNone(child_table_soup.find(string="#ChildTable.code"))

            # Validate constraints section and cross-table FK references.
            self.assertIsNotNone(child_table_soup.find(id="table-childtable-constraints"))
            self.assertIsNotNone(child_table_soup.find(string="CK_ChildTable_amount_positive"))
            self.assertIsNotNone(child_table_soup.find("code", string="amount > 0"))
            self.assertIsNotNone(child_table_soup.find(string="UQ_ChildTable_code_parent"))
            self.assertIsNotNone(find_link(child_table_soup, "parenttable.html"))
            self.assertIsNotNone(
                find_link(
                    child_table_soup,
                    "parenttable.html#column-id",
                )
            )

            fk_row = child_table_soup.find(id="constraint-fk-childtable-parenttable")
            self.assertIsNotNone(fk_row)
            if fk_row is not None:
                # The FK "References" links point to ParentTable's page, which
                # is in the same directory as ChildTable's page, so they must
                # be plain relative filenames, not paths from the site root
                # like "schemas/demo/parenttable.html".
                references = next(div for div in fk_row.find_all("div") if "References:" in div.get_text())
                self.assertEqual(
                    [link["href"] for link in references.find_all("a")],
                    ["parenttable.html", "parenttable.html#column-id"],
                )
                fk_text = fk_row.get_text(" ", strip=True)
                self.assertIn("On Delete:", fk_text)
                self.assertIn("CASCADE", fk_text)
                self.assertIn("On Update:", fk_text)
                self.assertIn("NO ACTION", fk_text)

            # Validate the schema page's tables table and its column counts.
            tables_table = schema_soup.find("table", class_="tables-table")
            assert tables_table is not None
            counts = [cell.get_text(strip=True) for cell in tables_table.select("tbody td.count")]
            self.assertIn("1", counts)
            self.assertIn("4", counts)

    def test_browser_file_list(self) -> None:
        """Generate browser output from multiple schema files.

        Validate homepage links and singular/plural table count labels.
        """
        schema_a = """
name: alpha
"@id": "#alpha"
description: Alpha schema.
tables:
  - name: A
    "@id": "#A"
    columns:
      - name: id
        "@id": "#A.id"
        datatype: int
        description: key
""".strip()
        schema_b = """
name: beta
"@id": "#beta"
description: Beta schema.
tables:
  - name: B
    "@id": "#B"
    columns:
      - name: id
        "@id": "#B.id"
        datatype: int
        description: key
  - name: B2
    "@id": "#B2"
    columns:
      - name: id
        "@id": "#B2.id"
        datatype: int
        description: key
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            file_a = os.path.join(tmpdir, "a.yaml")
            file_b = os.path.join(tmpdir, "b.yaml")
            output_dir = os.path.join(tmpdir, "browser-list")
            with open(file_a, "w", encoding="utf-8") as f:
                f.write(schema_a)
            with open(file_b, "w", encoding="utf-8") as f:
                f.write(schema_b)

            render_browser_site(output_dir=output_dir, files=[file_a, file_b])

            self.assertTrue(os.path.exists(os.path.join(output_dir, "index.html")))
            index_soup = get_page_soup_by_title(output_dir, "Home")

            # Validate top-level page shell links.
            self.assertEqual(index_soup.title.get_text(strip=True), "Home")
            self.assertIsNotNone(index_soup.find("nav", attrs={"aria-label": "Breadcrumb"}))
            self.assertIsNotNone(index_soup.find("nav", class_="breadcrumb"))
            self.assertIsNotNone(find_link(index_soup, "schemas/alpha/index.html"))
            self.assertIsNotNone(find_link(index_soup, "schemas/beta/index.html"))
            self.assertIsNotNone(index_soup.find(string="Alpha schema."))
            self.assertIsNotNone(index_soup.find(string="Beta schema."))

            # Each schema page shows its description.
            alpha_soup = get_page_soup_by_title(output_dir, "Home - alpha")
            description = alpha_soup.find("p", class_="schema-description")
            assert description is not None
            self.assertEqual(description.get_text(strip=True), "Alpha schema.")

            # Validate singular/plural table count grammar per schema.
            schema_list = index_soup.find("ul", class_="schema-list")
            self.assertIsNotNone(schema_list)
            if schema_list is not None:
                count_labels = [
                    element.get_text(strip=True)
                    for element in schema_list.find_all(class_="muted")
                    if element.get_text(strip=True).startswith("(")
                ]
                self.assertIn("(1 table)", count_labels)
                self.assertIn("(2 tables)", count_labels)

    def test_browser_name_collisions(self) -> None:
        """Names that differ only in case or punctuation get unique paths and
        element IDs, with ``-2`` appended to the later one.
        """
        schema_yaml = """
name: {name}
"@id": "#{name}"
tables:
  - name: Data
    "@id": "#Data"
    columns:
      - name: ra
        "@id": "#Data.ra"
        datatype: double
        description: Lowercase.
      - name: RA
        "@id": "#Data.RA"
        datatype: double
        description: Uppercase.
    indexes:
      - name: IDX_ra
        "@id": "#IDX_ra"
        columns: ["#Data.ra"]
      - name: idx-RA
        "@id": "#idx-RA"
        columns: ["#Data.RA"]
    constraints:
      - name: UQ_ra
        "@id": "#UQ_ra"
        "@type": Unique
        columns: ["#Data.ra"]
      - name: uq-RA
        "@id": "#uq-RA"
        "@type": Unique
        columns: ["#Data.RA"]
  - name: data
    "@id": "#data"
    columns:
      - name: id
        "@id": "#data.id"
        datatype: int
        description: Key.
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            files = []
            for file_name, schema_name in (("a.yaml", "collide"), ("b.yaml", "COLLIDE")):
                files.append(os.path.join(tmpdir, file_name))
                with open(files[-1], "w", encoding="utf-8") as f:
                    f.write(schema_yaml.format(name=schema_name))
            output_dir = os.path.join(tmpdir, "browser")

            render_browser_site(output_dir=output_dir, files=files)

            nav = load_nav(output_dir)
            self.assertEqual(
                [schema["path"] for schema in nav["schemas"]],
                ["schemas/collide/index.html", "schemas/collide-2/index.html"],
            )
            first, second = nav["schemas"][0]["tables"]
            self.assertEqual(
                [first["path"], second["path"]], ["schemas/collide/data.html", "schemas/collide/data-2.html"]
            )
            self.assertEqual([c["anchor"] for c in first["columns"]], ["column-ra", "column-ra-2"])
            self.assertEqual([i["anchor"] for i in first["indexes"]], ["index-idx-ra", "index-idx-ra-2"])
            self.assertEqual(
                [c["anchor"] for c in first["constraints"]], ["constraint-uq-ra", "constraint-uq-ra-2"]
            )

            # The page uses the same IDs, each only once, and links to them.
            page_path = os.path.join(output_dir, "schemas", "collide", "data.html")
            with open(page_path, encoding="utf-8") as f:
                page = parse_html(f.read())
            ids = [element["id"] for element in page.find_all(id=True)]
            self.assertEqual(len(ids), len(set(ids)))
            for anchor in ("column-ra", "column-ra-2", "index-idx-ra-2", "constraint-uq-ra-2"):
                self.assertIn(anchor, ids)
            self.assertIsNotNone(find_link(page, "#column-ra-2"))

    def test_browser_custom_stylesheets(self) -> None:
        """Custom stylesheets are copied into the site and linked after the
        built-in stylesheet, in the order given.
        """
        schema_yaml = """
name: styled
"@id": "#styled"
tables:
  - name: Data
    "@id": "#Data"
    columns:
      - name: id
        "@id": "#Data.id"
        datatype: int
        description: Key.
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "styled.yaml")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)
            stylesheets = []
            for name, rule in (
                ("theme.css", ":root { --color-link: red; }"),
                ("local.css", "a { color: blue; }"),
            ):
                stylesheets.append(os.path.join(tmpdir, name))
                with open(stylesheets[-1], "w", encoding="utf-8") as f:
                    f.write(rule)
            output_dir = os.path.join(tmpdir, "browser")

            config_path = os.path.join(tmpdir, "site.yaml")
            with open(config_path, "w", encoding="utf-8") as f:
                f.write("stylesheets:\n  - theme.css\n  - local.css\n")

            render_browser_site(
                output_dir=output_dir, files=[schema_path], config=SiteConfig.from_file(Path(config_path))
            )

            assets = os.path.join(output_dir, "assets")
            for copied, original in (
                ("custom-1-theme.css", stylesheets[0]),
                ("custom-2-local.css", stylesheets[1]),
            ):
                with (
                    open(os.path.join(assets, copied), encoding="utf-8") as f,
                    open(original, encoding="utf-8") as g,
                ):
                    self.assertEqual(f.read(), g.read())

            for title, prefix in (("Home", "."), ("Home - styled - Data", "../..")):
                soup = get_page_soup_by_title(output_dir, title)
                self.assertEqual(
                    [link["href"] for link in soup.find_all("link", rel="stylesheet")],
                    [
                        f"{prefix}/assets/style.css",
                        f"{prefix}/assets/custom-1-theme.css",
                        f"{prefix}/assets/custom-2-local.css",
                    ],
                )

    def test_browser_site_title(self) -> None:
        """The configured title is the first breadcrumb item and starts every
        page title.
        """
        schema_yaml = """
name: titled
"@id": "#titled"
tables:
  - name: Data
    "@id": "#Data"
    columns:
      - name: id
        "@id": "#Data.id"
        datatype: int
        description: Key.
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "titled.yaml")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)
            output_dir = os.path.join(tmpdir, "browser")

            render_browser_site(
                output_dir=output_dir, files=[schema_path], config=SiteConfig(title="SDM Schemas")
            )

            for title in ("SDM Schemas", "SDM Schemas - titled", "SDM Schemas - titled - Data"):
                soup = get_page_soup_by_title(output_dir, title)
                breadcrumb = soup.find("nav", class_="breadcrumb")
                assert breadcrumb is not None
                home = breadcrumb.find(["a", "span"])
                assert home is not None
                self.assertEqual(home.get_text(strip=True), "SDM Schemas")

    def test_browser_site_icon(self) -> None:
        """A configured icon is copied into the site and shown in every
        page's header, linking to the home page; a configured repository is
        linked from every page's header.
        """
        schema_yaml = """
name: iconic
"@id": "#iconic"
tables:
  - name: Data
    "@id": "#Data"
    columns:
      - name: id
        "@id": "#Data.id"
        datatype: int
        description: Key.
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "iconic.yaml")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)
            icon_path = Path(tmpdir) / "logo.png"
            icon_path.write_bytes(b"not really a png")
            output_dir = os.path.join(tmpdir, "browser")

            render_browser_site(
                output_dir=output_dir,
                files=[schema_path],
                config=SiteConfig(
                    title="SDM Schemas", icon=icon_path, repository="https://github.com/lsst/sdm_schemas"
                ),
            )

            copied = Path(output_dir) / "assets" / "site-icon-logo.png"
            self.assertEqual(copied.read_bytes(), icon_path.read_bytes())
            for title, prefix in (
                ("SDM Schemas", "."),
                ("SDM Schemas - iconic", "../.."),
                ("SDM Schemas - iconic - Data", "../.."),
            ):
                soup = get_page_soup_by_title(output_dir, title)
                header = soup.find(class_="sidebar-header")
                assert header is not None
                repository = soup.find("a", class_="repository-link")
                assert repository is not None
                self.assertEqual(repository.get("href"), "https://github.com/lsst/sdm_schemas")
                link = header.find("a", class_="site-icon")
                assert link is not None
                self.assertEqual(link.get("href"), f"{prefix}/index.html")
                image = link.find("img")
                assert image is not None
                self.assertEqual(image.get("src"), f"{prefix}/assets/site-icon-logo.png")
                self.assertEqual(image.get("alt"), "SDM Schemas")

    def test_browser_relationship_diagrams(self) -> None:
        """With diagrams enabled, a schema with foreign keys gets a Mermaid
        relationship diagram on its page; otherwise there is none.
        """
        schema_yaml = """
name: linked
"@id": "#linked"
tables:
  - name: Parent
    "@id": "#Parent"
    columns:
      - name: id
        "@id": "#Parent.id"
        datatype: long
        description: Key.
    primaryKey: "#Parent.id"
  - name: Child
    "@id": "#Child"
    columns:
      - name: id
        "@id": "#Child.id"
        datatype: long
        description: Key.
      - name: parent_id
        "@id": "#Child.parent_id"
        datatype: long
        description: Parent.
      - name: value
        "@id": "#Child.value"
        datatype: double
        description: Value.
    primaryKey: "#Child.id"
    constraints:
      - name: FK_Child_Parent
        "@id": "#FK_Child_Parent"
        "@type": ForeignKey
        columns: ["#Child.parent_id"]
        referencedColumns: ["#Parent.id"]
  - name: Unrelated
    "@id": "#Unrelated"
    columns:
      - name: id
        "@id": "#Unrelated.id"
        datatype: int
        description: Key.
""".strip()
        unlinked_yaml = """
name: unlinked
"@id": "#unlinked"
tables:
  - name: Lonely
    "@id": "#Lonely"
    columns:
      - name: id
        "@id": "#Lonely.id"
        datatype: int
        description: Key.
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            files = []
            for name, text in (("linked.yaml", schema_yaml), ("unlinked.yaml", unlinked_yaml)):
                files.append(os.path.join(tmpdir, name))
                with open(files[-1], "w", encoding="utf-8") as f:
                    f.write(text)

            # Off by default.
            output_dir = os.path.join(tmpdir, "default")
            render_browser_site(output_dir=output_dir, files=files)
            soup = get_page_soup_by_title(output_dir, "Home - linked")
            self.assertIsNone(soup.find(class_="schema-diagram"))
            self.assertIsNone(soup.find(id="relationships"))

            output_dir = os.path.join(tmpdir, "diagrams")
            render_browser_site(output_dir=output_dir, files=files, config=SiteConfig(diagrams=True))

            soup = get_page_soup_by_title(output_dir, "Home - linked")
            self.assertIsNotNone(soup.find("h3", id="relationships"))
            diagram = soup.find(class_="schema-diagram")
            assert diagram is not None
            pre = diagram.find("pre", class_="mermaid")
            assert pre is not None
            self.assertEqual(
                pre.get_text().strip().splitlines(),
                [
                    "erDiagram",
                    '  t0["Parent"] {',
                    "    id long PK",
                    "  }",
                    '  t1["Child"] {',
                    "    id long PK",
                    "    parent_id long FK",
                    "  }",
                    '  t1 }o--|| t0 : "FK_Child_Parent"',
                ],
            )

            # A schema without foreign keys has no diagram.
            soup = get_page_soup_by_title(output_dir, "Home - unlinked")
            self.assertIsNone(soup.find(class_="schema-diagram"))

    def test_browser_show_empty_columns(self) -> None:
        """With ``show_empty_columns``, every column field is a column of the
        columns table, even if no column sets it.
        """
        schema_yaml = """
name: sparse
"@id": "#sparse"
tables:
  - name: Data
    "@id": "#Data"
    columns:
      - name: id
        "@id": "#Data.id"
        datatype: int
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "sparse.yaml")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)

            for show_empty, expected in (
                # tap:principal and tap:std have default values.
                (False, ["name", "datatype", "description", "unit", "nullable", "tap:principal", "tap:std"]),
                (
                    True,
                    [
                        "name",
                        "datatype",
                        "description",
                        "unit",
                        "nullable",
                        "ivoa:ucd",
                        "tap:column_index",
                        "tap:principal",
                        "tap:std",
                        "votable:arraysize",
                        "votable:datatype",
                        "votable:xtype",
                        "votable:utype",
                        "value",
                        "length",
                        "precision",
                        "autoincrement",
                    ],
                ),
            ):
                output_dir = os.path.join(tmpdir, f"browser-{show_empty}")
                render_browser_site(
                    output_dir=output_dir,
                    files=[schema_path],
                    config=SiteConfig(show_empty_columns=show_empty),
                )
                soup = get_page_soup_by_title(output_dir, "Home - sparse - Data")
                columns_table = soup.find("table", class_="columns-table")
                assert columns_table is not None
                self.assertEqual([th.get_text(strip=True) for th in columns_table.find_all("th")], expected)

    def test_browser_maintainer(self) -> None:
        """A configured maintainer is named in every page's footer, linked to
        its website if it has one.
        """
        schema_yaml = """
name: kept
"@id": "#kept"
tables:
  - name: Data
    "@id": "#Data"
    columns:
      - name: id
        "@id": "#Data.id"
        datatype: int
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "kept.yaml")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)

            for url in ("https://rubinobservatory.org/", None):
                output_dir = os.path.join(tmpdir, f"browser-{bool(url)}")
                render_browser_site(
                    output_dir=output_dir,
                    files=[schema_path],
                    config=SiteConfig(maintainer=Maintainer(name="Rubin Observatory", url=url)),
                )
                for title in ("Home", "Home - kept", "Home - kept - Data"):
                    soup = get_page_soup_by_title(output_dir, title)
                    footer = soup.find("footer", class_="site-footer")
                    assert footer is not None
                    self.assertIn("Maintained by: Rubin Observatory", footer.get_text(" ", strip=True))
                    link = footer.find("a")
                    if url:
                        assert link is not None
                        self.assertEqual(link.get("href"), url)
                        self.assertEqual(link.get_text(), "Rubin Observatory")
                    else:
                        self.assertIsNone(link)

    def test_site_config(self) -> None:
        """Site configuration defaults, file loading and validation."""
        self.assertEqual(SiteConfig().title, "Home")
        self.assertEqual(SiteConfig().stylesheets, [])
        self.assertFalse(SiteConfig().diagrams)
        self.assertIsNone(SiteConfig().repository)
        self.assertFalse(SiteConfig().show_empty_columns)
        self.assertIsNone(SiteConfig().maintainer)

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            config_dir = Path(tmpdir)
            (config_dir / "theme.css").write_text(":root {}\n", encoding="utf-8")

            # Stylesheet paths are relative to the configuration file.
            config_path = config_dir / "site.yaml"
            config_path.write_text("title: SDM Schemas\nstylesheets: [theme.css]\n", encoding="utf-8")
            config = SiteConfig.from_file(config_path)
            self.assertEqual(config.title, "SDM Schemas")
            self.assertEqual(config.stylesheets, [config_dir / "theme.css"])

            # A relative icon path is relative to the configuration file too.
            (config_dir / "logo.png").write_bytes(b"png")
            config_path.write_text("icon: logo.png\n", encoding="utf-8")
            self.assertEqual(SiteConfig.from_file(config_path).icon, config_dir / "logo.png")
            self.assertIsNone(SiteConfig().icon)

            # An empty file gives the defaults.
            config_path.write_text("", encoding="utf-8")
            self.assertEqual(SiteConfig.from_file(config_path), SiteConfig())

            # Unknown settings and missing stylesheets are errors.
            config_path.write_text("titel: SDM Schemas\n", encoding="utf-8")
            with self.assertRaises(ValidationError):
                SiteConfig.from_file(config_path)
            config_path.write_text("stylesheets: [missing.css]\n", encoding="utf-8")
            with self.assertRaises(ValidationError):
                SiteConfig.from_file(config_path)
            config_path.write_text("icon: missing.png\n", encoding="utf-8")
            with self.assertRaises(ValidationError):
                SiteConfig.from_file(config_path)
            config_path.write_text("repository: not a url\n", encoding="utf-8")
            with self.assertRaises(ValidationError):
                SiteConfig.from_file(config_path)

            # The maintainer needs a name; its URL is optional.
            config_path.write_text(
                "maintainer:\n  name: Rubin Observatory\n  url: https://rubinobservatory.org/\n",
                encoding="utf-8",
            )
            maintainer = SiteConfig.from_file(config_path).maintainer
            assert maintainer is not None
            self.assertEqual(maintainer.name, "Rubin Observatory")
            self.assertEqual(str(maintainer.url), "https://rubinobservatory.org/")
            config_path.write_text("maintainer:\n  url: https://rubinobservatory.org/\n", encoding="utf-8")
            with self.assertRaises(ValidationError):
                SiteConfig.from_file(config_path)

    def test_browser_compound_primary_key(self) -> None:
        """Every column of a compound primary key is tagged as part of it."""
        schema_yaml = """
name: demo
"@id": "#demo"
tables:
  - name: CompoundKeyTable
    "@id": "#CompoundKeyTable"
    columns:
      - name: first_id
        "@id": "#CompoundKeyTable.first_id"
        datatype: int
      - name: second_id
        "@id": "#CompoundKeyTable.second_id"
        datatype: int
      - name: value
        "@id": "#CompoundKeyTable.value"
        datatype: int
    primaryKey:
      - "#CompoundKeyTable.first_id"
      - "#CompoundKeyTable.second_id"
    indexes:
      - name: IDX_first_id
        "@id": "#IDX_first_id"
        columns:
          - "#CompoundKeyTable.first_id"
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "compound-key.yaml")
            output_dir = os.path.join(tmpdir, "browser")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)

            render_browser_site(output_dir=output_dir, files=[schema_path])

            table_soup = get_page_soup_by_title(output_dir, "Home - demo - CompoundKeyTable")
            for column_id, is_key in (
                ("column-first-id", True),
                ("column-second-id", True),
                ("column-value", False),
            ):
                row = table_soup.find("tr", id=column_id)
                assert row is not None
                tag = row.find(class_="column-tag-primary-key")
                self.assertEqual(tag is not None, is_key)
                if tag is not None:
                    self.assertEqual(tag.get_text(strip=True), "primary key")

            # A column can have several tags, in a fixed order.
            first_id = table_soup.find("tr", id="column-first-id")
            assert first_id is not None
            self.assertEqual(
                [tag.get_text(strip=True) for tag in first_id.find_all(class_="column-tag")],
                ["primary key", "indexed"],
            )

    def test_browser_non_yaml_file_error(self) -> None:
        """Fail when input file is not YAML."""
        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            output_dir = os.path.join(tmpdir, "browser-invalid")
            bad_file = os.path.join(tmpdir, "not_yaml.txt")
            with open(bad_file, "w", encoding="utf-8") as f:
                f.write("hello")

            with self.assertRaisesRegex(ValueError, "Input file must be .yaml or .yml"):
                render_browser_site(output_dir=output_dir, files=[bad_file])
