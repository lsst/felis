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

from felis.browser.render import render_static_site
from felis.datamodel import Schema

TEST_DIR = os.path.abspath(os.path.dirname(__file__))


def render_browser_site(
    output_dir: str,
    files: list[str],
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
    render_static_site(schemas=schemas, output_dir=Path(output_dir))


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

        This test covers navigation controls, section wiring, details rows,
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
        "tap:std": 0
        description: Business code.
      - name: parent_id
        "@id": "#ChildTable.parent_id"
        datatype: int
        description: Parent link.
      - name: amount
        "@id": "#ChildTable.amount"
        datatype: int
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
            schema_soup = get_page_soup_by_title(output_dir, "demo - Felis Browser")
            child_table_soup = get_page_soup_by_title(output_dir, "ChildTable - demo - Felis Browser")

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
                scripts = [str(script.get("src")).rsplit("/", 1)[-1] for script in soup.find_all("script")]
                self.assertEqual(scripts, ["nav-data.js", "sidebar.js"])

            # A schema without a version shows no version line.
            self.assertIsNone(schema_soup.find(class_="schema-version"))

            # The indexes table has no Type column.
            indexes_heading = child_table_soup.find(id="table-childtable-indexes")
            assert indexes_heading is not None
            indexes_table = indexes_heading.find_next("table")
            assert indexes_table is not None
            self.assertEqual(
                [th.get_text(strip=True) for th in indexes_table.find_all("th")],
                ["Index", "Definition", "Description"],
            )

            # Validate columns/details table content and detail-toggle wiring.
            self.assertIsNotNone(child_table_soup.find(id="table-childtable-primary-key"))
            self.assertIsNotNone(child_table_soup.find(string="IDX_ChildTable_code"))
            self.assertIsNotNone(child_table_soup.find("th", string="UCD"))
            self.assertIsNotNone(child_table_soup.find("td", string="meta.code"))

            # Datatype is shown in the main table row and should not be
            # repeated in details; other non-row fields should appear.
            self.assertIsNotNone(
                child_table_soup.find(attrs={"data-column-details-target": "column-code-details"})
            )
            code_details_row = child_table_soup.find(id="column-code-details")
            self.assertIsNotNone(code_details_row)
            if code_details_row is not None:
                self.assertTrue(code_details_row.has_attr("hidden"))
                self.assertIsNone(code_details_row.find("th", class_="field-key", string="datatype"))
                self.assertIsNone(code_details_row.find("th", class_="field-key", string="@id"))
                self.assertIsNotNone(code_details_row.find("th", class_="field-key", string="tap:std"))
                self.assertIsNotNone(code_details_row.find("td", class_="field-value", string="0"))

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

            # Validate schema-page singular/plural column count grammar.
            schema_list = schema_soup.find("ul", class_="schema-list")
            self.assertIsNotNone(schema_list)
            if schema_list is not None:
                count_labels = [
                    element.get_text(strip=True)
                    for element in schema_list.find_all(class_="muted")
                    if element.get_text(strip=True).startswith("(")
                ]
                self.assertIn("(1 column)", count_labels)
                self.assertIn("(4 columns)", count_labels)

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

    def test_browser_compound_primary_key_spacing(self) -> None:
        """Render compound primary keys without whitespace before commas."""
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
    primaryKey:
      - "#CompoundKeyTable.first_id"
      - "#CompoundKeyTable.second_id"
""".strip()

        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            schema_path = os.path.join(tmpdir, "compound-key.yaml")
            output_dir = os.path.join(tmpdir, "browser")
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_yaml)

            render_browser_site(output_dir=output_dir, files=[schema_path])

            table_soup = get_page_soup_by_title(output_dir, "CompoundKeyTable - demo - Felis Browser")

            # Validate both primary key links are present and comma spacing is
            # normalized.
            primary_key_header = table_soup.find(id="table-compoundkeytable-primary-key")
            self.assertIsNotNone(primary_key_header)
            if primary_key_header is not None:
                primary_key_value = primary_key_header.find_next("p")
                self.assertIsNotNone(primary_key_value)
                if primary_key_value is not None:
                    self.assertEqual(
                        primary_key_value.get_text(" ", strip=True).replace(" ,", ","),
                        "first_id, second_id",
                    )
                    self.assertIsNotNone(find_link(primary_key_value, "#column-first-id"))
                    self.assertIsNotNone(find_link(primary_key_value, "#column-second-id"))

    def test_browser_non_yaml_file_error(self) -> None:
        """Fail when input file is not YAML."""
        with tempfile.TemporaryDirectory(dir=TEST_DIR) as tmpdir:
            output_dir = os.path.join(tmpdir, "browser-invalid")
            bad_file = os.path.join(tmpdir, "not_yaml.txt")
            with open(bad_file, "w", encoding="utf-8") as f:
                f.write("hello")

            with self.assertRaisesRegex(ValueError, "Input file must be .yaml or .yml"):
                render_browser_site(output_dir=output_dir, files=[bad_file])
