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

    schemas: list[Schema] = []
    source_paths: list[str] = []
    for path in paths:
        schemas.append(
            Schema.from_uri(path, context={"id_generation": True, "column_ref_index_increment": None})
        )
        source_paths.append(path)

    render_static_site(
        schemas=schemas,
        source_paths=source_paths,
        output_dir=Path(output_dir),
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


def find_link(container: BeautifulSoup | Tag, href: str, title: str | None = None) -> Tag | None:
    """Find a link by href and optional title."""
    attrs: dict[str, str] = {"href": href}
    if title is not None:
        attrs["title"] = title
    return container.find("a", attrs=attrs)


def find_details_section(soup: BeautifulSoup, section_class: str) -> Tag | None:
    """Find a details section by its class name."""
    return soup.find("details", class_=section_class)


def find_summary_link(section: Tag | None, href: str) -> Tag | None:
    """Find the summary link in a details section by href."""
    if section is None:
        return None
    summary = section.find("summary")
    if summary is None:
        return None
    return find_link(summary, href)


def find_section_summary_link(soup: BeautifulSoup, section_class: str, href: str) -> Tag | None:
    """Find the section heading link for a named section class."""
    for section in soup.find_all(class_=section_class):
        link = find_link(section, href)
        if link is not None:
            return link
    return None


def find_section_by_summary_href(soup: BeautifulSoup, section_class: str, href: str) -> Tag | None:
    """Find a section by class and heading-link href."""
    for section in soup.find_all(class_=section_class):
        if find_link(section, href) is not None:
            return section
    return None


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

            # Validate tooltip-bearing links in sidebar/tree context.
            self.assertIsNotNone(child_table_soup.find(attrs={"title": "Parent rows."}))
            self.assertIsNotNone(child_table_soup.find(attrs={"title": "Child rows."}))
            self.assertIsNotNone(child_table_soup.find(attrs={"title": "Business code."}))
            self.assertIsNotNone(child_table_soup.find(attrs={"title": "Index on code."}))
            self.assertIsNotNone(child_table_soup.find(attrs={"title": "Unique code per parent."}))
            self.assertIsNotNone(
                find_link(
                    child_table_soup,
                    "childtable.html#index-idx-childtable-code",
                    title="Index on code.",
                )
            )

            # Validate section summary linkss.
            columns_summary = find_section_summary_link(
                child_table_soup,
                section_class="tree-columns",
                href="childtable.html#table-childtable-columns",
            )
            indexes_summary = find_section_summary_link(
                child_table_soup,
                section_class="tree-indexes",
                href="childtable.html#table-childtable-indexes",
            )
            constraints_summary = find_section_summary_link(
                child_table_soup,
                section_class="tree-constraints",
                href="childtable.html#table-childtable-constraints",
            )
            self.assertIsNotNone(columns_summary)
            self.assertIsNotNone(indexes_summary)
            self.assertIsNotNone(constraints_summary)
            if columns_summary is not None:
                self.assertFalse(columns_summary.has_attr("title"))
            if indexes_summary is not None:
                self.assertFalse(indexes_summary.has_attr("title"))
            if constraints_summary is not None:
                self.assertFalse(constraints_summary.has_attr("title"))

            # Validate section collapsed/open state.
            columns_section = find_section_by_summary_href(
                child_table_soup,
                section_class="tree-columns",
                href="childtable.html#table-childtable-columns",
            )
            indexes_section = find_section_by_summary_href(
                child_table_soup,
                section_class="tree-indexes",
                href="childtable.html#table-childtable-indexes",
            )
            constraints_section = find_section_by_summary_href(
                child_table_soup,
                section_class="tree-constraints",
                href="childtable.html#table-childtable-constraints",
            )
            self.assertIsNotNone(columns_section)
            self.assertIsNotNone(indexes_section)
            self.assertIsNotNone(constraints_section)
            if columns_section is not None:
                self.assertEqual(columns_section.name, "div")
            if indexes_section is not None:
                self.assertEqual(indexes_section.name, "div")
            if constraints_section is not None:
                self.assertEqual(constraints_section.name, "div")

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
            constraints_anchor = find_section_summary_link(
                child_table_soup,
                section_class="tree-constraints",
                href="childtable.html#table-childtable-constraints",
            )
            self.assertIsNotNone(constraints_anchor)
            if constraints_anchor is not None:
                self.assertEqual(constraints_anchor.get_text(strip=True), "Constraints")

            self.assertIsNotNone(child_table_soup.find(id="table-childtable-constraints"))
            self.assertIsNotNone(child_table_soup.find(string="CK_ChildTable_amount_positive"))
            self.assertIsNotNone(child_table_soup.find("code", string="amount > 0"))
            self.assertIsNotNone(child_table_soup.find(string="UQ_ChildTable_code_parent"))
            self.assertIsNotNone(
                find_link(
                    child_table_soup,
                    "childtable.html#constraint-fk-childtable-parenttable",
                )
            )
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
