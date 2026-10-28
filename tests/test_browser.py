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
import posixpath
import re
import tempfile
import unittest
from pathlib import Path

from bs4 import BeautifulSoup
from pydantic import ValidationError

from felis.browser import SiteConfig, render_static_site
from felis.browser.diagram import _RelationshipDiagram
from felis.browser.sitemap import _UniqueNames
from felis.datamodel import Schema

TEST_DIR = Path(__file__).parent
SALES_YAML = TEST_DIR / "data" / "sales.yaml"
# The site configuration with every setting, and the files it names.
SITE_CONFIG = TEST_DIR / "data" / "browser" / "site.yaml"


def load_sales() -> Schema:
    """Load the sales test schema."""
    return Schema.from_uri(str(SALES_YAML), context={"id_generation": True})


def build_site(site_dir: Path, schemas: list[Schema], config: SiteConfig | None = None) -> None:
    """Render a browser site into ``site_dir``."""
    render_static_site(schemas=schemas, output_dir=site_dir, config=config)


def read_page(site_dir: Path, path: str) -> BeautifulSoup:
    """Parse a page of a site, given its path from the site root."""
    return BeautifulSoup((site_dir / path).read_text(encoding="utf-8"), "html.parser")


def read_nav(site_dir: Path) -> dict:
    """Read the navigation data that the sidebar and the search use."""
    script = (site_dir / "assets" / "nav-data.js").read_text(encoding="utf-8")
    prefix, suffix = "window.FELIS_NAV = ", ";\n"
    assert script.startswith(prefix) and script.endswith(suffix), "Unexpected nav-data.js format"
    return json.loads(script[len(prefix) : -len(suffix)])


def html_pages(site_dir: Path) -> list[str]:
    """Return the paths of all the pages of a site, from the site root."""
    return sorted(path.relative_to(site_dir).as_posix() for path in site_dir.rglob("*.html"))


def cell_texts(row: BeautifulSoup) -> list[str]:
    """Return the text of each cell of a table row."""
    return [cell.get_text(" ", strip=True) for cell in row.find_all("td")]


def parse_diagram(text: str) -> tuple[dict[str, list[str]], list[tuple[str, str, str]]]:
    """Read the Mermaid text of a relationship diagram.

    Returns
    -------
    tuple
        The tables, by label, each with its key column lines ("name type
        marks"), and the relationships, as (child, parent, label) with the
        tables' labels.
    """
    tables: dict[str, list[str]] = {}
    labels: dict[str, str] = {}
    relationships = []
    current = None
    for line in text.strip().splitlines()[1:]:
        line = line.strip()
        if entity := re.fullmatch(r'(\w+)\["(.*)"\] \{', line):
            key, label = entity.groups()
            labels[key] = label
            current = tables.setdefault(label, [])
        elif line == "}":
            current = None
        elif relationship := re.fullmatch(r'(\w+) \}o--\|\| (\w+) : "(.*)"', line):
            child, parent, label = relationship.groups()
            relationships.append((child, parent, label))
        elif current is not None:
            current.append(line)
    return tables, [(labels[child], labels[parent], label) for child, parent, label in relationships]


class BrowserSiteTestCase(unittest.TestCase):
    """Tests of a site built from the sales schema with every setting of the
    site configuration on.
    """

    site: Path
    """The directory of the built site."""

    _tmpdir: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmpdir = tempfile.TemporaryDirectory()
        cls.site = Path(cls._tmpdir.name) / "site"
        build_site(cls.site, [load_sales()], SiteConfig.from_file(SITE_CONFIG))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmpdir.cleanup()

    def page(self, path: str) -> BeautifulSoup:
        """Parse a page of the site, given its path from the site root."""
        return read_page(self.site, path)

    def test_pages(self) -> None:
        """There is a home page, a page for the schema, and a page for each
        table, named after it in lowercase with hyphens.
        """
        self.assertEqual(
            html_pages(self.site),
            [
                "index.html",
                "schemas/sales/customers.html",
                "schemas/sales/index.html",
                "schemas/sales/items.html",
                "schemas/sales/order-items.html",
                "schemas/sales/orders.html",
            ],
        )

    def test_links(self) -> None:
        """Every link, stylesheet, script and image on every page that isn't
        on another website points to a file of the site, and every ``#``
        fragment points to an element on its page.
        """
        for page_path in html_pages(self.site):
            soup = self.page(page_path)
            for element in soup.find_all(["a", "link", "script", "img"]):
                url = element.get("href") or element.get("src")
                # Leave out links to other websites.
                if not url or url.startswith(("https:", "http:")):
                    continue
                with self.subTest(page=page_path, url=url):
                    path, _, fragment = url.partition("#")
                    target = posixpath.normpath(posixpath.join(posixpath.dirname(page_path), path or "."))
                    if not path:
                        target = page_path
                    self.assertTrue((self.site / target).is_file(), f"{target} doesn't exist")
                    if fragment:
                        self.assertIsNotNone(
                            read_page(self.site, target).find(id=fragment), f"#{fragment} isn't on {target}"
                        )

    def test_navigation_data(self) -> None:
        """The navigation data, from which the sidebar tree and the search
        are built, lists the schema's tables and each table's columns,
        indexes and constraints, all sorted by name.
        """
        nav = read_nav(self.site)

        def entries(*names_and_descriptions: tuple[str, str], kind: str) -> list[dict]:
            # The anchor of a column, index or constraint is its kind
            # followed by its name in lowercase with hyphens.
            return [
                {"name": name, "anchor": f"{kind}-{name.lower().replace('_', '-')}", "description": text}
                for name, text in names_and_descriptions
            ]

        self.assertEqual(
            nav,
            {
                "schemas": [
                    {
                        "name": "sales",
                        "path": "schemas/sales/index.html",
                        "description": "Simple sales schema",
                        "tables": [
                            {
                                "name": "customers",
                                "path": "schemas/sales/customers.html",
                                "anchor": "table-customers",
                                "description": "Customer information",
                                "columns": entries(
                                    ("address", "Customer address"),
                                    ("customer_id", "Unique customer identifier"),
                                    ("name", "Customer name"),
                                    kind="column",
                                ),
                                "indexes": entries(
                                    ("idx_customers_name", "Index on customer name"), kind="index"
                                ),
                                "constraints": entries(
                                    ("idx_customers_uniq_name", "Unique customer name"), kind="constraint"
                                ),
                            },
                            {
                                "name": "items",
                                "path": "schemas/sales/items.html",
                                "anchor": "table-items",
                                "description": "Items",
                                "columns": entries(
                                    ("item_id", "Unique item identifier"),
                                    ("note", "Item note"),
                                    ("order_id", "Order identifier"),
                                    ("product_id", "Product identifier"),
                                    ("quantity", "Quantity ordered"),
                                    kind="column",
                                ),
                                "indexes": [],
                                "constraints": entries(
                                    ("non_negative_quantity", "Quantity must be non-negative"),
                                    kind="constraint",
                                ),
                            },
                            {
                                "name": "order_items",
                                "path": "schemas/sales/order-items.html",
                                "anchor": "table-order-items",
                                "description": "Order items",
                                "columns": entries(
                                    ("item_id", "Item identifier"),
                                    ("order_id", "Order identifier"),
                                    kind="column",
                                ),
                                "indexes": [],
                                "constraints": [],
                            },
                            {
                                "name": "orders",
                                "path": "schemas/sales/orders.html",
                                "anchor": "table-orders",
                                "description": "Customer orders",
                                "columns": entries(
                                    ("customer_id", "Customer identifier"),
                                    ("note", "Order note"),
                                    ("order_date", "Order date"),
                                    ("order_id", "Unique order identifier"),
                                    kind="column",
                                ),
                                "indexes": [],
                                "constraints": entries(
                                    ("fk_customer_id", "Foreign key to customers"), kind="constraint"
                                ),
                            },
                        ],
                    }
                ]
            },
        )

    def test_navigation_data_targets(self) -> None:
        """Every page and anchor in the navigation data exists, so the links
        in the sidebar tree and the search results work.
        """
        for schema in read_nav(self.site)["schemas"]:
            self.assertTrue((self.site / schema["path"]).is_file())
            for table in schema["tables"]:
                soup = self.page(table["path"])
                anchors = [table["anchor"]] + [
                    entry["anchor"] for kind in ("columns", "indexes", "constraints") for entry in table[kind]
                ]
                for anchor in anchors:
                    with self.subTest(page=table["path"], anchor=anchor):
                        self.assertIsNotNone(soup.find(id=anchor))

    def test_page_titles(self) -> None:
        """Each page's title is the site title, followed by the schema and
        table names on the schema and table pages.
        """
        for path, title in (
            ("index.html", "Test Schemas"),
            ("schemas/sales/index.html", "Test Schemas - sales"),
            ("schemas/sales/orders.html", "Test Schemas - sales - orders"),
        ):
            with self.subTest(page=path):
                self.assertEqual(self.page(path).title.get_text(), title)

    def test_breadcrumbs(self) -> None:
        """The header of a table page links to the home page (named after the
        site title), the schema page, and the page itself.
        """
        breadcrumb = self.page("schemas/sales/orders.html").find("nav", class_="breadcrumb")
        self.assertEqual(
            [(link.get_text(strip=True), link["href"]) for link in breadcrumb.find_all("a")],
            [("Test Schemas", "../../index.html"), ("sales", "index.html"), ("orders", "orders.html")],
        )

    def test_headings(self) -> None:
        """Each page has one h1, with the site title, schema name or table
        name, and its sections have h2 headings, with no h3.
        """
        for path, title, sections in (
            ("index.html", "Test Schemas", ["Schemas"]),
            ("schemas/sales/index.html", "sales", ["Tables", "Relationships"]),
            ("schemas/sales/orders.html", "orders", ["Columns", "Constraints"]),
        ):
            with self.subTest(page=path):
                soup = self.page(path)
                self.assertEqual([h1.get_text() for h1 in soup.find_all("h1")], [title])
                self.assertEqual([h2.get_text() for h2 in soup.find_all("h2")], sections)
                self.assertEqual(soup.find_all("h3"), [])

    def test_home_page_schemas(self) -> None:
        """The home page lists each schema with its version (blank if it has
        none), description and number of tables.
        """
        table = self.page("index.html").find("table", class_="summary-table")
        self.assertEqual(
            [cell_texts(row) for row in table.select("tbody tr")],
            [["sales", "", "Simple sales schema", "4"]],
        )

    def test_site_description(self) -> None:
        """The site description is shown on the home page as plain text: HTML
        in it is shown as written, not used as markup.
        """
        description = self.page("index.html").find("p", class_="site-description")
        self.assertEqual(description.get_text(), "Schemas for <b>testing</b>: tables & columns.")
        self.assertIsNone(description.find("b"))

    def test_schema_page(self) -> None:
        """The schema page shows the schema's description, and lists each table
        by name with its description and number of columns.
        """
        soup = self.page("schemas/sales/index.html")
        self.assertEqual(soup.find(class_="schema-description").get_text(), "Simple sales schema")
        # The sales schema has no version, so none is shown.
        self.assertIsNone(soup.find(class_="schema-version"))
        table = soup.find("table", class_="summary-table")
        self.assertEqual(
            [cell_texts(row) for row in table.select("tbody tr")],
            [
                ["customers", "Customer information", "3"],
                ["items", "Items", "5"],
                ["order_items", "Order items", "2"],
                ["orders", "Customer orders", "4"],
            ],
        )

    def test_columns_table(self) -> None:
        """A table page's columns table has a row for each column, by name,
        with the value of each column field.
        """
        table = self.page("schemas/sales/customers.html").find("table", class_="columns-table")
        headers = [th.get_text() for th in table.find_all("th")]
        rows = {
            row["id"]: dict(zip(headers, cell_texts(row), strict=True)) for row in table.select("tbody tr")
        }
        self.assertEqual(list(rows), ["column-address", "column-customer-id", "column-name"])
        name = rows["column-name"]
        # The name cell also holds the column's tags.
        self.assertEqual(name["name"], "name indexed")
        self.assertEqual(name["datatype"], "string")
        self.assertEqual(name["description"], "Customer name")
        self.assertEqual(name["nullable"], "false")
        self.assertEqual(name["length"], "30")
        # The CSV download is named after the schema and table.
        self.assertEqual(table["data-export-name"], "sales.customers")

    def test_show_empty_columns(self) -> None:
        """With ``show_empty_columns`` on, the columns table has a column for
        every column field, even ones that no column of the table sets.
        """
        table = self.page("schemas/sales/order-items.html").find("table", class_="columns-table")
        self.assertEqual(
            [th.get_text() for th in table.find_all("th")],
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
        )

    def test_column_tags(self) -> None:
        """Columns in the primary key, a foreign key or an index are tagged,
        with hover text giving details.
        """
        for path, column, tags in (
            ("customers.html", "column-customer-id", [("primary key", "Part of the table's primary key")]),
            ("customers.html", "column-name", [("indexed", "In index idx_customers_name")]),
            ("customers.html", "column-address", []),
            ("orders.html", "column-customer-id", [("foreign key", "References customers.customer_id")]),
        ):
            with self.subTest(page=path, column=column):
                row = self.page(f"schemas/sales/{path}").find("tr", id=column)
                self.assertEqual(
                    [(tag.get_text(), tag["title"]) for tag in row.find_all(class_="column-tag")], tags
                )

    def test_indexes(self) -> None:
        """A table page lists the table's indexes, with links to their
        columns.
        """
        row = self.page("schemas/sales/customers.html").find("tr", id="index-idx-customers-name")
        self.assertEqual(cell_texts(row), ["idx_customers_name", "name", "Index on customer name"])
        self.assertEqual(row.find("a")["href"], "#column-name")

    def test_unique_constraint(self) -> None:
        """A unique constraint is listed with links to its columns."""
        row = self.page("schemas/sales/customers.html").find("tr", id="constraint-idx-customers-uniq-name")
        self.assertEqual(
            cell_texts(row), ["idx_customers_uniq_name", "Unique", "name", "Unique customer name"]
        )
        self.assertEqual(row.find("a")["href"], "#column-name")

    def test_foreign_key_constraint(self) -> None:
        """A foreign key is listed with its columns, the table and columns it
        references, and its delete and update actions.
        """
        row = self.page("schemas/sales/orders.html").find("tr", id="constraint-fk-customer-id")
        self.assertEqual(
            cell_texts(row),
            [
                "fk_customer_id",
                "ForeignKey",
                "Columns: customer_id References: customers . customer_id "
                "On Delete: SET NULL On Update: CASCADE",
                "Foreign key to customers",
            ],
        )
        self.assertEqual(
            [link["href"] for link in row.find_all("a")],
            ["#column-customer-id", "customers.html", "customers.html#column-customer-id"],
        )

    def test_check_constraint(self) -> None:
        """A check constraint is listed with its expression."""
        row = self.page("schemas/sales/items.html").find("tr", id="constraint-non-negative-quantity")
        self.assertEqual(
            cell_texts(row),
            ["non_negative_quantity", "Check", "quantity >= 0", "Quantity must be non-negative"],
        )

    def test_stylesheets(self) -> None:
        """The configured stylesheets are copied into the site and linked from
        every page after the built-in one, in the order given.
        """
        for name in ("theme.css", "local.css"):
            index = 1 if name == "theme.css" else 2
            self.assertEqual(
                (self.site / "assets" / f"custom-{index}-{name}").read_text(encoding="utf-8"),
                (SITE_CONFIG.parent / name).read_text(encoding="utf-8"),
            )
        for path, root in (("index.html", "."), ("schemas/sales/orders.html", "../..")):
            with self.subTest(page=path):
                self.assertEqual(
                    [link["href"] for link in self.page(path).find_all("link", rel="stylesheet")],
                    [
                        f"{root}/assets/style.css",
                        f"{root}/assets/custom-1-theme.css",
                        f"{root}/assets/custom-2-local.css",
                    ],
                )

    def test_icon(self) -> None:
        """The configured icon is copied into the site and shown at the top of
        every page, linking to the home page.
        """
        self.assertEqual(
            (self.site / "assets" / "site-icon-logo.svg").read_bytes(),
            (SITE_CONFIG.parent / "logo.svg").read_bytes(),
        )
        for path, root in (("index.html", "."), ("schemas/sales/orders.html", "../..")):
            with self.subTest(page=path):
                link = self.page(path).find("a", class_="site-icon")
                self.assertEqual(link["href"], f"{root}/index.html")
                self.assertEqual(link.img["src"], f"{root}/assets/site-icon-logo.svg")
                # The icon's alternative text is the site title.
                self.assertEqual(link.img["alt"], "Test Schemas")

    def test_favicon(self) -> None:
        """The configured favicon is copied into the site and linked from every
        page.
        """
        self.assertEqual(
            (self.site / "assets" / "favicon.svg").read_bytes(),
            (SITE_CONFIG.parent / "favicon.svg").read_bytes(),
        )
        for path, root in (("index.html", "."), ("schemas/sales/orders.html", "../..")):
            with self.subTest(page=path):
                self.assertEqual(
                    self.page(path).find("link", rel="icon")["href"], f"{root}/assets/favicon.svg"
                )

    def test_footer(self) -> None:
        """Every page's footer names the maintainer, linked to its website, and
        says the site was built with Felis, linked to its documentation.
        """
        for path in ("index.html", "schemas/sales/index.html", "schemas/sales/orders.html"):
            with self.subTest(page=path):
                footer = self.page(path).find("footer", class_="site-footer")
                text = footer.get_text(" ", strip=True)
                self.assertIn("Maintained by Test Observatory", text)
                self.assertIn("Built with Felis", text)
                links = [(a.get_text(), a["href"]) for a in footer.find_all("a")]
                self.assertEqual(
                    links, [("Test Observatory", "https://example.org/"), ("Felis", "https://felis.lsst.io/")]
                )

    def test_relationship_diagram(self) -> None:
        """With ``diagrams`` on, the schema page has a diagram of the tables
        linked by foreign keys, showing their key columns, for Mermaid to
        draw.
        """
        diagram = self.page("schemas/sales/index.html").find("pre", class_="mermaid")
        tables, relationships = parse_diagram(diagram.get_text())
        # orders references customers; the other tables have no foreign
        # keys, so aren't shown.
        self.assertEqual(
            tables, {"customers": ["customer_id int PK"], "orders": ["order_id int PK", "customer_id int FK"]}
        )
        self.assertEqual(relationships, [("orders", "customers", "fk_customer_id")])


class BrowserDefaultsTestCase(unittest.TestCase):
    """Tests of a site built from the sales schema without a site
    configuration.
    """

    site: Path
    """The directory of the built site."""

    _tmpdir: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmpdir = tempfile.TemporaryDirectory()
        cls.site = Path(cls._tmpdir.name) / "site"
        build_site(cls.site, [load_sales()])

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmpdir.cleanup()

    def test_title(self) -> None:
        """Without a configured title, the site is titled "Home"."""
        self.assertEqual(read_page(self.site, "index.html").title.get_text(), "Home")
        self.assertEqual(read_page(self.site, "schemas/sales/index.html").title.get_text(), "Home - sales")

    def test_no_optional_parts(self) -> None:
        """Without a configuration, pages have no site description, icon,
        favicon, extra stylesheets, maintainer or relationship diagram.
        """
        for path in ("index.html", "schemas/sales/index.html", "schemas/sales/orders.html"):
            with self.subTest(page=path):
                soup = read_page(self.site, path)
                self.assertIsNone(soup.find(class_="site-description"))
                self.assertIsNone(soup.find(class_="site-icon"))
                self.assertIsNone(soup.find("link", rel="icon"))
                self.assertEqual(len(soup.find_all("link", rel="stylesheet")), 1)
                self.assertNotIn("Maintained by", soup.find("footer").get_text())
                self.assertIsNone(soup.find(class_="schema-diagram"))

    def test_columns_table_leaves_out_empty_fields(self) -> None:
        """By default, the columns table has a column for a field only if some
        column of the table sets it to something other than its default;
        name, datatype, description, unit and nullable are always shown.
        """

        def headings(path: str) -> list[str]:
            table = read_page(self.site, path).find("table", class_="columns-table")
            return [th.get_text() for th in table.find_all("th")]

        # No column of customers sets tap:principal or tap:std from their
        # default, 0; one column of order_items is principal.
        self.assertEqual(
            headings("schemas/sales/customers.html"),
            ["name", "datatype", "description", "unit", "nullable", "votable:arraysize", "length"],
        )
        self.assertEqual(
            headings("schemas/sales/order-items.html"),
            ["name", "datatype", "description", "unit", "nullable", "tap:principal"],
        )


class BrowserNameClashTestCase(unittest.TestCase):
    """Tests of a site with two schemas of the same name."""

    def test_name_clash(self) -> None:
        """When two schemas have the same name, the second one's pages are
        put under the name with ``-2`` added, so neither overwrites the
        other.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            site = Path(tmpdir)
            build_site(site, [load_sales(), load_sales()])
            self.assertEqual(
                [schema["path"] for schema in read_nav(site)["schemas"]],
                ["schemas/sales/index.html", "schemas/sales-2/index.html"],
            )
            self.assertTrue((site / "schemas" / "sales-2" / "orders.html").is_file())


class UniqueNamesTestCase(unittest.TestCase):
    """Tests of the unique names given to pages and element IDs."""

    def test_claim(self) -> None:
        """A repeated name gets the next free ``-N`` suffix, skipping names
        already handed out, whether as repeats or as themselves.
        """
        names = _UniqueNames()
        self.assertEqual(
            [names.claim(name) for name in ("foo", "foo", "foo-2", "foo", "foo-3")],
            ["foo", "foo-2", "foo-2-2", "foo-3", "foo-3-2"],
        )

    def test_site(self) -> None:
        """Tables and columns whose names give the same slug, including one
        that is already a suffixed name, each get their own page or element
        ID.
        """
        columns = [
            {"name": name, "@id": f"#{{table}}.{name}", "datatype": "int", "description": "Column"}
            for name in ("x", "X", "x-2")
        ]
        schema = Schema.model_validate(
            {
                "name": "clash",
                "@id": "#clash",
                "tables": [
                    {
                        "name": name,
                        "@id": f"#{name}",
                        "columns": [
                            {**column, "@id": column["@id"].format(table=name)} for column in columns
                        ],
                    }
                    for name in ("foo", "Foo", "foo-2")
                ],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            site = Path(tmpdir)
            build_site(site, [schema])
            tables = read_nav(site)["schemas"][0]["tables"]
            self.assertEqual(
                sorted(table["path"] for table in tables),
                [
                    "schemas/clash/foo-2-2.html",
                    "schemas/clash/foo-2.html",
                    "schemas/clash/foo.html",
                ],
            )
            for table in tables:
                with self.subTest(table=table["name"]):
                    anchors = [column["anchor"] for column in table["columns"]]
                    self.assertEqual(len(set(anchors)), 3)
                    ids = [element["id"] for element in read_page(site, table["path"]).find_all(id=True)]
                    self.assertEqual(len(ids), len(set(ids)))

    def test_table_named_index(self) -> None:
        """A table named ``index``, whose page would be its schema's page,
        ``index.html``, is an error.
        """
        schema = Schema.model_validate(
            {
                "name": "named",
                "@id": "#named",
                "tables": [
                    {
                        "name": "Index",
                        "@id": "#Index",
                        "columns": [
                            {"name": "id", "@id": "#Index.id", "datatype": "int", "description": "Key"}
                        ],
                    }
                ],
            }
        )
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            self.assertRaisesRegex(ValueError, "Invalid table name 'Index'"),
        ):
            build_site(Path(tmpdir), [schema])


class RelationshipDiagramTestCase(unittest.TestCase):
    """Tests of the Mermaid text of relationship diagrams."""

    def test_names_mermaid_rejects(self) -> None:
        """A column name starting with a digit gets an underscore in front,
        and a table or constraint name loses any ``%`` and backslash and has
        its double quotes changed to single ones, as Mermaid's grammar
        requires.
        """
        schema = Schema.model_validate(
            {
                "name": "awkward",
                "@id": "#awkward",
                "tables": [
                    {
                        "name": "a%b\\c",
                        "@id": "#parent",
                        "primaryKey": "#parent.2mass_id",
                        "columns": [
                            {
                                "name": "2mass_id",
                                "@id": "#parent.2mass_id",
                                "datatype": "long",
                                "description": "Key",
                            }
                        ],
                    },
                    {
                        "name": "child",
                        "@id": "#child",
                        "columns": [
                            {
                                "name": "parent_id",
                                "@id": "#child.parent_id",
                                "datatype": "long",
                                "description": "Ref",
                            }
                        ],
                        "constraints": [
                            {
                                "name": 'fk_"x"%',
                                "@id": "#fk_x",
                                "@type": "ForeignKey",
                                "columns": ["#child.parent_id"],
                                "referencedColumns": ["#parent.2mass_id"],
                            }
                        ],
                    },
                ],
            }
        )
        tables, relationships = parse_diagram(_RelationshipDiagram(schema).text() or "")
        self.assertEqual(tables, {"abc": ["_2mass_id long PK"], "child": ["parent_id long FK"]})
        self.assertEqual(relationships, [("child", "abc", "fk_'x'")])


class BrowserEscapingTestCase(unittest.TestCase):
    """Tests that text from a schema is escaped on every page."""

    MARKUP = "<b>bold</b> & <script>alert(1)</script>"
    """Text containing markup, used in the schema below."""

    def test_markup_escaped(self) -> None:
        """Markup in a schema's description, a table's name and description,
        and a column's name, shown in an index, is shown as text on every
        page, never used as markup.
        """
        schema = Schema.model_validate(
            {
                "name": "markup",
                "@id": "#markup",
                "description": f"Schema {self.MARKUP}",
                "tables": [
                    {
                        "name": "tab<u>le</u>",
                        "@id": "#table",
                        "description": f"Table {self.MARKUP}",
                        "columns": [
                            {
                                "name": "co<u>l</u>",
                                "@id": "#table.col",
                                "datatype": "int",
                                "description": "Key",
                            }
                        ],
                        "indexes": [{"name": "idx_col", "@id": "#idx_col", "columns": ["#table.col"]}],
                    }
                ],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            site = Path(tmpdir)
            build_site(site, [schema])
            pages = html_pages(site)
            self.assertEqual(len(pages), 3)
            for path in pages:
                with self.subTest(page=path):
                    soup = read_page(site, path)
                    self.assertEqual(soup.find_all(["b", "u"]), [])
                    # The site's own scripts only load files.
                    self.assertFalse(any("alert" in script.get_text() for script in soup.find_all("script")))
            # The text itself is shown as written.
            self.assertIn(f"Schema {self.MARKUP}", read_page(site, "index.html").get_text())
            schema_page = read_page(site, "schemas/markup/index.html")
            self.assertIn("tab<u>le</u>", schema_page.get_text())
            self.assertIn(f"Table {self.MARKUP}", schema_page.get_text())
            (table_page,) = (path for path in pages if path.startswith("schemas/markup/tab"))
            self.assertIn("co<u>l</u>", read_page(site, table_page).find("tr", id="index-idx-col").get_text())


class UnresolvedColumnTestCase(unittest.TestCase):
    """Tests of a column reference that isn't a column of the schema."""

    def test_error(self) -> None:
        """Rendering a site fails with an error naming the reference."""
        schema = Schema.model_validate(
            {
                "name": "refs",
                "@id": "#refs",
                "tables": [
                    {
                        "name": "data",
                        "@id": "#data",
                        "columns": [
                            {"name": "id", "@id": "#data.id", "datatype": "int", "description": "Key"}
                        ],
                        "indexes": [
                            {"name": "idx_missing", "@id": "#idx_missing", "columns": ["#data.missing"]}
                        ],
                    }
                ],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir, self.assertRaisesRegex(ValueError, "#data.missing"):
            build_site(Path(tmpdir), [schema])


class SiteConfigTestCase(unittest.TestCase):
    """Tests of reading a site configuration file."""

    def test_full_configuration(self) -> None:
        """Every setting is read from the file, with the stylesheet, icon and
        favicon paths taken as relative to the file's directory.
        """
        config = SiteConfig.from_file(SITE_CONFIG)
        directory = SITE_CONFIG.parent
        self.assertEqual(config.title, "Test Schemas")
        self.assertEqual(config.stylesheets, [directory / "theme.css", directory / "local.css"])
        self.assertEqual(config.icon, directory / "logo.svg")
        self.assertEqual(config.favicon, directory / "favicon.svg")
        assert config.maintainer is not None
        self.assertEqual(config.maintainer.name, "Test Observatory")
        self.assertTrue(config.diagrams)
        self.assertTrue(config.show_empty_columns)

    def test_empty_file(self) -> None:
        """An empty configuration file gives the default settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "site.yaml"
            path.write_text("", encoding="utf-8")
            self.assertEqual(SiteConfig.from_file(path), SiteConfig())

    def test_invalid(self) -> None:
        """A configuration file with an unknown setting, a missing file or a
        maintainer without a name is rejected.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "site.yaml"
            for text in (
                "titel: Misspelled\n",
                "stylesheets: [missing.css]\n",
                "icon: missing.png\n",
                "favicon: missing.png\n",
                "maintainer:\n  url: https://example.org/\n",
            ):
                with self.subTest(config=text):
                    path.write_text(text, encoding="utf-8")
                    with self.assertRaises(ValidationError):
                        SiteConfig.from_file(path)
