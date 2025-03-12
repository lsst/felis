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

import unittest
from typing import Any

from sqlalchemy import create_engine

from felis import Schema
from felis import datamodel as dm
from felis.diff import DatabaseDiff, FormattedSchemaDiff, SchemaDiff
from felis.metadata import MetaDataBuilder


class TestSchemaDiff(unittest.TestCase):
    """Test the SchemaDiff class."""

    def _diff(self, schema1: Schema, schema2: Schema) -> dict[str, Any]:
        return SchemaDiff(schema1, schema2).diff

    def test_schema_diff(self) -> None:
        """Test the comparison output generated by the SchemaDiff class."""
        # Two schemas with different values
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema2", id="#schema2", version="4.5.6", description="Schema 2", tables=[])
        diff = self._diff(schema1, schema2)
        self.assertSetEqual(
            set(diff.get("values_changed").keys()),
            set(f"root['{key}']" for key in ["name", "id", "version", "description"]),
        )

        # Call formatted handler function
        FormattedSchemaDiff(schema1, schema2)._handle_values_changed(diff["values_changed"])

        # Table added
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        diff = self._diff(schema1, schema2)
        self.assertIn("iterable_item_added", diff)
        self.assertIn("root['tables'][0]", diff["iterable_item_added"])

        # Call formatted handler function
        FormattedSchemaDiff(schema1, schema2)._handle_iterable_item_added(diff["iterable_item_added"])

        # Table removed
        schema2.tables.clear()
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        diff = self._diff(schema1, schema2)
        self.assertIn("iterable_item_removed", diff)
        self.assertIn("root['tables'][0]", diff["iterable_item_removed"])

        # Call formatted handler function
        FormattedSchemaDiff(schema1, schema2)._handle_iterable_item_removed(diff["iterable_item_removed"])

        # Different table descriptions
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 2", columns=[]))
        diff = self._diff(schema1, schema2)
        self.assertIn("values_changed", diff)
        self.assertIn("root['tables'][0]['description']", diff["values_changed"])
        old_value = diff["values_changed"]["root['tables'][0]['description']"]["old_value"]
        new_value = diff["values_changed"]["root['tables'][0]['description']"]["new_value"]
        self.assertEqual(old_value, "Table 1")
        self.assertEqual(new_value, "Table 2")

        # Two different tables
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables.append(dm.Table(name="table2", id="#table2", description="Table 2", columns=[]))
        diff = self._diff(schema1, schema2)
        self.assertSetEqual(
            set(diff.get("values_changed").keys()),
            set(f"root['tables'][0]['{key}']" for key in ["name", "id", "description"]),
        )

        # Two tables with different columns
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        diff = self._diff(schema1, schema2)
        self.assertIn("iterable_item_added", diff)
        self.assertIn("root['tables'][0]['columns'][0]", diff["iterable_item_added"])

        # Same columns in different order (no diff)
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema1.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        schema1.tables[0].columns.append(
            dm.Column(name="column2", datatype="string", length=256, id="#column2", description="Column 2")
        )
        schema2.tables[0].columns.append(
            dm.Column(name="column2", datatype="string", length=256, id="#column2", description="Column 2")
        )
        schema2.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        diff = self._diff(schema1, schema2)
        self.assertEqual(len(diff), 0)

        # Same columns with different descriptions
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema1.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        schema2.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 2")
        )
        diff = self._diff(schema1, schema2)
        self.assertIn("values_changed", diff)
        self.assertIn("root['tables'][0]['columns'][0]['description']", diff["values_changed"])
        old_value = diff["values_changed"]["root['tables'][0]['columns'][0]['description']"]["old_value"]
        new_value = diff["values_changed"]["root['tables'][0]['columns'][0]['description']"]["new_value"]
        self.assertEqual(old_value, "Column 1")
        self.assertEqual(new_value, "Column 2")

        # Added a field to a column
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema1.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        schema2.tables[0].columns.append(
            dm.Column(
                name="column1",
                datatype="string",
                length=256,
                id="#column1",
                description="Column 1",
                ivoa_ucd="meta.id;src;meta.main	",
            )
        )
        diff = self._diff(schema1, schema2)
        self.assertIn("dictionary_item_added", diff)
        self.assertIn("root['tables'][0]['columns'][0]['ivoa_ucd']", diff["dictionary_item_added"])

        # Call formatted handler function
        FormattedSchemaDiff(schema1, schema2)._handle_dictionary_item_added(diff["dictionary_item_added"])

        # Removed a field from a column
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema1.tables[0].columns.append(
            dm.Column(
                name="column1",
                datatype="string",
                length=256,
                id="#column1",
                description="Column 1",
                ivoa_ucd="meta.id;src;meta.main	",
            )
        )
        schema2.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        diff = self._diff(schema1, schema2)
        self.assertIn("dictionary_item_removed", diff)
        self.assertIn("root['tables'][0]['columns'][0]['ivoa_ucd']", diff["dictionary_item_removed"])

        # Call formatted handler function
        FormattedSchemaDiff(schema1, schema2)._handle_dictionary_item_removed(diff["dictionary_item_removed"])

    def test_index_diff(self) -> None:
        """Test differences in indices between tables."""
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema1.tables[0].columns.append(
            dm.Column(name="column1", datatype="int", id="#column1", description="Column 1")
        )
        schema1.tables[0].indexes.append(
            dm.Index(name="index1", id="#index1", description="Index 1", columns=["column1"])
        )

        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables[0].columns.append(
            dm.Column(name="column2", datatype="int", id="#column2", description="Column 2")
        )
        schema2.tables[0].indexes.append(
            dm.Index(name="index1", id="#index1", description="Index 1", columns=["column2"])
        )
        diff = self._diff(schema1, schema2)
        self.assertIn("values_changed", diff)
        self.assertIn("root['tables'][0]['indexes'][0]['columns'][0]", diff["values_changed"])
        new_value = diff["values_changed"]["root['tables'][0]['indexes'][0]['columns'][0]"]["new_value"]
        old_value = diff["values_changed"]["root['tables'][0]['indexes'][0]['columns'][0]"]["old_value"]
        self.assertEqual(old_value, "column1")
        self.assertEqual(new_value, "column2")

        # Print formatted diff to make sure it works for these changes
        FormattedSchemaDiff(schema1, schema2).print()

    def test_print(self) -> None:
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema2", id="#schema2", version="4.5.6", description="Schema 2", tables=[])
        SchemaDiff(schema1, schema2).print()

    def test_formatted_print(self) -> None:
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = dm.Schema(name="schema2", id="#schema2", version="4.5.6", description="Schema 2", tables=[])
        FormattedSchemaDiff(schema1, schema2).print()

    def test_parse_deepdiff_path(self) -> None:
        path = "root['tables'][0]['columns'][0]['ivoa_ucd']"
        keys = FormattedSchemaDiff._parse_deepdiff_path(path)
        self.assertListEqual(keys, ["tables", 0, "columns", 0, "ivoa_ucd"])

    def test_get_id_error(self) -> None:
        id_dict = {"tables": [{"indexes": [{"columns": [{"name": "column1"}, {"name": "column2"}]}]}]}
        keys = ["tables", 0, "indexes", 0, "columns", 0]
        with self.assertRaises(ValueError):
            FormattedSchemaDiff._get_id(id_dict, keys)

    def test_table_filter(self) -> None:
        schema1 = dm.Schema(
            name="schema1",
            id="#schema1",
            description="Schema 1",
            tables=[
                dm.Table(name="table1", id="#table1", description="Table 1", columns=[]),
                dm.Table(name="table2", id="#table2", description="Table 2", columns=[]),
                dm.Table(name="table3", id="#table3", description="Table 3", columns=[]),
            ],
        )

        schema2 = dm.Schema(
            name="schema1",
            id="#schema1",
            description="Schema 1",
            tables=[
                dm.Table(name="table1", id="#table1", description="Table 1", columns=[]),
                dm.Table(name="table2", id="#table2", description="Table 2", columns=[]),
            ],
        )

        diff = SchemaDiff(schema1, schema2, table_filter=["table1"]).diff
        self.assertEqual(len(diff), 0)

        diff = SchemaDiff(schema1, schema2, table_filter=["table2"]).diff
        self.assertEqual(len(diff), 0)

        diff = SchemaDiff(schema1, schema2, table_filter=["table3"]).diff
        self.assertEqual(len(diff), 1)
        self.assertTrue("iterable_item_removed" in diff)


class TestDatabaseDiff(unittest.TestCase):
    """Test the DatabaseDiff class."""

    def test_database_diff(self) -> None:
        """Test the comparison output generated by the DatabaseDiff class."""
        # Two tables with different columns
        schema1 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema1.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )

        schema2 = dm.Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2.tables.append(dm.Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables[0].columns.append(
            dm.Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        schema2.tables[0].columns.append(
            dm.Column(name="column2", datatype="string", length=256, id="#column2", description="Column 2")
        )

        metadata_db = MetaDataBuilder(schema1, apply_schema_to_metadata=False).build()
        engine = create_engine("sqlite:///:memory:")
        metadata_db.create_all(engine)

        db_diff = DatabaseDiff(schema2, engine)
        db_diff.print()

        self.assertEqual(db_diff.diff[0][0], "add_column")
