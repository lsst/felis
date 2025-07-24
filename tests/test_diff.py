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

import os
import unittest
from typing import Any

from sqlalchemy import create_engine

from felis import Column, Index, Schema, Table
from felis.diff import DatabaseDiff, FormattedSchemaDiff, SchemaDiff
from felis.metadata import MetaDataBuilder


class SchemaDiffTestCase(unittest.TestCase):
    """Test the SchemaDiff class."""

    def _diff(
        self, print_diff: bool = False, label: str = "", table_filter: list[str] | None = None
    ) -> dict[str, Any]:
        """Generate a diff between the two schemas managed by the test case,
        optionally printing the differences to the console for debugging
        purposes.
        """
        diff = SchemaDiff(self.sch1, self.sch2, table_filter=table_filter).diff
        if print_diff:
            print(label, "diff:", diff)
        return diff

    def setUp(self) -> None:
        """Set up two schemas for testing."""
        self.sch1: Schema = Schema(
            name="schema", id="#schema", version="1.0.0", description="Schema", tables=[]
        )
        self.sch2: Schema = Schema(
            name="schema", id="#schema", version="1.0.0", description="Schema", tables=[]
        )

    def test_schema_changed(self) -> None:
        """Test comparison of schemas with different attribute values."""
        self.sch2.name = "schema2"
        self.sch2.version = "1.0.1"
        self.sch2.description = "Schema 2"
        diff = self._diff()
        self.assertSetEqual(
            set(diff_level.path() for diff_level in diff["values_changed"]),
            set(f"root['{key}']" for key in ["name", "version", "description"]),
        )

    def test_table_added(self) -> None:
        """Test the addition of a table to a schema. Because of how the data is
        restructured for comparison, a table addition will show up as a
        dictionary item added.
        """
        self.sch2.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        diff = self._diff()
        self.assertIn("dictionary_item_added", diff)
        self.assertEqual(diff["dictionary_item_added"][0].path(), "root['tables']['table1']")

    def test_table_removed(self) -> None:
        """Test the removal of a table from a schema. Because of how the data
        is restructured for comparison, a table removal will show up as a
        dictionary item removed.
        """
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        diff = self._diff()
        # Because of how the data is restructured for comparison, a table
        # removal will show up as a dictionary item removed.
        self.assertIn("dictionary_item_removed", diff)
        self.assertEqual(diff["dictionary_item_removed"][0].path(), "root['tables']['table1']")

    def test_table_descriptions_changed(self) -> None:
        """Test the change of a table's description."""
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables.append(Table(name="table1", id="#table1", description="Table 1 changed", columns=[]))
        diff = self._diff()

        self.assertIn("values_changed", diff)
        values_changed = diff["values_changed"][0]
        self.assertEqual(values_changed.path(), "root['tables']['table1']['description']")
        self.assertEqual(values_changed.t1, "Table 1")
        self.assertEqual(values_changed.t2, "Table 1 changed")

    def test_tables_changed(self) -> None:
        """Test schemas with different tables."""
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables.append(Table(name="table2", id="#table2", description="Table 2", columns=[]))
        diff = self._diff()

        values_changed = diff["values_changed"][0]
        self.assertEqual(values_changed.path(), "root['tables']")
        self.assertEqual(
            values_changed.t1, {"table1": {"name": "table1", "description": "Table 1", "columns": {}}}
        )
        self.assertEqual(
            values_changed.t2, {"table2": {"name": "table2", "description": "Table 2", "columns": {}}}
        )

    def test_columns_changed(self) -> None:
        # Two tables with different columns
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables.append(
            Table(
                name="table1",
                id="#table1",
                description="Table 1",
                columns=[
                    Column(
                        name="column1", id="#column1", datatype="string", length=256, description="Column 1"
                    )
                ],
            )
        )
        diff = self._diff()
        dictionary_item_added = diff["dictionary_item_added"][0]
        self.assertEqual(dictionary_item_added.path(), "root['tables']['table1']['columns']['column1']")
        self.assertEqual(str(dictionary_item_added.t1), "not present")
        self.assertEqual(
            dictionary_item_added.t2,
            {
                "name": "column1",
                "description": "Column 1",
                "datatype": "string",
                "length": 256,
                "votable:arraysize": "256*",
            },
        )

    def test_column_order_changed(self) -> None:
        """Test the same columns in a different order. This should not be
        considered a difference.
        """
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch1.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        self.sch1.tables[0].columns.append(
            Column(name="column2", datatype="string", length=256, id="#column2", description="Column 2")
        )
        self.sch2.tables[0].columns.append(
            Column(name="column2", datatype="string", length=256, id="#column2", description="Column 2")
        )
        self.sch2.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        diff = self._diff()
        self.assertEqual(len(diff), 0)

    def test_column_description_changed(self) -> None:
        """Test the change of a column's description."""
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch1.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        self.sch2.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 2")
        )
        diff = self._diff()
        print("test_column_descriptions_changed diff:", diff)
        self.assertIn("values_changed", diff)
        values_changed = diff["values_changed"][0]
        self.assertEqual(
            values_changed.path(), "root['tables']['table1']['columns']['column1']['description']"
        )
        self.assertEqual(values_changed.t1, "Column 1")
        self.assertEqual(values_changed.t2, "Column 2")

    def test_field_added_to_column(self) -> None:
        """Test the addition of a field to a column."""
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch1.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        self.sch2.tables[0].columns.append(
            Column(
                name="column1",
                datatype="string",
                length=256,
                id="#column1",
                description="Column 1",
                ivoa_ucd="meta.id;src;meta.main	",
            )
        )
        diff = self._diff()
        print("test_field_added_to_column diff:", diff)
        self.assertIn("dictionary_item_added", diff)
        dictionary_item_added = diff["dictionary_item_added"][0]
        self.assertEqual(
            dictionary_item_added.path(), "root['tables']['table1']['columns']['column1']['ivoa:ucd']"
        )
        self.assertEqual(str(dictionary_item_added.t1), "not present")
        self.assertEqual(dictionary_item_added.t2, "meta.id;src;meta.main")

    def test_field_removed_from_column(self) -> None:
        """Test the removal of a field from a column."""
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch1.tables[0].columns.append(
            Column(
                name="column1",
                datatype="string",
                length=256,
                id="#column1",
                description="Column 1",
                ivoa_ucd="meta.id;src;meta.main	",
            )
        )
        self.sch2.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        diff = self._diff()
        print("test_field_removed_from_column diff:", diff)

        self.assertIn("dictionary_item_removed", diff)
        dictionary_item_removed = diff["dictionary_item_removed"][0]
        self.assertEqual(
            dictionary_item_removed.path(), "root['tables']['table1']['columns']['column1']['ivoa:ucd']"
        )
        self.assertEqual(dictionary_item_removed.t1, "meta.id;src;meta.main")
        self.assertEqual(str(dictionary_item_removed.t2), "not present")

    def test_index_columns_changed(self) -> None:
        """Test differences in indices between tables."""
        self.sch1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch1.tables[0].columns.append(
            Column(name="column1", datatype="int", id="#column1", description="Column 1")
        )
        self.sch1.tables[0].indexes.append(
            Index(name="index1", id="#index1", description="Index 1", columns=["column1"])
        )
        self.sch2.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        self.sch2.tables[0].columns.append(
            Column(name="column2", datatype="int", id="#column2", description="Column 2")
        )
        self.sch2.tables[0].indexes.append(
            Index(name="index1", id="#index1", description="Index 1", columns=["column2"])
        )
        diff = self._diff(print_diff=True, label="test_index_diff")
        values_changed = diff["values_changed"][1]
        self.assertEqual(values_changed.path(), "root['tables']['table1']['indexes']['index1']['columns'][0]")
        self.assertEqual(values_changed.t1, "column1")
        self.assertEqual(values_changed.t2, "column2")

    def test_schema_diff_print(self) -> None:
        schema1 = Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2 = Schema(name="schema2", id="#schema2", version="4.5.6", description="Schema 2", tables=[])
        SchemaDiff(schema1, schema2).print()

    def test_table_filter(self) -> None:
        self.sch1.tables.extend(
            [
                Table(name="table1", id="#table1", description="Table 1", columns=[]),
                Table(name="table2", id="#table2", description="Table 2", columns=[]),
                Table(name="table3", id="#table3", description="Table 3", columns=[]),
            ]
        )
        self.sch2.tables.extend(
            [
                Table(name="table1", id="#table1", description="Table 1", columns=[]),
                Table(name="table2", id="#table2", description="Table 2", columns=[]),
            ]
        )

        diff = self._diff(table_filter=["table1"])
        self.assertEqual(len(diff), 0)

        diff = self._diff(table_filter=["table2"])
        self.assertEqual(len(diff), 0)

        diff = self._diff(table_filter=["table3"], print_diff=True, label="test_table_filter")
        dictionary_item_removed = diff["dictionary_item_removed"][0]
        self.assertEqual(dictionary_item_removed.path(), "root['tables']['table3']")
        self.assertEqual(
            str(dictionary_item_removed.t1), "{'name': 'table3', 'description': 'Table 3', 'columns': {}}"
        )
        self.assertEqual(str(dictionary_item_removed.t2), "not present")


class FormattedSchemaDiffTestCase(unittest.TestCase):
    """Test the FormattedSchemaDiff class."""

    def test_formatted_diff_print(self) -> None:
        """Test the formatted output of the SchemaDiff by printing the
        differences between two YAML schema files files.
        """
        test_dir = os.path.abspath(os.path.dirname(__file__))
        test_diff1_path = os.path.join(test_dir, "data", "test_diff1.yaml")
        test_diff2_path = os.path.join(test_dir, "data", "test_diff2.yaml")

        context = {"id_generation": True}
        sch1 = Schema.from_uri(test_diff1_path, context=context)
        sch2 = Schema.from_uri(test_diff2_path, context=context)

        formatted_diff = FormattedSchemaDiff(sch1, sch2)
        formatted_diff.print()


class DatabaseDiffTestCase(unittest.TestCase):
    """Test the DatabaseDiff class."""

    def test_database_diff(self) -> None:
        """Test the comparison output generated by the DatabaseDiff class."""
        # Two tables with different columns
        schema1 = Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema1.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema1.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )

        schema2 = Schema(name="schema1", id="#schema1", version="1.2.3", description="Schema 1", tables=[])
        schema2.tables.append(Table(name="table1", id="#table1", description="Table 1", columns=[]))
        schema2.tables[0].columns.append(
            Column(name="column1", datatype="string", length=256, id="#column1", description="Column 1")
        )
        schema2.tables[0].columns.append(
            Column(name="column2", datatype="string", length=256, id="#column2", description="Column 2")
        )

        metadata_db = MetaDataBuilder(schema1, apply_schema_to_metadata=False).build()
        engine = create_engine("sqlite:///:memory:")
        metadata_db.create_all(engine)

        db_diff = DatabaseDiff(schema2, engine)
        db_diff.print()

        self.assertEqual(db_diff.diff[0][0], "add_column")
