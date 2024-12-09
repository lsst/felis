"""Compare schemas and print the differences."""

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

import logging
import pprint
import re
from collections.abc import Callable
from typing import Any

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from deepdiff.diff import DeepDiff
from sqlalchemy import Engine, MetaData

from .datamodel import Schema
from .metadata import MetaDataBuilder

__all__ = ["SchemaDiff", "DatabaseDiff"]

logger = logging.getLogger(__name__)

# Change alembic log level to avoid unnecessary output
logging.getLogger("alembic").setLevel(logging.WARNING)


class SchemaDiff:
    """
    Compare two schemas using DeepDiff and print the differences.

    Parameters
    ----------
    schema1
        The first schema to compare.
    schema2
        The second schema to compare.
    """

    def __init__(self, schema1: Schema, schema2: Schema):
        self.dict1 = schema1.model_dump(exclude_none=True)
        self.dict2 = schema2.model_dump(exclude_none=True)
        self.diff = DeepDiff(self.dict1, self.dict2, ignore_order=True)

    def print(self) -> None:
        """
        Print the differences between the two schemas.
        """
        pprint.pprint(self.diff)

    @property
    def has_changes(self) -> bool:
        """
        Check if there are any differences between the two schemas.

        Returns
        -------
        bool
            True if there are differences, False otherwise.
        """
        return len(self.diff) > 0


class FormattedSchemaDiff(SchemaDiff):
    """
    Compare two schemas using DeepDiff and print the differences using a
    customized output format.

    Parameters
    ----------
    schema1
        The first schema to compare.
    schema2
        The second schema to compare.
    """

    def __init__(self, schema1: Schema, schema2: Schema):
        super().__init__(schema1, schema2)

    def print(self) -> None:
        """
        Print the differences between the two schemas using a custom format.
        """
        handlers: dict[str, Callable[[dict[str, Any]], None]] = {
            "values_changed": self._handle_values_changed,
            "iterable_item_added": self._handle_iterable_item_added,
            "iterable_item_removed": self._handle_iterable_item_removed,
            "dictionary_item_added": self._handle_dictionary_item_added,
            "dictionary_item_removed": self._handle_dictionary_item_removed,
        }

        for change_type, handler in handlers.items():
            if change_type in self.diff:
                handler(self.diff[change_type])

    def _print_header(self, id_dict: dict[str, Any], keys: list[int | str]) -> None:
        id = self._get_id(id_dict, keys)
        print(f"{id} @ {self._get_key_display(keys)}")

    def _handle_values_changed(self, changes: dict[str, Any]) -> None:
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            value1 = self._get_value_from_keys(self.dict1, keys)
            value2 = self._get_value_from_keys(self.dict2, keys)
            self._print_header(self.dict1, keys)
            print(f"- {value1}")
            print(f"+ {value2}")

    def _handle_iterable_item_added(self, changes: dict[str, Any]) -> None:
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            value = self._get_value_from_keys(self.dict2, keys)
            self._print_header(self.dict2, keys)
            print(f"+ {value}")

    def _handle_iterable_item_removed(self, changes: dict[str, Any]) -> None:
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            value = self._get_value_from_keys(self.dict1, keys)
            self._print_header(self.dict1, keys)
            print(f"- {value}")

    def _handle_dictionary_item_added(self, changes: dict[str, Any]) -> None:
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            value = self._get_value_from_keys(self.dict2, keys)
            self._print_header(self.dict2, keys)
            print(f"+ {value}")

    def _handle_dictionary_item_removed(self, changes: dict[str, Any]) -> None:
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            value = self._get_value_from_keys(self.dict1, keys)
            self._print_header(self.dict1, keys)
            print(f"- {value}")

    @staticmethod
    def _get_id(values: dict, keys: list[str | int]) -> str:
        value = values
        last_id = None

        for key in keys:
            if isinstance(value, dict) and "id" in value:
                last_id = value["id"]
            value = value[key]

        if isinstance(value, dict) and "id" in value:
            last_id = value["id"]

        if last_id is not None:
            return last_id
        else:
            raise ValueError("No 'id' found in the specified path")

    @staticmethod
    def _get_key_display(keys: list[str | int]) -> str:
        return ".".join(str(k) for k in keys)

    @staticmethod
    def _parse_deepdiff_path(path: str) -> list[str | int]:
        if path.startswith("root"):
            path = path[4:]

        pattern = re.compile(r"\['([^']+)'\]|\[(\d+)\]")
        matches = pattern.findall(path)

        keys = []
        for match in matches:
            if match[0]:  # String key
                keys.append(match[0])
            elif match[1]:  # Integer index
                keys.append(int(match[1]))

        return keys

    @staticmethod
    def _get_value_from_keys(data: dict, keys: list[str | int]) -> Any:
        value = data
        for key in keys:
            value = value[key]
        return value


class DatabaseDiff(SchemaDiff):
    """
    Compare a schema with a database and print the differences.

    Parameters
    ----------
    schema
        The schema to compare.
    engine
        The database engine to compare with.
    """

    def __init__(self, schema: Schema, engine: Engine):
        db_metadata = MetaData()
        with engine.connect() as connection:
            db_metadata.reflect(bind=connection)
            mc = MigrationContext.configure(
                connection, opts={"compare_type": True, "target_metadata": db_metadata}
            )
            schema_metadata = MetaDataBuilder(schema, apply_schema_to_metadata=False).build()
            self.diff = compare_metadata(mc, schema_metadata)

    def print(self) -> None:
        """
        Print the differences between the schema and the database.
        """
        if self.has_changes:
            pprint.pprint(self.diff)
