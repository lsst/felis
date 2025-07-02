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

import copy
import json
import logging
import re
from typing import IO, Any

import sqlalchemy
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from deepdiff.diff import DeepDiff
from sqlalchemy import Engine, MetaData

from .datamodel import Schema
from .metadata import MetaDataBuilder

__all__ = ["DatabaseDiff", "FormattedSchemaDiff", "SchemaDiff"]

logger = logging.getLogger(__name__)

# Change alembic log level to avoid unnecessary output
logging.getLogger("alembic").setLevel(logging.WARNING)


class SchemaDiff:
    """
    Compare two schemas using DeepDiff and print the differences.

    Parameters
    ----------
    schema_old
        The old schema to compare, typically the original schema.
    schema_new
        The new schema to compare, typically the modified schema.
    table_filter
        A list of table names to filter on.

    Notes
    -----
    This class uses DeepDiff to compare two schemas and provides methods to
    retrieve the differences. It is designed to be extended for more structured
    output, such as in `FormattedSchemaDiff` and would not typically be used
    directly.
    """

    def __init__(self, schema_old: Schema, schema_new: Schema, table_filter: list[str] = []):
        self.schema_old = copy.deepcopy(schema_old)
        self.schema_new = copy.deepcopy(schema_new)
        if table_filter:
            logger.debug(f"Filtering on tables: {table_filter}")
        self.table_filter = table_filter
        self._create_diff()

    def _create_diff(self) -> dict[str, Any]:
        if self.table_filter:
            self.schema_old.tables = [
                table for table in self.schema_old.tables if table.name in self.table_filter
            ]
            self.schema_new.tables = [
                table for table in self.schema_new.tables if table.name in self.table_filter
            ]
        self.dict_old = self.schema_old.model_dump(exclude_none=True, exclude_defaults=True)
        self.dict_new = self.schema_new.model_dump(exclude_none=True, exclude_defaults=True)
        self._diff = DeepDiff(self.dict_old, self.dict_new, ignore_order=True)
        return self._diff

    @property
    def diff(self) -> dict[str, Any]:
        """
        Return the differences between the two schemas.

        Returns
        -------
        dict
            The differences between the two schemas.
        """
        return self._diff

    def to_change_list(self) -> list[dict[str, Any]]:
        """
        Convert differences to a structured format.

        Returns
        -------
        list[dict[str, Any]]
            List of change dictionaries.
        """
        return []  # Base implementation returns empty list

    def print(self, output_stream: IO[str] | None = None) -> None:
        """
        Print the differences between the two schemas in raw format.

        Parameters
        ----------
        output_stream
            The output stream for printing the differences.
        """
        print(self.diff, file=output_stream)

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
    Compare two schemas using DeepDiff and emit structured JSON differences.

    Parameters
    ----------
    schema_old
        The old schema to compare, typically the original schema.
    schema_new
        The new schema to compare, typically the modified schema.
    table_filter
        A list of table names to filter on.

    Notes
    -----
    This class extends `SchemaDiff` to provide a more structured output of
    differences. It formats the differences into a list of dictionaries, each
    representing a change with details such as change type, path, and values
    involved.

    Output dictionaries representing the changes are formatted as follows::

        {
            "change_type": str,
            "id": str,
            "path": str,
            "old_value": Any (for value changes),
            "new_value": Any (for value changes),
            "value": Any (for additions/removals)
        }

    The changes can be printed to JSON using the `print` method.
    """

    def __init__(self, schema_old: Schema, schema_new: Schema, table_filter: list[str] = []):
        super().__init__(schema_old, schema_new, table_filter)

    def to_change_list(self) -> list[dict[str, Any]]:
        """
        Convert differences to a structured format.

        Returns
        -------
        list[dict[str, Any]]
            List of changes in their dictionary representation.
        """
        changes = []

        handlers = {
            "values_changed": self._collect_values_changed,
            "iterable_item_added": self._collect_iterable_item_added,
            "iterable_item_removed": self._collect_iterable_item_removed,
            "dictionary_item_added": lambda paths: self._collect_dictionary_item_added(paths),
            "dictionary_item_removed": self._collect_dictionary_item_removed,
        }

        for change_type, handler in handlers.items():
            if change_type in self.diff:
                changes.extend(handler(self.diff[change_type]))

        return changes

    def print(self, output_stream: IO[str] | None = None) -> None:
        """
        Print the differences between the two schemas as JSON.

        Parameters
        ----------
        output_stream
            The output stream for printing the differences.
        """
        print(json.dumps(self.to_change_list(), indent=2), file=output_stream)

    def _get_id(self, source_dict: dict, keys: list[str | int]) -> str:
        """
        Extract the most relevant ID from the path using `_find_id` and return
        it. If no ID is found, return "unknown".

        Parameters
        ----------
        keys : list[str | int]
            The path to extract the ID from.

        Returns
        -------
        str
            The extracted ID.
        """
        try:
            return self._find_id(source_dict, keys)
        except ValueError:
            return "unknown"

    def _collect_values_changed(self, changes: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect value change differences."""
        results = []
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            results.append(
                {
                    "change_type": "values_changed",
                    "id": self._get_id(self.dict_old, keys),
                    "path": self._get_key_display(keys),
                    "old_value": changes[key]["old_value"],
                    "new_value": changes[key]["new_value"],
                }
            )
        return results

    def _collect_iterable_item_added(self, changes: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect iterable item addition differences."""
        results = []
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            results.append(
                {
                    "change_type": "iterable_item_added",
                    "id": self._get_id(self.dict_new, keys),
                    "path": self._get_key_display(keys),
                    "value": changes[key],
                }
            )
        return results

    def _collect_iterable_item_removed(self, changes: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect iterable item removal differences."""
        results = []
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            results.append(
                {
                    "change_type": "iterable_item_removed",
                    "id": self._get_id(self.dict_old, keys),
                    "path": self._get_key_display(keys),
                    "value": changes[key],
                }
            )
        return results

    @classmethod
    def _get_value_from_key(cls, data: Any, keys: list[str | int]) -> Any:
        for key in keys:
            data = data[key]  # step through nested dicts/lists
        return data

    def _collect_dictionary_item_added(self, paths: list[str]) -> list[dict[str, Any]]:
        """Collect dictionary item addition differences from DeepDiff path
        list.
        """
        results = []
        for path in paths:
            keys = self._parse_deepdiff_path(path)
            added_key = keys[-1]
            parent_keys = keys[:-1]
            try:
                value = self._get_value_from_key(self.dict_new, keys)
            except (KeyError, IndexError, TypeError):
                logger.warning(f"Could not resolve value for path: {path}")
                value = None
            results.append(
                {
                    "change_type": "dictionary_item_added",
                    "id": self._get_id(self.dict_new, keys),
                    "path": self._get_key_display(parent_keys),
                    "added_key": added_key,
                    "value": value,
                }
            )
        return results

    def _collect_dictionary_item_removed(self, changes: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect dictionary item removal differences."""
        results = []
        for key in changes:
            keys = self._parse_deepdiff_path(key)
            removed_key = keys[-1]
            parent_keys = keys[:-1]
            results.append(
                {
                    "change_type": "dictionary_item_removed",
                    "id": self._get_id(self.dict_old, keys),
                    "path": self._get_key_display(parent_keys),
                    "removed_key": removed_key,
                    "value": changes[key],
                }
            )
        return results

    @staticmethod
    def _find_id(values: dict, keys: list[str | int]) -> str:
        """Extract the most relevant ID from the path, usually the last 'id'
        found.
        """
        value: list | dict = values
        last_id = None

        for key in keys:
            logger.debug(f"Processing key <{key}> with type {type(key)}")
            logger.debug(f"Type of value: {type(value)}")

            # Store the ID if current value is a dict with an 'id' field
            if isinstance(value, dict) and "id" in value:
                last_id = value["id"]

            # Navigate to the next level
            if isinstance(value, dict) and key in value:
                value = value[key]
            elif isinstance(value, list) and isinstance(key, int):
                if 0 <= key < len(value):
                    value = value[key]
                else:
                    raise ValueError(f"Index '{key}' is out of range for list of length {len(value)}")
            else:
                raise ValueError(f"Key '{key}' not found in value of type {type(value)}")

        if last_id is not None:
            return last_id
        else:
            raise ValueError("No 'id' found in the specified path")

    @staticmethod
    def _get_key_display(keys: list[str | int]) -> str:
        """Convert keys list to a dot-notation path. If no keys are provided,
        we assume the root path.
        """
        return ".".join(str(k) for k in keys) if keys else "root"

    @staticmethod
    def _parse_deepdiff_path(path: str) -> list[str | int]:
        """Parse a DeepDiff path into a list of keys."""
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


class DatabaseDiff(SchemaDiff):
    """
    Compare a schema with a database and emit structured differences.

    Parameters
    ----------
    schema
        The schema to compare.
    engine
        The database engine to compare with.

    Notes
    -----
    The `DatabaseDiff` class uses SQLAlchemy to reflect the database schema
    and compare it with the provided `~felis.datamodel.Schema` object. It
    generates a list of differences between the two schemas, which can be
    printed or converted to a structured format.

    The error-handling during the reflection and comparison process is
    robust, catching various exceptions that may arise from database
    connectivity issues, invalid configurations, or unexpected errors.
    This is done because otherwise some obscure errors may be raised
    during the reflection process and configuration of alembic, which are not
    very informative to the user.
    """

    def __init__(self, schema: Schema, engine: Engine):
        self.schema = schema
        self.engine = engine
        self._generate_diff()

    def _generate_diff(self) -> None:
        """Generate the differences between the provided schema and
        database.
        """
        db_metadata = MetaData()
        with self.engine.connect() as connection:
            # Reflect the database schema
            try:
                db_metadata.reflect(bind=connection)
            except (sqlalchemy.exc.DatabaseError, sqlalchemy.exc.OperationalError) as e:
                raise RuntimeError(f"Database reflection failed: {e}") from e
            except AttributeError as e:  # Happens when no database is provided in the URL
                raise ValueError(
                    f"Invalid engine URL: <{self.engine.url}> (Missing database or schema?)"
                ) from e
            except sqlalchemy.exc.ArgumentError as e:
                raise ValueError(f"Invalid database URL or configuration: {e}") from e
            except Exception as e:
                raise RuntimeError(f"Unexpected error during database reflection: {e}") from e

            # Configure the alembic migration context using the reflected
            # metadata
            try:
                mc = MigrationContext.configure(
                    connection, opts={"compare_type": True, "target_metadata": db_metadata}
                )
            except (sqlalchemy.exc.DatabaseError, TypeError, ValueError) as e:
                raise RuntimeError(f"Migration context configuration failed: {e}") from e
            except Exception as e:
                raise RuntimeError(f"Unexpected error in migration context configuration: {e}") from e

            # Build the schema metadata for comparison
            try:
                schema_metadata = MetaDataBuilder(self.schema, apply_schema_to_metadata=False).build()
            except (ValueError, TypeError) as e:
                raise ValueError(f"Schema metadata construction failed: {e}") from e
            except Exception as e:
                raise RuntimeError(f"Unexpected error in schema metadata construction: {e}") from e

            # Compare the database metadata with the schema metadata
            try:
                self._diff = compare_metadata(mc, schema_metadata)
            except (sqlalchemy.exc.DatabaseError, AttributeError, TypeError) as e:
                raise RuntimeError(f"Metadata comparison failed: {e}") from e
            except Exception as e:
                raise RuntimeError(f"Unexpected error during metadata comparison: {e}") from e

    def to_change_list(self) -> list[dict[str, Any]]:
        """
        Convert database differences to structured format.

        Returns
        -------
        list[dict[str, Any]]
            List of database change dictionaries.
        """
        changes = []
        for change in self._diff:
            changes.append(
                {
                    "change_type": "database_diff",
                    "operation": str(change[0]) if change else "unknown",
                    "details": str(change) if change else "no details",
                }
            )
        return changes

    def print(self, output_stream: IO[str] | None = None) -> None:
        """
        Print the differences between the schema and the database as JSON.

        Parameters
        ----------
        output_stream
            The output stream for printing the differences.
        """
        print(json.dumps(self.to_change_list(), indent=2), file=output_stream)
