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
from typing import IO, Any

import sqlalchemy
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from deepdiff.diff import DeepDiff
from deepdiff.model import DiffLevel
from sqlalchemy import Engine, MetaData

from .datamodel import Schema
from .metadata import MetaDataBuilder

__all__ = ["DatabaseDiff", "FormattedSchemaDiff", "SchemaDiff"]

logger = logging.getLogger(__name__)

# Change alembic log level to avoid unnecessary output
logging.getLogger("alembic").setLevel(logging.WARNING)


def _normalize_lists_by_name(obj: Any) -> Any:
    """
    Recursively normalize structures:
    - Lists of dicts under specified keys become dicts keyed by 'name'.
    - Lists of strings under specified keys become sorted lists.
    - Everything else is recursively normalized in place.

    Parameters
    ----------
    obj
        The object to normalize, which can be a list, dict, or any other type.
    """
    dict_like_keys = {"tables", "columns", "constraints", "indexes", "column_groups"}
    set_like_keys = {"columns", "referencedColumns"}

    if isinstance(obj, list):
        return [_normalize_lists_by_name(item) for item in obj]

    elif isinstance(obj, dict):
        normalized: dict[str, Any] = {}

        for k, v in obj.items():
            if isinstance(v, list):
                if k in dict_like_keys and all(isinstance(i, dict) and "name" in i for i in v):
                    logger.debug(f"Normalizing list of dicts under key '{k}' to dict keyed by 'name'")
                    normalized[k] = {i["name"]: _normalize_lists_by_name(i) for i in v}
                elif k in set_like_keys and all(isinstance(i, str) for i in v):
                    logger.debug(f"Normalizing list of strings under key '{k}' to sorted list: {v}")
                    normalized[k] = sorted(v)
                else:
                    normalized[k] = [_normalize_lists_by_name(i) for i in v]
            else:
                normalized[k] = _normalize_lists_by_name(v)

        return normalized

    else:
        return obj


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
    strip_ids
        Whether to strip '@id' fields from the schemas before comparison.

    Notes
    -----
    This class uses DeepDiff to compare two schemas and provides methods to
    retrieve the differences. It is designed to be extended for more structured
    output, such as in `FormattedSchemaDiff` and would not typically be used
    directly.
    """

    def __init__(
        self,
        schema_old: Schema,
        schema_new: Schema,
        table_filter: list[str] | None = None,
        strip_ids: bool = True,
    ):
        self.schema_old = copy.deepcopy(schema_old)
        self.schema_new = copy.deepcopy(schema_new)
        if table_filter:
            logger.debug(f"Filtering on tables: {table_filter}")
        self.table_filter = table_filter or []
        self.strip_ids = strip_ids
        self._create_diff()

    def _create_diff(self) -> dict[str, Any]:
        if self.table_filter:
            self.schema_old.tables = [
                table for table in self.schema_old.tables if table.name in self.table_filter
            ]
            logger.debug(f"Filtered old schema tables: {[table.name for table in self.schema_old.tables]}")
            self.schema_new.tables = [
                table for table in self.schema_new.tables if table.name in self.table_filter
            ]
            logger.debug(f"Filtered new schema tables: {[table.name for table in self.schema_new.tables]}")
        self.dict_old = _normalize_lists_by_name(self.schema_old._model_dump(strip_ids=self.strip_ids))
        self.dict_new = _normalize_lists_by_name(self.schema_new._model_dump(strip_ids=self.strip_ids))
        logger.debug(f"Normalized old dict:\n{json.dumps(self.dict_old, indent=2)}")
        logger.debug(f"Normalized new dict:\n{json.dumps(self.dict_new, indent=2)}")
        self._diff = DeepDiff(
            self.dict_old,
            self.dict_new,
            ignore_order=True,
            view="tree",
        )
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
        raise NotImplementedError("Subclasses must implement to_change_list()")

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


class DiffHandler:
    def collect(self, diff_items: list[DiffLevel]) -> list[dict[str, Any]]:
        """Collect differences from the provided diff items.

        Parameters
        ----------
        diff_items
            The list of differences to collect.
        """
        raise NotImplementedError


class ValuesChangedHandler(DiffHandler):
    def collect(self, diff_items: list[DiffLevel]) -> list[dict[str, Any]]:
        results = []
        for diff in diff_items:
            results.append(
                {
                    "change_type": diff.report_type,
                    "path": diff.path(),
                    "old_value": diff.t1,
                    "new_value": diff.t2,
                }
            )
        return results


class IterableItemAddedHandler(DiffHandler):
    def collect(self, diff_items: list[DiffLevel]) -> list[dict[str, Any]]:
        results = []
        for diff in diff_items:
            results.append(
                {
                    "change_type": diff.report_type,
                    "path": diff.path(),
                    "value": diff.t2,
                }
            )
        return results


class IterableItemRemovedHandler(DiffHandler):
    def collect(self, diff_items: list[DiffLevel]) -> list[dict[str, Any]]:
        results = []
        for diff in diff_items:
            results.append(
                {
                    "change_type": diff.report_type,
                    "path": diff.path(),
                    "value": diff.t1,
                }
            )
        return results


class DictionaryItemAddedHandler(DiffHandler):
    def collect(self, diff_items: list[DiffLevel]) -> list[dict[str, Any]]:
        results = []
        for diff in diff_items:
            keys = diff.path(output_format="list")
            added_key = keys[-1] if keys else None
            results.append(
                {
                    "change_type": diff.report_type,
                    "path": diff.path(),
                    "added_key": added_key,
                    "value": diff.t2,
                }
            )
        return results


class DictionaryItemRemovedHandler(DiffHandler):
    def collect(self, diff_items: list[DiffLevel]) -> list[dict[str, Any]]:
        results = []
        for diff in diff_items:
            keys = diff.path(output_format="list")
            removed_key = keys[-1] if keys else None
            results.append(
                {
                    "change_type": diff.report_type,
                    "path": diff.path(),
                    "removed_key": removed_key,
                    "value": diff.t1,
                }
            )
        return results


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

        # Define a mapping between types of changes and their handlers
        self.handlers = {
            "values_changed": ValuesChangedHandler(),
            "iterable_item_added": IterableItemAddedHandler(),
            "iterable_item_removed": IterableItemRemovedHandler(),
            "dictionary_item_added": DictionaryItemAddedHandler(),
            "dictionary_item_removed": DictionaryItemRemovedHandler(),
        }

    def to_change_list(self) -> list[dict[str, Any]]:
        """
        Convert differences to a structured format.

        Returns
        -------
        list[dict[str, Any]]
            List of changes in their dictionary representation.
        """
        changes = []

        for change_type, handler in self.handlers.items():
            if change_type in self.diff:
                changes.extend(handler.collect(self.diff[change_type]))

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
