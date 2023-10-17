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

__all__ = ["CheckingVisitor", "FelisValidator"]

import logging
from collections.abc import Iterable, Mapping, MutableSet
from typing import Any

from astropy import units as u  # type: ignore
from astropy.io.votable import ucd  # type: ignore

from .types import FelisType
from .visitor import Visitor

_Mapping = Mapping[str, Any]

logger = logging.getLogger("felis")


class FelisValidator:
    """Class defining methods for validating individual objects in a felis
    structure.

    The class implements all reasonable consistency checks for types of
    objects (mappings) that can appear in the Felis structure. It also
    verifies that object ID (``@id`` field) is unique, hence all check methods
    can only be called once for a given object.
    """

    def __init__(self) -> None:
        self._ids: MutableSet[str] = set()

    def check_schema(self, schema_obj: _Mapping) -> None:
        """Validate contents of Felis schema object.

        Parameters
        ----------
        schema_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a schema.

        Raises
        ------
        ValueError
            Raised if validation fails.
        """
        _id = self._assert_id(schema_obj)
        self._check_visited(_id)

    def check_schema_version(self, version_obj: Any, schema_obj: _Mapping) -> None:
        """Validate contents of schema description object.

        Parameters
        ----------
        version_obj : `Any`
            Felis object (should be string or mapping) representing schema
            version.
        schema_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent schema.

        Raises
        ------
        TypeError
            Raised if validation fails for expected types of items.
        ValueError
            Raised if validation fails for the content of the object.
        """
        if isinstance(version_obj, Mapping):
            # "current" is required, other keys are optional.
            possible_keys = {"current", "compatible", "read_compatible"}
            if not possible_keys.issuperset(version_obj):
                extra_keys = list(set(version_obj) - possible_keys)
                logger.error(f"unexpected keys in schema version description: {extra_keys}")
            if "current" not in version_obj:
                raise ValueError(f"missing 'current' key in schema version description: {version_obj}")
            if not isinstance(version_obj["current"], str):
                raise TypeError(f"schema version 'current' value is not a string: {version_obj['current']!r}")
            if (compatible := version_obj.get("compatible")) is not None:
                if not isinstance(compatible, list):
                    raise TypeError(f"schema version 'compatible' value is not a list: {compatible!r}")
                for item in compatible:
                    if not isinstance(item, str):
                        raise TypeError(f"items in 'compatible' value are not strings: {compatible!r}")
            if (read_compatible := version_obj.get("read_compatible")) is not None:
                if not isinstance(read_compatible, list):
                    raise TypeError(
                        f"schema version 'read_compatible' value is not a list: {read_compatible!r}"
                    )
                for item in read_compatible:
                    if not isinstance(item, str):
                        raise TypeError(
                            f"items in 'read_compatible' value are not strings: {read_compatible!r}"
                        )
        elif not isinstance(version_obj, str):
            raise TypeError(f"schema version description is not a string or object: {version_obj}")

    def check_table(self, table_obj: _Mapping, schema_obj: _Mapping) -> None:
        """Validate contents of Felis table object.

        Parameters
        ----------
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a table.
        schema_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent schema.

        Raises
        ------
        ValueError
            Raised if validation fails.
        """
        _id = self._assert_id(table_obj)
        self._assert_name(table_obj)
        self._check_visited(_id)

    def check_column(self, column_obj: _Mapping, table_obj: _Mapping) -> None:
        """Validate contents of Felis column object.

        Parameters
        ----------
        column_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a column.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Raises
        ------
        ValueError
            Raised if validation fails.
        """
        _id = self._assert_id(column_obj)
        self._assert_name(column_obj)
        datatype_name = self._assert_datatype(column_obj)
        length = column_obj.get("length")
        felis_type = FelisType.felis_type(datatype_name)
        if not length and (felis_type.is_sized or felis_type.is_timestamp):
            # This is not a warning, because it's usually fine
            logger.info(f"No length defined for {_id} for type {datatype_name}")

        # Check UCDs of columns
        ivoa_ucd = column_obj.get("ivoa:ucd")
        if ivoa_ucd:
            try:
                ucd.parse_ucd(ivoa_ucd, check_controlled_vocabulary=True, has_colon=";" in ivoa_ucd)
            except ValueError as e:
                logger.error(f"{e} in UCD '{ivoa_ucd}' for '{_id}'")

        # Check Units of columns
        fits_unit = column_obj.get("fits:tunit")
        ivoa_unit = column_obj.get("ivoa:unit")

        # There should only be one type of unit
        if fits_unit and ivoa_unit:
            logger.error("two types of units")
        elif fits_unit:
            unit = fits_unit
        elif ivoa_unit:
            unit = ivoa_unit
        else:
            unit = ""

        # Check the unit using astropy
        try:
            u.Unit(unit)
        except ValueError as e:
            logger.error(f"invalid unit for {_id} " + str(e))

        self._check_visited(_id)

    def check_primary_key(self, primary_key_obj: str | Iterable[str], table_obj: _Mapping) -> None:
        """Validate contents of Felis primary key object.

        Parameters
        ----------
        primary_key_obj : `str` or `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a primary key.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Raises
        ------
        ValueError
            Raised if validation fails.
        """
        pass

    def check_constraint(self, constraint_obj: _Mapping, table_obj: _Mapping) -> None:
        """Validate contents of Felis constraint object.

        Parameters
        ----------
        constraint_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a constraint.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Raises
        ------
        ValueError
            Raised if validation fails.
        """
        _id = self._assert_id(constraint_obj)
        constraint_type = constraint_obj.get("@type")
        if not constraint_type:
            raise ValueError(f"Constraint has no @type: {_id}")
        if constraint_type not in ["ForeignKey", "Check", "Unique"]:
            raise ValueError(f"Not a valid constraint type: {constraint_type}")
        self._check_visited(_id)

    def check_index(self, index_obj: _Mapping, table_obj: _Mapping) -> None:
        """Validate contents of Felis constraint object.

        Parameters
        ----------
        index_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing an index.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Raises
        ------
        ValueError
            Raised if validation fails.
        """
        _id = self._assert_id(index_obj)
        self._assert_name(index_obj)
        if "columns" in index_obj and "expressions" in index_obj:
            raise ValueError(f"Defining columns and expressions is not valid for index {_id}")
        self._check_visited(_id)

    def _assert_id(self, obj: _Mapping) -> str:
        """Verify that an object has a non-empty ``@id`` field.

        Parameters
        ----------
        obj : `Mapping` [ `str`, `Any` ]
            Felis object.

        Raises
        ------
        ValueError
            Raised if ``@id`` field is missing or empty.

        Returns
        -------
        id : `str`
            The value of ``@id`` field.
        """
        _id: str = obj.get("@id", "")
        if not _id:
            name = obj.get("name", "")
            maybe_string = f"(check object with name: {name})" if name else ""
            raise ValueError(f"No @id defined for object {maybe_string}")
        return _id

    def _assert_name(self, obj: _Mapping) -> None:
        """Verify that an object has a ``name`` field.

        Parameters
        ----------
        obj : `Mapping` [ `str`, `Any` ]
            Felis object.

        Raises
        ------
        ValueError
            Raised if ``name`` field is missing.
        """
        if "name" not in obj:
            _id = obj.get("@id")
            raise ValueError(f"No name for table object {_id}")

    def _assert_datatype(self, obj: _Mapping) -> str:
        """Verify that an object has a valid ``datatype`` field.

        Parameters
        ----------
        obj : `Mapping` [ `str`, `Any` ]
            Felis object.

        Raises
        ------
        ValueError
            Raised if ``datatype`` field is missing or invalid.

        Returns
        -------
        datatype : `str`
            The value of ``datatype`` field.
        """
        datatype_name: str = obj.get("datatype", "")
        _id = obj["@id"]
        if not datatype_name:
            raise ValueError(f"No datatype defined for id {_id}")
        try:
            FelisType.felis_type(datatype_name)
        except TypeError:
            raise ValueError(f"Incorrect Type Name for id {_id}: {datatype_name}") from None
        return datatype_name

    def _check_visited(self, _id: str) -> None:
        """Check that given ID has not been visited, generates a warning
        otherwise.

        Parameters
        ----------
        _id : `str`
            Felis object ID.
        """
        if _id in self._ids:
            logger.warning(f"Duplication of @id {_id}")
        self._ids.add(_id)


class CheckingVisitor(Visitor[None, None, None, None, None, None, None]):
    """Visitor implementation which validates felis structures and raises
    exceptions for errors.
    """

    def __init__(self) -> None:
        super().__init__()
        self.checker = FelisValidator()

    def visit_schema(self, schema_obj: _Mapping) -> None:
        # Docstring is inherited.
        self.checker.check_schema(schema_obj)
        if (version_obj := schema_obj.get("version")) is not None:
            self.visit_schema_version(version_obj, schema_obj)
        for table_obj in schema_obj["tables"]:
            self.visit_table(table_obj, schema_obj)

    def visit_schema_version(self, version_obj: str | Mapping[str, Any], schema_obj: _Mapping) -> None:
        # Docstring is inherited.
        self.checker.check_schema_version(version_obj, schema_obj)

    def visit_table(self, table_obj: _Mapping, schema_obj: _Mapping) -> None:
        # Docstring is inherited.
        self.checker.check_table(table_obj, schema_obj)
        for column_obj in table_obj["columns"]:
            self.visit_column(column_obj, table_obj)
        self.visit_primary_key(table_obj.get("primaryKey", []), table_obj)
        for constraint_obj in table_obj.get("constraints", []):
            self.visit_constraint(constraint_obj, table_obj)
        for index_obj in table_obj.get("indexes", []):
            self.visit_index(index_obj, table_obj)

    def visit_column(self, column_obj: _Mapping, table_obj: _Mapping) -> None:
        # Docstring is inherited.
        self.checker.check_column(column_obj, table_obj)

    def visit_primary_key(self, primary_key_obj: str | Iterable[str], table_obj: _Mapping) -> None:
        # Docstring is inherited.
        self.checker.check_primary_key(primary_key_obj, table_obj)

    def visit_constraint(self, constraint_obj: _Mapping, table_obj: _Mapping) -> None:
        # Docstring is inherited.
        self.checker.check_constraint(constraint_obj, table_obj)

    def visit_index(self, index_obj: _Mapping, table_obj: _Mapping) -> None:
        # Docstring is inherited.
        self.checker.check_index(index_obj, table_obj)
