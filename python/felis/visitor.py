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

__all__ = ["Visitor"]

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any, Generic, TypeVar

_Schema = TypeVar("_Schema")
_SchemaVersion = TypeVar("_SchemaVersion")
_Table = TypeVar("_Table")
_Column = TypeVar("_Column")
_PrimaryKey = TypeVar("_PrimaryKey")
_Constraint = TypeVar("_Constraint")
_Index = TypeVar("_Index")


class Visitor(ABC, Generic[_Schema, _Table, _Column, _PrimaryKey, _Constraint, _Index, _SchemaVersion]):
    """Abstract interface for visitor classes working on a Felis tree.

    Clients will only normally use `visit_schema` method, other methods
    defined in this interface should be called by implementation of
    `visit_schema` and the methods called from it.
    """

    @abstractmethod
    def visit_schema(self, schema_obj: Mapping[str, Any]) -> _Schema:
        """Visit Felis schema object.

        Parameters
        ----------
        schema_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a schema.

        Returns
        -------
        schema : `_Schema`
            Returns schema representation, type depends on implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_schema_version(
        self, version_obj: str | Mapping[str, Any], schema_obj: Mapping[str, Any]
    ) -> _SchemaVersion:
        """Visit schema version object.

        Parameters
        ----------
        version_obj : `str` or `Mapping` [ `str`, `Any` ]
            String of object describing schema version.
        schema_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent schema.

        Returns
        -------
        schema_version : `_SchemaVersion`
            Returns version representation, type depends on implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_table(self, table_obj: Mapping[str, Any], schema_obj: Mapping[str, Any]) -> _Table:
        """Visit Felis table object, this method is usually called from
        `visit_schema`, clients normally do not need to call it directly.

        Parameters
        ----------
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a table.
        schema_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent schema.

        Returns
        -------
        table : `_Table`
            Returns table representation, type depends on implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_column(self, column_obj: Mapping[str, Any], table_obj: Mapping[str, Any]) -> _Column:
        """Visit Felis column object, this method is usually called from
        `visit_table`, clients normally do not need to call it directly.

        Parameters
        ----------
        column_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a column.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Returns
        -------
        column : `_Column`
            Returns column representation, type depends on implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_primary_key(
        self, primary_key_obj: str | Iterable[str], table_obj: Mapping[str, Any]
    ) -> _PrimaryKey:
        """Visit Felis primary key object, this method is usually called from
        `visit_table`, clients normally do not need to call it directly.

        Parameters
        ----------
        primary_key_obj : `str` or `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a primary key.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Returns
        -------
        pk : `_PrimaryKey`
            Returns primary key representation, type depends on implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_constraint(
        self, constraint_obj: Mapping[str, Any], table_obj: Mapping[str, Any]
    ) -> _Constraint:
        """Visit Felis constraint object, this method is usually called from
        `visit_table`, clients normally do not need to call it directly.

        Parameters
        ----------
        constraint_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing a constraint.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Returns
        -------
        constraint : `_Constraint`
            Returns primary key representation, type depends on implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_index(self, index_obj: Mapping[str, Any], table_obj: Mapping[str, Any]) -> _Index:
        """Visit Felis index object, this method is usually called from
        `visit_table`, clients normally do not need to call it directly.

        Parameters
        ----------
        index_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing an index.
        table_obj : `Mapping` [ `str`, `Any` ]
            Felis object (mapping) representing parent table.

        Returns
        -------
        index : `_Index`
            Returns index representation, type depends on implementation.
        """
        raise NotImplementedError()
