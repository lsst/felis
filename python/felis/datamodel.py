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
from collections.abc import Mapping
from enum import Enum
from typing import Any, Literal

from astropy import units as units  # type: ignore
from astropy.io.votable import ucd  # type: ignore
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

__all__ = (
    "BaseObject",
    "Column",
    "Constraint",
    "CheckConstraint",
    "UniqueConstraint",
    "Index",
    "ForeignKeyConstraint",
    "Table",
    "SchemaVersion",
    "Schema",
)


class BaseObject(BaseModel):
    """Base class for all Felis objects."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", use_enum_values=True)
    """Configuration for the `BaseModel` class.

    Allow attributes to be populated by name and forbid extra attributes.
    """

    name: str
    """The name of the database object.

    All Felis database objects must have a name.
    """

    id: str = Field(alias="@id")
    """The unique identifier of the database object.

    All Felis database objects must have a unique identifier.
    """

    description: str | None = None
    """A description of the database object.

    The description is optional.
    """


class DataType(Enum):
    """`Enum` representing the data types supported by Felis."""

    BOOLEAN = "boolean"
    BYTE = "byte"
    SHORT = "short"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    CHAR = "char"
    STRING = "string"
    UNICODE = "unicode"
    TEXT = "text"
    BINARY = "binary"
    TIMESTAMP = "timestamp"


class Column(BaseObject):
    """A column in a table."""

    datatype: DataType
    """The datatype of the column."""

    length: int | None = None
    """The length of the column."""

    nullable: bool = True
    """Whether the column can be `NULL`."""

    value: Any = None
    """The default value of the column."""

    autoincrement: bool | None = None
    """Whether the column is autoincremented."""

    mysql_datatype: str | None = Field(None, alias="mysql:datatype")
    """The MySQL datatype of the column."""

    ivoa_ucd: str | None = Field(None, alias="ivoa:ucd")
    """The IVOA UCD of the column."""

    fits_tunit: str | None = Field(None, alias="fits:tunit")
    """The FITS TUNIT of the column."""

    ivoa_unit: str | None = Field(None, alias="ivoa:unit")
    """The IVOA unit of the column."""

    tap_column_index: int | None = Field(None, alias="tap:column_index")
    """The TAP_SCHEMA column index of the column."""

    tap_principal: int | None = Field(0, alias="tap:principal", ge=0, le=1)
    """Whether this is a TAP_SCHEMA principal column; can be either 0 or 1.

    This could be a boolean instead of 0 or 1.
    """

    votable_arraysize: int | Literal["*"] | None = Field(None, alias="votable:arraysize")
    """The VOTable arraysize of the column."""

    tap_std: int | None = Field(0, alias="tap:std", ge=0, le=1)
    """TAP_SCHEMA indication that this column is defined by an IVOA standard.
    """

    votable_utype: str | None = Field(None, alias="votable:utype")
    """The VOTable utype (usage-specific or unique type) of the column."""

    votable_xtype: str | None = Field(None, alias="votable:xtype")
    """The VOTable xtype (extended type) of the column."""

    @field_validator("ivoa_ucd")
    @classmethod
    def check_ivoa_ucd(cls, ivoa_ucd: str) -> str:
        """Check that IVOA UCD values are valid."""
        if ivoa_ucd is not None:
            try:
                ucd.parse_ucd(ivoa_ucd, check_controlled_vocabulary=True, has_colon=";" in ivoa_ucd)
            except ValueError as e:
                raise ValueError(f"Invalid IVOA UCD: {e}")
        return ivoa_ucd

    @model_validator(mode="before")
    @classmethod
    def check_units(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Check that units are valid."""
        fits_unit = values.get("fits:tunit")
        ivoa_unit = values.get("ivoa:unit")

        if fits_unit and ivoa_unit:
            raise ValueError("Column cannot have both FITS and IVOA units")
        unit = fits_unit or ivoa_unit

        if unit is not None:
            try:
                units.Unit(unit)
            except ValueError as e:
                raise ValueError(f"Invalid unit: {e}")

        return values


class Constraint(BaseObject):
    """A database table constraint."""

    deferrable: bool = False
    """If `True` then this constraint will be declared as deferrable."""

    initially: str | None = None
    """Value for ``INITIALLY`` clause, only used if ``deferrable`` is True."""

    annotations: Mapping[str, Any] = Field(default_factory=dict)
    """Additional annotations for this constraint."""

    type: str | None = Field(None, alias="@type")
    """The type of the constraint."""


class CheckConstraint(Constraint):
    """A check constraint on a table."""

    expression: str
    """The expression for the check constraint."""


class UniqueConstraint(Constraint):
    """A unique constraint on a table."""

    columns: list[str]
    """The columns in the unique constraint."""


class Index(BaseObject):
    """A database table index.

    An index can be defined on either columns or expressions, but not both.
    """

    columns: list[str] | None = None
    """The columns in the index."""

    expressions: list[str] | None = None
    """The expressions in the index."""

    @model_validator(mode="before")
    @classmethod
    def check_columns_or_expressions(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Check that columns or expressions are specified, but not both."""
        if "columns" in values and "expressions" in values:
            raise ValueError("Defining columns and expressions is not valid")
        elif "columns" not in values and "expressions" not in values:
            raise ValueError("Must define columns or expressions")
        return values


class ForeignKeyConstraint(Constraint):
    """A foreign key constraint on a table.

    These will be reflected in the TAP_SCHEMA keys and key_columns data.
    """

    columns: list[str]
    """The columns comprising the foreign key."""

    referenced_columns: list[str] = Field(alias="referencedColumns")
    """The columns referenced by the foreign key."""


class Table(BaseObject):
    """A database table."""

    columns: list[Column]
    """The columns in the table."""

    constraints: list[Constraint] = Field(default_factory=list)
    """The constraints on the table."""

    indexes: list[Index] = Field(default_factory=list)
    """The indexes on the table."""

    primaryKey: str | list[str] | None = None
    """The primary key of the table."""

    tap_table_index: int | None = Field(None, alias="tap:table_index")
    """The IVOA TAP_SCHEMA table index of the table."""

    mysql_engine: str | None = Field(None, alias="mysql:engine")
    """The mysql engine to use for the table.

    For now this is a freeform string but it could be constrained to a list of
    known engines in the future.
    """

    mysql_charset: str | None = Field(None, alias="mysql:charset")
    """The mysql charset to use for the table.

    For now this is a freeform string but it could be constrained to a list of
    known charsets in the future.
    """

    @model_validator(mode="before")
    @classmethod
    def create_constraints(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Create constraints from the ``constraints`` field."""
        if "constraints" in values:
            new_constraints: list[Constraint] = []
            for item in values["constraints"]:
                if item["@type"] == "ForeignKey":
                    new_constraints.append(ForeignKeyConstraint(**item))
                elif item["@type"] == "Unique":
                    new_constraints.append(UniqueConstraint(**item))
                elif item["@type"] == "Check":
                    new_constraints.append(CheckConstraint(**item))
                else:
                    raise ValueError(f"Unknown constraint type: {item['@type']}")
            values["constraints"] = new_constraints
        return values

    @field_validator("columns", mode="after")
    @classmethod
    def check_unique_column_names(cls, columns: list[Column]) -> list[Column]:
        """Check that column names are unique."""
        if len(columns) != len(set(column.name for column in columns)):
            raise ValueError("Column names must be unique")
        return columns


class SchemaVersion(BaseModel):
    """The version of the schema."""

    current: str
    """The current version of the schema."""

    compatible: list[str] | None = None
    """The compatible versions of the schema."""

    read_compatible: list[str] | None = None
    """The read compatible versions of the schema."""


class SchemaVisitor:
    """Visitor to build a Schema object's map of IDs to objects.

    Duplicates are added to a set when they are encountered, which can be
    accessed via the `duplicates` attribute. The presence of duplicates will
    not throw an error. Only the first object with a given ID will be added to
    the map, but this should not matter, since a ValidationError will be thrown
    by the `model_validator` method if any duplicates are found in the schema.

    This class is intended for internal use only.
    """

    def __init__(self) -> None:
        """Create a new SchemaVisitor."""
        self.schema: "Schema" | None = None
        self.duplicates: set[str] = set()

    def add(self, obj: BaseObject) -> None:
        """Add an object to the ID map."""
        if hasattr(obj, "id"):
            obj_id = getattr(obj, "id")
            if self.schema is not None:
                if obj_id in self.schema.id_map:
                    self.duplicates.add(obj_id)
                else:
                    self.schema.id_map[obj_id] = obj

    def visit_schema(self, schema: "Schema") -> None:
        """Visit the schema object that was added during initialization.

        This will set an internal variable pointing to the schema object.
        """
        self.schema = schema
        self.duplicates.clear()
        self.add(self.schema)
        for table in self.schema.tables:
            self.visit_table(table)

    def visit_table(self, table: Table) -> None:
        """Visit a table object."""
        self.add(table)
        for column in table.columns:
            self.visit_column(column)
        for constraint in table.constraints:
            self.visit_constraint(constraint)

    def visit_column(self, column: Column) -> None:
        """Visit a column object."""
        self.add(column)

    def visit_constraint(self, constraint: Constraint) -> None:
        """Visit a constraint object."""
        self.add(constraint)


class Schema(BaseObject):
    """The database schema."""

    version: SchemaVersion | str | None = None
    """The version of the schema."""

    tables: list[Table]
    """The tables in the schema."""

    id_map: dict[str, Any] = Field(default_factory=dict, exclude=True)
    """Map of IDs to objects."""

    @field_validator("tables", mode="after")
    @classmethod
    def check_unique_table_names(cls, tables: list[Table]) -> list[Table]:
        """Check that table names are unique."""
        if len(tables) != len(set(table.name for table in tables)):
            raise ValueError("Table names must be unique")
        return tables

    @model_validator(mode="after")
    def create_id_map(self) -> "Schema":
        """Create a map of IDs to objects."""
        visitor: SchemaVisitor = SchemaVisitor()
        visitor.visit_schema(self)
        logger.debug(f"ID map contains {len(self.id_map.keys())} objects")
        if len(visitor.duplicates):
            raise ValueError(
                "Duplicate IDs found in schema:\n    " + "\n    ".join(visitor.duplicates) + "\n"
            )
        return self

    def get_object_by_id(self, id: str) -> BaseObject:
        """Get an object by its unique "@id" field value.

        An error will be thrown if the object is not found.
        """
        if id not in self.id_map:
            raise ValueError(f"Object with ID {id} not found in schema")
        return self.id_map[id]
