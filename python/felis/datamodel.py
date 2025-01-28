"""Define Pydantic data models for Felis."""

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

import logging
from collections.abc import Sequence
from enum import StrEnum, auto
from typing import IO, Annotated, Any, Generic, Literal, TypeAlias, TypeVar, Union

import yaml
from astropy import units as units  # type: ignore
from astropy.io.votable import ucd  # type: ignore
from lsst.resources import ResourcePath, ResourcePathExpression
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from .db.dialects import get_supported_dialects
from .db.sqltypes import get_type_func
from .db.utils import string_to_typeengine
from .types import Boolean, Byte, Char, Double, FelisType, Float, Int, Long, Short, String, Text, Unicode

logger = logging.getLogger(__name__)

__all__ = (
    "BaseObject",
    "Column",
    "CheckConstraint",
    "Constraint",
    "ForeignKeyConstraint",
    "Index",
    "Schema",
    "SchemaVersion",
    "Table",
    "UniqueConstraint",
)

CONFIG = ConfigDict(
    populate_by_name=True,  # Populate attributes by name.
    extra="forbid",  # Do not allow extra fields.
    str_strip_whitespace=True,  # Strip whitespace from string fields.
)
"""Pydantic model configuration as described in:
https://docs.pydantic.dev/2.0/api/config/#pydantic.config.ConfigDict
"""

DESCR_MIN_LENGTH = 3
"""Minimum length for a description field."""

DescriptionStr: TypeAlias = Annotated[str, Field(min_length=DESCR_MIN_LENGTH)]
"""Type for a description, which must be three or more characters long."""


class BaseObject(BaseModel):
    """Base model.

    All classes representing objects in the Felis data model should inherit
    from this class.
    """

    model_config = CONFIG
    """Pydantic model configuration."""

    name: str
    """Name of the database object."""

    id: str = Field(alias="@id")
    """Unique identifier of the database object."""

    description: DescriptionStr | None = None
    """Description of the database object."""

    votable_utype: str | None = Field(None, alias="votable:utype")
    """VOTable utype (usage-specific or unique type) of the object."""

    @model_validator(mode="after")
    def check_description(self, info: ValidationInfo) -> BaseObject:
        """Check that the description is present if required.

        Parameters
        ----------
        info
            Validation context used to determine if the check is enabled.

        Returns
        -------
        `BaseObject`
            The object being validated.
        """
        context = info.context
        if not context or not context.get("check_description", False):
            return self
        if self.description is None or self.description == "":
            raise ValueError("Description is required and must be non-empty")
        if len(self.description) < DESCR_MIN_LENGTH:
            raise ValueError(f"Description must be at least {DESCR_MIN_LENGTH} characters long")
        return self


class DataType(StrEnum):
    """`Enum` representing the data types supported by Felis."""

    boolean = auto()
    byte = auto()
    short = auto()
    int = auto()
    long = auto()
    float = auto()
    double = auto()
    char = auto()
    string = auto()
    unicode = auto()
    text = auto()
    binary = auto()
    timestamp = auto()


def validate_ivoa_ucd(ivoa_ucd: str) -> str:
    """Validate IVOA UCD values.

    Parameters
    ----------
    ivoa_ucd
        IVOA UCD value to check.

    Returns
    -------
    `str`
        The IVOA UCD value if it is valid.

    Raises
    ------
    ValueError
        If the IVOA UCD value is invalid.
    """
    if ivoa_ucd is not None:
        try:
            ucd.parse_ucd(ivoa_ucd, check_controlled_vocabulary=True, has_colon=";" in ivoa_ucd)
        except ValueError as e:
            raise ValueError(f"Invalid IVOA UCD: {e}")
    return ivoa_ucd


class Column(BaseObject):
    """Column model."""

    datatype: DataType
    """Datatype of the column."""

    length: int | None = Field(None, gt=0)
    """Length of the column."""

    precision: int | None = Field(None, ge=0)
    """The numerical precision of the column.

    For timestamps, this is the number of fractional digits retained in the
    seconds field.
    """

    nullable: bool = True
    """Whether the column can be ``NULL``."""

    value: str | int | float | bool | None = None
    """Default value of the column."""

    autoincrement: bool | None = None
    """Whether the column is autoincremented."""

    mysql_datatype: str | None = Field(None, alias="mysql:datatype")
    """MySQL datatype override on the column."""

    postgresql_datatype: str | None = Field(None, alias="postgresql:datatype")
    """PostgreSQL datatype override on the column."""

    ivoa_ucd: str | None = Field(None, alias="ivoa:ucd")
    """IVOA UCD of the column."""

    fits_tunit: str | None = Field(None, alias="fits:tunit")
    """FITS TUNIT of the column."""

    ivoa_unit: str | None = Field(None, alias="ivoa:unit")
    """IVOA unit of the column."""

    tap_column_index: int | None = Field(None, alias="tap:column_index")
    """TAP_SCHEMA column index of the column."""

    tap_principal: int | None = Field(0, alias="tap:principal", ge=0, le=1)
    """Whether this is a TAP_SCHEMA principal column."""

    votable_arraysize: int | str | None = Field(None, alias="votable:arraysize")
    """VOTable arraysize of the column."""

    tap_std: int | None = Field(0, alias="tap:std", ge=0, le=1)
    """TAP_SCHEMA indication that this column is defined by an IVOA standard.
    """

    votable_xtype: str | None = Field(None, alias="votable:xtype")
    """VOTable xtype (extended type) of the column."""

    votable_datatype: str | None = Field(None, alias="votable:datatype")
    """VOTable datatype of the column."""

    @model_validator(mode="after")
    def check_value(self) -> Column:
        """Check that the default value is valid.

        Returns
        -------
        `Column`
            The column being validated.
        """
        if (value := self.value) is not None:
            if value is not None and self.autoincrement is True:
                raise ValueError("Column cannot have both a default value and be autoincremented")
            felis_type = FelisType.felis_type(self.datatype)
            if felis_type.is_numeric:
                if felis_type in (Byte, Short, Int, Long) and not isinstance(value, int):
                    raise ValueError("Default value must be an int for integer type columns")
                elif felis_type in (Float, Double) and not isinstance(value, float):
                    raise ValueError("Default value must be a decimal number for float and double columns")
            elif felis_type in (String, Char, Unicode, Text):
                if not isinstance(value, str):
                    raise ValueError("Default value must be a string for string columns")
                if not len(value):
                    raise ValueError("Default value must be a non-empty string for string columns")
            elif felis_type is Boolean and not isinstance(value, bool):
                raise ValueError("Default value must be a boolean for boolean columns")
        return self

    @field_validator("ivoa_ucd")
    @classmethod
    def check_ivoa_ucd(cls, ivoa_ucd: str) -> str:
        """Check that IVOA UCD values are valid.

        Parameters
        ----------
        ivoa_ucd
            IVOA UCD value to check.

        Returns
        -------
        `str`
            The IVOA UCD value if it is valid.
        """
        return validate_ivoa_ucd(ivoa_ucd)

    @model_validator(mode="after")
    def check_units(self) -> Column:
        """Check that the ``fits:tunit`` or ``ivoa:unit`` field has valid
        units according to astropy. Only one may be provided.

        Returns
        -------
        `Column`
            The column being validated.

        Raises
        ------
        ValueError
            Raised if both FITS and IVOA units are provided, or if the unit is
            invalid.
        """
        fits_unit = self.fits_tunit
        ivoa_unit = self.ivoa_unit

        if fits_unit and ivoa_unit:
            raise ValueError("Column cannot have both FITS and IVOA units")
        unit = fits_unit or ivoa_unit

        if unit is not None:
            try:
                units.Unit(unit)
            except ValueError as e:
                raise ValueError(f"Invalid unit: {e}")

        return self

    @model_validator(mode="before")
    @classmethod
    def check_length(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Check that a valid length is provided for sized types.

        Parameters
        ----------
        values
            Values of the column.

        Returns
        -------
        `dict` [ `str`, `Any` ]
            The values of the column.

        Raises
        ------
        ValueError
            Raised if a length is not provided for a sized type.
        """
        datatype = values.get("datatype")
        if datatype is None:
            # Skip this validation if datatype is not provided
            return values
        length = values.get("length")
        felis_type = FelisType.felis_type(datatype)
        if felis_type.is_sized and length is None:
            raise ValueError(
                f"Length must be provided for type '{datatype}'"
                + (f" in column '{values['@id']}'" if "@id" in values else "")
            )
        elif not felis_type.is_sized and length is not None:
            logger.warning(
                f"The datatype '{datatype}' does not support a specified length"
                + (f" in column '{values['@id']}'" if "@id" in values else "")
            )
        return values

    @model_validator(mode="after")
    def check_redundant_datatypes(self, info: ValidationInfo) -> Column:
        """Check for redundant datatypes on columns.

        Parameters
        ----------
        info
            Validation context used to determine if the check is enabled.

        Returns
        -------
        `Column`
            The column being validated.

        Raises
        ------
        ValueError
            Raised if a datatype override is redundant.
        """
        context = info.context
        if not context or not context.get("check_redundant_datatypes", False):
            return self
        if all(
            getattr(self, f"{dialect}:datatype", None) is not None
            for dialect in get_supported_dialects().keys()
        ):
            return self

        datatype = self.datatype
        length: int | None = self.length or None

        datatype_func = get_type_func(datatype)
        felis_type = FelisType.felis_type(datatype)
        if felis_type.is_sized:
            datatype_obj = datatype_func(length)
        else:
            datatype_obj = datatype_func()

        for dialect_name, dialect in get_supported_dialects().items():
            db_annotation = f"{dialect_name}_datatype"
            if datatype_string := self.model_dump().get(db_annotation):
                db_datatype_obj = string_to_typeengine(datatype_string, dialect, length)
                if datatype_obj.compile(dialect) == db_datatype_obj.compile(dialect):
                    raise ValueError(
                        "'{}: {}' is a redundant override of 'datatype: {}' in column '{}'{}".format(
                            db_annotation,
                            datatype_string,
                            self.datatype,
                            self.id,
                            "" if length is None else f" with length {length}",
                        )
                    )
                else:
                    logger.debug(
                        f"Type override of 'datatype: {self.datatype}' "
                        f"with '{db_annotation}: {datatype_string}' in column '{self.id}' "
                        f"compiled to '{datatype_obj.compile(dialect)}' and "
                        f"'{db_datatype_obj.compile(dialect)}'"
                    )
        return self

    @model_validator(mode="after")
    def check_precision(self) -> Column:
        """Check that precision is only valid for timestamp columns.

        Returns
        -------
        `Column`
            The column being validated.
        """
        if self.precision is not None and self.datatype != "timestamp":
            raise ValueError("Precision is only valid for timestamp columns")
        return self

    @model_validator(mode="before")
    @classmethod
    def check_votable_arraysize(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Set the default value for the ``votable_arraysize`` field, which
        corresponds to ``arraysize`` in the IVOA VOTable standard.

        Parameters
        ----------
        values
            Values of the column.

        Returns
        -------
        `dict` [ `str`, `Any` ]
            The values of the column.

        Notes
        -----
        Following the IVOA VOTable standard, an ``arraysize`` of 1 should not
        be used.
        """
        if values.get("name", None) is None or values.get("datatype", None) is None:
            # Skip bad column data that will not validate
            return values
        arraysize = values.get("votable:arraysize", None)
        if arraysize is None:
            length = values.get("length", None)
            datatype = values.get("datatype")
            if length is not None and length > 1:
                # Following the IVOA standard, arraysize of 1 is disallowed
                if datatype == "char":
                    arraysize = str(length)
                elif datatype in ("string", "unicode", "binary"):
                    arraysize = f"{length}*"
            elif datatype in ("timestamp", "text"):
                arraysize = "*"
            if arraysize is not None:
                values["votable:arraysize"] = arraysize
                logger.debug(
                    f"Set default 'votable:arraysize' to '{arraysize}' on column '{values['name']}'"
                    + f" with datatype '{values['datatype']}' and length '{values.get('length', None)}'"
                )
        else:
            logger.debug(f"Using existing 'votable:arraysize' of '{arraysize}' on column '{values['name']}'")
            if isinstance(values["votable:arraysize"], int):
                logger.warning(
                    f"Usage of an integer value for 'votable:arraysize' in column '{values['name']}' is "
                    + "deprecated"
                )
                values["votable:arraysize"] = str(arraysize)
        return values


class Constraint(BaseObject):
    """Table constraint model."""

    deferrable: bool = False
    """Whether this constraint will be declared as deferrable."""

    initially: Literal["IMMEDIATE", "DEFERRED"] | None = None
    """Value for ``INITIALLY`` clause; only used if `deferrable` is
    `True`."""

    @model_validator(mode="after")
    def check_deferrable(self) -> Constraint:
        """Check that the ``INITIALLY`` clause is only used if `deferrable` is
        `True`.

        Returns
        -------
        `Constraint`
            The constraint being validated.
        """
        if self.initially is not None and not self.deferrable:
            raise ValueError("INITIALLY clause can only be used if deferrable is True")
        return self


class CheckConstraint(Constraint):
    """Table check constraint model."""

    type: Literal["Check"] = Field("Check", alias="@type")
    """Type of the constraint."""

    expression: str
    """Expression for the check constraint."""


class UniqueConstraint(Constraint):
    """Table unique constraint model."""

    type: Literal["Unique"] = Field("Unique", alias="@type")
    """Type of the constraint."""

    columns: list[str]
    """Columns in the unique constraint."""


class ForeignKeyConstraint(Constraint):
    """Table foreign key constraint model.

    This constraint is used to define a foreign key relationship between two
    tables in the schema.

    Notes
    -----
    These relationships will be reflected in the TAP_SCHEMA ``keys`` and
    ``key_columns`` data.
    """

    type: Literal["ForeignKey"] = Field("ForeignKey", alias="@type")
    """Type of the constraint."""

    columns: list[str]
    """The columns comprising the foreign key."""

    referenced_columns: list[str] = Field(alias="referencedColumns")
    """The columns referenced by the foreign key."""


class Index(BaseObject):
    """Table index model.

    An index can be defined on either columns or expressions, but not both.
    """

    columns: list[str] | None = None
    """Columns in the index."""

    expressions: list[str] | None = None
    """Expressions in the index."""

    @model_validator(mode="before")
    @classmethod
    def check_columns_or_expressions(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Check that columns or expressions are specified, but not both.

        Parameters
        ----------
        values
            Values of the index.

        Returns
        -------
        `dict` [ `str`, `Any` ]
            The values of the index.

        Raises
        ------
        ValueError
            Raised if both columns and expressions are specified, or if neither
            are specified.
        """
        if "columns" in values and "expressions" in values:
            raise ValueError("Defining columns and expressions is not valid")
        elif "columns" not in values and "expressions" not in values:
            raise ValueError("Must define columns or expressions")
        return values


_ConstraintType = Annotated[
    Union[CheckConstraint, ForeignKeyConstraint, UniqueConstraint], Field(discriminator="type")
]
"""Type alias for a constraint type."""


ColumnRef: TypeAlias = str
"""Type alias for a column reference."""


class ColumnGroup(BaseObject):
    """Column group model."""

    columns: list[ColumnRef | Column] = Field(..., min_length=1)
    """Columns in the group."""

    ivoa_ucd: str | None = Field(None, alias="ivoa:ucd")
    """IVOA UCD of the column."""

    table: Table | None = None
    """Reference to the parent table."""

    @field_validator("ivoa_ucd")
    @classmethod
    def check_ivoa_ucd(cls, ivoa_ucd: str) -> str:
        """Check that IVOA UCD values are valid.

        Parameters
        ----------
        ivoa_ucd
            IVOA UCD value to check.

        Returns
        -------
        `str`
            The IVOA UCD value if it is valid.
        """
        return validate_ivoa_ucd(ivoa_ucd)

    @model_validator(mode="after")
    def check_unique_columns(self) -> ColumnGroup:
        """Check that the columns list contains unique items.

        Returns
        -------
        `ColumnGroup`
            The column group being validated.
        """
        column_ids = [col if isinstance(col, str) else col.id for col in self.columns]
        if len(column_ids) != len(set(column_ids)):
            raise ValueError("Columns in the group must be unique")
        return self

    def _dereference_columns(self) -> None:
        """Dereference ColumnRef to Column objects."""
        if self.table is None:
            raise ValueError("ColumnGroup must have a reference to its parent table")

        dereferenced_columns: list[ColumnRef | Column] = []
        for col in self.columns:
            if isinstance(col, str):
                # Dereference ColumnRef to Column object
                col_obj = self.table._find_column_by_id(col)
                dereferenced_columns.append(col_obj)
            else:
                dereferenced_columns.append(col)

        self.columns = dereferenced_columns


class Table(BaseObject):
    """Table model."""

    columns: Sequence[Column]
    """Columns in the table."""

    constraints: list[_ConstraintType] = Field(default_factory=list)
    """Constraints on the table."""

    indexes: list[Index] = Field(default_factory=list)
    """Indexes on the table."""

    column_groups: list[ColumnGroup] = Field(default_factory=list, alias="columnGroups")
    """Column groups in the table."""

    primary_key: str | list[str] | None = Field(None, alias="primaryKey")
    """Primary key of the table."""

    tap_table_index: int | None = Field(None, alias="tap:table_index")
    """IVOA TAP_SCHEMA table index of the table."""

    mysql_engine: str | None = Field("MyISAM", alias="mysql:engine")
    """MySQL engine to use for the table."""

    mysql_charset: str | None = Field(None, alias="mysql:charset")
    """MySQL charset to use for the table."""

    @field_validator("columns", mode="after")
    @classmethod
    def check_unique_column_names(cls, columns: list[Column]) -> list[Column]:
        """Check that column names are unique.

        Parameters
        ----------
        columns
            The columns to check.

        Returns
        -------
        `list` [ `Column` ]
            The columns if they are unique.

        Raises
        ------
        ValueError
            Raised if column names are not unique.
        """
        if len(columns) != len(set(column.name for column in columns)):
            raise ValueError("Column names must be unique")
        return columns

    @model_validator(mode="after")
    def check_tap_table_index(self, info: ValidationInfo) -> Table:
        """Check that the table has a TAP table index.

        Parameters
        ----------
        info
            Validation context used to determine if the check is enabled.

        Returns
        -------
        `Table`
            The table being validated.

        Raises
        ------
        ValueError
            Raised If the table is missing a TAP table index.
        """
        context = info.context
        if not context or not context.get("check_tap_table_indexes", False):
            return self
        if self.tap_table_index is None:
            raise ValueError("Table is missing a TAP table index")
        return self

    @model_validator(mode="after")
    def check_tap_principal(self, info: ValidationInfo) -> Table:
        """Check that at least one column is flagged as 'principal' for TAP
        purposes.

        Parameters
        ----------
        info
            Validation context used to determine if the check is enabled.

        Returns
        -------
        `Table`
            The table being validated.

        Raises
        ------
        ValueError
            Raised if the table is missing a column flagged as 'principal'.
        """
        context = info.context
        if not context or not context.get("check_tap_principal", False):
            return self
        for col in self.columns:
            if col.tap_principal == 1:
                return self
        raise ValueError(f"Table '{self.name}' is missing at least one column designated as 'tap:principal'")

    def _find_column_by_id(self, id: str) -> Column:
        """Find a column by ID.

        Parameters
        ----------
        id
            The ID of the column to find.

        Returns
        -------
        `Column`
            The column with the given ID.

        Raises
        ------
        ValueError
            Raised if the column is not found.
        """
        for column in self.columns:
            if column.id == id:
                return column
        raise ValueError(f"Column '{id}' not found in table '{self.name}'")

    @model_validator(mode="after")
    def dereference_column_groups(self: Table) -> Table:
        """Dereference columns in column groups.

        Returns
        -------
        `Table`
            The table with dereferenced column groups.
        """
        for group in self.column_groups:
            group.table = self
            group._dereference_columns()
        return self


class SchemaVersion(BaseModel):
    """Schema version model."""

    current: str
    """The current version of the schema."""

    compatible: list[str] = Field(default_factory=list)
    """The compatible versions of the schema."""

    read_compatible: list[str] = Field(default_factory=list)
    """The read compatible versions of the schema."""


class SchemaIdVisitor:
    """Visit a schema and build the map of IDs to objects.

    Notes
    -----
    Duplicates are added to a set when they are encountered, which can be
    accessed via the ``duplicates`` attribute. The presence of duplicates will
    not throw an error. Only the first object with a given ID will be added to
    the map, but this should not matter, since a ``ValidationError`` will be
    thrown by the ``model_validator`` method if any duplicates are found in the
    schema.
    """

    def __init__(self) -> None:
        """Create a new SchemaVisitor."""
        self.schema: Schema | None = None
        self.duplicates: set[str] = set()

    def add(self, obj: BaseObject) -> None:
        """Add an object to the ID map.

        Parameters
        ----------
        obj
            The object to add to the ID map.
        """
        if hasattr(obj, "id"):
            obj_id = getattr(obj, "id")
            if self.schema is not None:
                if obj_id in self.schema.id_map:
                    self.duplicates.add(obj_id)
                else:
                    self.schema.id_map[obj_id] = obj

    def visit_schema(self, schema: Schema) -> None:
        """Visit the objects in a schema and build the ID map.

        Parameters
        ----------
        schema
            The schema object to visit.

        Notes
        -----
        This will set an internal variable pointing to the schema object.
        """
        self.schema = schema
        self.duplicates.clear()
        self.add(self.schema)
        for table in self.schema.tables:
            self.visit_table(table)

    def visit_table(self, table: Table) -> None:
        """Visit a table object.

        Parameters
        ----------
        table
            The table object to visit.
        """
        self.add(table)
        for column in table.columns:
            self.visit_column(column)
        for constraint in table.constraints:
            self.visit_constraint(constraint)

    def visit_column(self, column: Column) -> None:
        """Visit a column object.

        Parameters
        ----------
        column
            The column object to visit.
        """
        self.add(column)

    def visit_constraint(self, constraint: Constraint) -> None:
        """Visit a constraint object.

        Parameters
        ----------
        constraint
            The constraint object to visit.
        """
        self.add(constraint)


T = TypeVar("T", bound=BaseObject)


class Schema(BaseObject, Generic[T]):
    """Database schema model.

    This represents a database schema, which contains one or more tables.
    """

    version: SchemaVersion | str | None = None
    """The version of the schema."""

    tables: Sequence[Table]
    """The tables in the schema."""

    id_map: dict[str, Any] = Field(default_factory=dict, exclude=True)
    """Map of IDs to objects."""

    @model_validator(mode="before")
    @classmethod
    def generate_ids(cls, values: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
        """Generate IDs for objects that do not have them.

        Parameters
        ----------
        values
            The values of the schema.
        info
            Validation context used to determine if ID generation is enabled.

        Returns
        -------
        `dict` [ `str`, `Any` ]
            The values of the schema with generated IDs.
        """
        context = info.context
        if not context or not context.get("id_generation", False):
            logger.debug("Skipping ID generation")
            return values
        schema_name = values["name"]
        if "@id" not in values:
            values["@id"] = f"#{schema_name}"
            logger.debug(f"Generated ID '{values['@id']}' for schema '{schema_name}'")
        if "tables" in values:
            for table in values["tables"]:
                if "@id" not in table:
                    table["@id"] = f"#{table['name']}"
                    logger.debug(f"Generated ID '{table['@id']}' for table '{table['name']}'")
                if "columns" in table:
                    for column in table["columns"]:
                        if "@id" not in column:
                            column["@id"] = f"#{table['name']}.{column['name']}"
                            logger.debug(f"Generated ID '{column['@id']}' for column '{column['name']}'")
                if "constraints" in table:
                    for constraint in table["constraints"]:
                        if "@id" not in constraint:
                            constraint["@id"] = f"#{constraint['name']}"
                            logger.debug(
                                f"Generated ID '{constraint['@id']}' for constraint '{constraint['name']}'"
                            )
                if "indexes" in table:
                    for index in table["indexes"]:
                        if "@id" not in index:
                            index["@id"] = f"#{index['name']}"
                            logger.debug(f"Generated ID '{index['@id']}' for index '{index['name']}'")
        return values

    @field_validator("tables", mode="after")
    @classmethod
    def check_unique_table_names(cls, tables: list[Table]) -> list[Table]:
        """Check that table names are unique.

        Parameters
        ----------
        tables
            The tables to check.

        Returns
        -------
        `list` [ `Table` ]
            The tables if they are unique.

        Raises
        ------
        ValueError
            Raised if table names are not unique.
        """
        if len(tables) != len(set(table.name for table in tables)):
            raise ValueError("Table names must be unique")
        return tables

    @model_validator(mode="after")
    def check_tap_table_indexes(self, info: ValidationInfo) -> Schema:
        """Check that the TAP table indexes are unique.

        Parameters
        ----------
        info
            The validation context used to determine if the check is enabled.

        Returns
        -------
        `Schema`
            The schema being validated.
        """
        context = info.context
        if not context or not context.get("check_tap_table_indexes", False):
            return self
        table_indicies = set()
        for table in self.tables:
            table_index = table.tap_table_index
            if table_index is not None:
                if table_index in table_indicies:
                    raise ValueError(f"Duplicate 'tap:table_index' value {table_index} found in schema")
                table_indicies.add(table_index)
        return self

    @model_validator(mode="after")
    def check_unique_constraint_names(self: Schema) -> Schema:
        """Check for duplicate constraint names in the schema.

        Returns
        -------
        `Schema`
            The schema being validated.

        Raises
        ------
        ValueError
            Raised if duplicate constraint names are found in the schema.
        """
        constraint_names = set()
        duplicate_names = []

        for table in self.tables:
            for constraint in table.constraints:
                constraint_name = constraint.name
                if constraint_name in constraint_names:
                    duplicate_names.append(constraint_name)
                else:
                    constraint_names.add(constraint_name)

        if duplicate_names:
            raise ValueError(f"Duplicate constraint names found in schema: {duplicate_names}")

        return self

    @model_validator(mode="after")
    def check_unique_index_names(self: Schema) -> Schema:
        """Check for duplicate index names in the schema.

        Returns
        -------
        `Schema`
            The schema being validated.

        Raises
        ------
        ValueError
            Raised if duplicate index names are found in the schema.
        """
        index_names = set()
        duplicate_names = []

        for table in self.tables:
            for index in table.indexes:
                index_name = index.name
                if index_name in index_names:
                    duplicate_names.append(index_name)
                else:
                    index_names.add(index_name)

        if duplicate_names:
            raise ValueError(f"Duplicate index names found in schema: {duplicate_names}")

        return self

    def _create_id_map(self: Schema) -> Schema:
        """Create a map of IDs to objects.

        Raises
        ------
        ValueError
            Raised if duplicate identifiers are found in the schema.

        Notes
        -----
        This is called automatically by the `model_post_init` method. If the
        ID map is already populated, this method will return immediately.
        """
        if len(self.id_map):
            logger.debug("Ignoring call to create_id_map() - ID map was already populated")
            return self
        visitor: SchemaIdVisitor = SchemaIdVisitor()
        visitor.visit_schema(self)
        if len(visitor.duplicates):
            raise ValueError(
                "Duplicate IDs found in schema:\n    " + "\n    ".join(visitor.duplicates) + "\n"
            )
        return self

    def model_post_init(self, ctx: Any) -> None:
        """Post-initialization hook for the model.

        Parameters
        ----------
        ctx
            The context object which was passed to the model.

        Notes
        -----
        This method is called automatically by Pydantic after the model is
        initialized. It is used to create the ID map for the schema.

        The ``ctx`` argument has the type `Any` because this is the function
        signature in Pydantic itself.
        """
        self._create_id_map()

    def __getitem__(self, id: str) -> BaseObject:
        """Get an object by its ID.

        Parameters
        ----------
        id
            The ID of the object to get.

        Raises
        ------
        KeyError
            Raised if the object with the given ID is not found in the schema.
        """
        if id not in self:
            raise KeyError(f"Object with ID '{id}' not found in schema")
        return self.id_map[id]

    def __contains__(self, id: str) -> bool:
        """Check if an object with the given ID is in the schema.

        Parameters
        ----------
        id
            The ID of the object to check.
        """
        return id in self.id_map

    def find_object_by_id(self, id: str, obj_type: type[T]) -> T:
        """Find an object with the given type by its ID.

        Parameters
        ----------
        id
            The ID of the object to find.
        obj_type
            The type of the object to find.

        Returns
        -------
        BaseObject
            The object with the given ID and type.

        Raises
        ------
        KeyError
            If the object with the given ID is not found in the schema.
        TypeError
            If the object that is found does not have the right type.

        Notes
        -----
        The actual return type is the user-specified argument ``T``, which is
        expected to be a subclass of `BaseObject`.
        """
        obj = self[id]
        if not isinstance(obj, obj_type):
            raise TypeError(f"Object with ID '{id}' is not of type '{obj_type.__name__}'")
        return obj

    def get_table_by_column(self, column: Column) -> Table:
        """Find the table that contains a column.

        Parameters
        ----------
        column
            The column to find.

        Returns
        -------
        `Table`
            The table that contains the column.

        Raises
        ------
        ValueError
            If the column is not found in any table.
        """
        for table in self.tables:
            if column in table.columns:
                return table
        raise ValueError(f"Column '{column.name}' not found in any table")

    @classmethod
    def from_uri(cls, resource_path: ResourcePathExpression, context: dict[str, Any] = {}) -> Schema:
        """Load a `Schema` from a string representing a ``ResourcePath``.

        Parameters
        ----------
        resource_path
            The ``ResourcePath`` pointing to a YAML file.
        context
            Pydantic context to be used in validation.

        Returns
        -------
        `str`
            The ID of the object.

        Raises
        ------
        yaml.YAMLError
            Raised if there is an error loading the YAML data.
        ValueError
            Raised if there is an error reading the resource.
        pydantic.ValidationError
            Raised if the schema fails validation.
        """
        logger.debug(f"Loading schema from: '{resource_path}'")
        try:
            rp_stream = ResourcePath(resource_path).read()
        except Exception as e:
            raise ValueError(f"Error reading resource from '{resource_path}' : {e}") from e
        yaml_data = yaml.safe_load(rp_stream)
        return Schema.model_validate(yaml_data, context=context)

    @classmethod
    def from_stream(cls, source: IO[str], context: dict[str, Any] = {}) -> Schema:
        """Load a `Schema` from a file stream which should contain YAML data.

        Parameters
        ----------
        source
            The file stream to read from.
        context
            Pydantic context to be used in validation.

        Returns
        -------
        `Schema`
            The Felis schema loaded from the stream.

        Raises
        ------
        yaml.YAMLError
            Raised if there is an error loading the YAML file.
        pydantic.ValidationError
            Raised if the schema fails validation.
        """
        logger.debug("Loading schema from: '%s'", source)
        yaml_data = yaml.safe_load(source)
        return Schema.model_validate(yaml_data, context=context)
