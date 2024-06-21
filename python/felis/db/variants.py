"""Handle variant datatype overrides on Felis columns."""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from sqlalchemy import types
from sqlalchemy.types import TypeEngine

from ..datamodel import Column
from .dialects import get_dialect_module, get_supported_dialects

__all__ = ["make_variant_dict"]


def _create_column_variant_overrides() -> dict[str, str]:
    """Map column variant overrides to their dialect name.

    Notes
    -----
    This function is intended for internal use only.
    """
    column_variant_overrides = {}
    for dialect_name in get_supported_dialects().keys():
        column_variant_overrides[f"{dialect_name}_datatype"] = dialect_name
    return column_variant_overrides


_COLUMN_VARIANT_OVERRIDES = MappingProxyType(_create_column_variant_overrides())
"""Map of column variant overrides to their dialect name."""


def _get_column_variant_overrides() -> Mapping[str, str]:
    """Get a dictionary of column variant overrides."""
    return _COLUMN_VARIANT_OVERRIDES


def _get_column_variant_override(field_name: str) -> str:
    """Get the dialect name from an override field name on the column like
    ``mysql_datatype``.

    Raises
    ------
    ValueError
        If the field name is not found in the column variant overrides.
    """
    if field_name not in _COLUMN_VARIANT_OVERRIDES:
        raise ValueError(f"Field name {field_name} not found in column variant overrides")
    return _COLUMN_VARIANT_OVERRIDES[field_name]


_length_regex = re.compile(r"\((\d+)\)")
"""A regular expression that is looking for numbers within parentheses."""


def _process_variant_override(dialect_name: str, variant_override_str: str) -> types.TypeEngine:
    """Get the variant type for given dialect.

    Parameters
    ----------
    dialect_name : `str`
        The name of the dialect to create.
    variant_override_str : `str`
        The string representation of the variant override.

    Raises
    ------
    ValueError
        If the type is not found in the dialect.
    """
    dialect = get_dialect_module(dialect_name)
    variant_type_name = variant_override_str.split("(")[0]

    # Process Variant Type
    if variant_type_name not in dir(dialect):
        raise ValueError(f"Type {variant_type_name} not found in dialect {dialect_name}")
    variant_type = getattr(dialect, variant_type_name)
    length_params = []
    if match := _length_regex.search(variant_override_str):
        length_params.extend([int(i) for i in match.group(1).split(",")])
    return variant_type(*length_params)


def make_variant_dict(column_obj: Column) -> dict[str, TypeEngine[Any]]:
    """Handle variant overrides for a `felis.datamodel.Column`.

    This function will return a dictionary of `str` to
    `sqlalchemy.types.TypeEngine` containing variant datatype information
    (e.g., for mysql, postgresql, etc).

    Parameters
    ----------
    column_obj : `felis.datamodel.Column`
        The column object from which to build the variant dictionary.

    Returns
    -------
    variant_dict : `dict`
        The dictionary of `str` to `sqlalchemy.types.TypeEngine` containing
        variant datatype information (e.g., for mysql, postgresql, etc).
    """
    variant_dict = {}
    variant_overrides = _get_column_variant_overrides()
    for field_name, value in iter(column_obj):
        if field_name in variant_overrides and value is not None:
            dialect = _get_column_variant_override(field_name)
            variant: TypeEngine = _process_variant_override(dialect, value)
            variant_dict[dialect] = variant
    return variant_dict
