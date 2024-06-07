from logging import getLogger

from pyparsing import (
    Combine,
    OneOrMore,
    Optional,
    ParseException,
    ParserElement,
    QuotedString,
    Suppress,
    Word,
    alphas,
    delimitedList,
    nums,
)
from sqlalchemy import types

from ..datamodel import _DIALECT_MODULES, _DIALECTS

logger = getLogger("felis")


class SQLTypeParser:
    """Parse SQL type strings into SQLAlchemy types."""

    def __init__(self, dialect_name: str):
        if dialect_name not in _DIALECT_MODULES:
            logger.error(f"Unknown SQL dialect: {dialect_name}")
            raise ValueError(f"Unknown SQL dialect: {dialect_name}")
        self.dialect_module = _DIALECT_MODULES[dialect_name]
        self.dialect = _DIALECTS[dialect_name]
        self.parser = SQLTypeParser._create_parser()

    @classmethod
    @classmethod
    def _create_parser(cls) -> ParserElement:
        """Create a parser for SQL type strings."""
        identifier = Combine(
            OneOrMore(Word(alphas + "_") + Optional(Suppress(" "))),
            joinString="_",
            adjacent=False,
        )

        number = Word(nums).setParseAction(lambda t: int(t[0]))
        string = QuotedString(quoteChar="'", escChar="\\")
        parameters = Optional(
            Suppress("(")
            + Optional(delimitedList(number | string | identifier, delim=","))("params")
            + Suppress(")"),
            default=[],
        )

        return identifier("type_name") + parameters

    def _get_params(self):
        """Extract parameters from the parse results."""
        return self.parse_results.get("params", [])

    def parse(self, type_string: str):
        """Parse a type string into a SQLAlchemy type."""
        try:
            self.parse_results = self.parser.parseString(type_string)
        except ParseException as e:
            logger.error(f"Failed to parse type string: {type_string}")
            raise ValueError(f"Failed to parse type string: {type_string}") from e
        type_name = str(self.parse_results["type_name"])
        print("type name:", type_name, type(type_name))
        params = self._get_params()
        print("params:", params)
        try:
            type_func = getattr(self.dialect_module, type_name)
        except AttributeError:
            type_func = getattr(types, type_name, None)
        if type_func is None:
            raise ValueError(f"Unknown type {type_name}")

        if len(params):
            return type_func(*params)
        else:
            return type_func()


class PostgresTypeParser(SQLTypeParser):
    """Parses PostgreSQL type strings into SQLAlchemy types."""

    def __init__(self):
        super().__init__("postgresql")
        timezone = Optional(
            Suppress("WITH TIME ZONE").setParseAction(lambda: True)
            | Suppress("WITHOUT TIME ZONE").setParseAction(lambda: False),
            default=None,
        )("timezone")
        self.parser = self.parser + timezone

    def _get_params(self):
        """Extract parameters from the parse results, including timezone."""
        params = super()._get_params()
        if "timezone" in self.parse_results:
            params.insert(0, self.parse_results["timezone"])
        return params
