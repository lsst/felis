from pyparsing import Word, alphas, nums, Optional, Suppress, OneOrMore, Combine, delimitedList
# from .utils import get_dialect_module  # Need DM-44721 merged
from ..datamodel import _DIALECT_MODULES


def _create_parser():
    """Create a parser for SQL type strings."""
    identifier = Combine(OneOrMore(Word(alphas) + Optional(Suppress(" "))), joinString="_", adjacent=False)
    number = Word(nums).setParseAction(lambda t: int(t[0]))
    parameters = Suppress("(") + Optional(delimitedList(number, delim=",")("params")) + Suppress(")")
    return identifier("type_name") + Optional(parameters)


class SQLTypeParser:
    """Parses SQL type strings into SQLAlchemy types."""

    def __init__(self, dialect_name: str):
        # Need DM-44721 merged
        # self.dialect_module = get_dialect_module(dialect_name)
        self.dialect_module = _DIALECT_MODULES[dialect_name]
        self.parser = _create_parser()
        self.tokens = None

    def _get_sqlalchemy_type(self, datatype):

        # Replace spaces with underscores for types such as "DOUBLE PRECISION"
        datatype = datatype.replace(" ", "_").upper()

        print(f"Finding datatype: {datatype}")

        # Check if the datatype exists in the dialect module
        if hasattr(self.dialect_module, datatype):
            return getattr(self.dialect_module, datatype)
        else:
            raise ValueError(f"Unknown type: {datatype}")

    def _get_params(self):
        return self.tokens.params.asList() if "params" in self.tokens else []

    def parse(self, sql_type_str):
        self.tokens = self.parser.parseString(sql_type_str)
        print(f"parsed: {self.tokens}")
        type_name = self.tokens.type_name.upper()
        type_func = self._get_sqlalchemy_type(type_name)
        params = self._get_params()
        print(f"params: {params}")
        if params and len(params):
            return type_func(*params)
        else:
            return type_func()


class PostgresTypeParser(SQLTypeParser):
    """Parses PostgreSQL type strings into SQLAlchemy types."""

    def __init__(self):
        super().__init__("postgresql")
        self.timezone = Optional(
            Suppress("WITH TIME ZONE").setParseAction(lambda: True) | Suppress("WITHOUT TIME ZONE"),
            default=None,
        )("timezone")
        self.parser = self.parser + self.timezone

    def _get_params(self):
        params = super()._get_params()
        if self.timezone is True:
            params.insert(0, True)
        return params
