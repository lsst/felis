from typing import Any, cast

from sqlalchemy import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    DATETIME,
    DOUBLE,
    FLOAT,
    INTEGER,
    SMALLINT,
    TEXT,
    TIMESTAMP,
    VARCHAR,
    Dialect,
    Unicode,
    create_mock_engine,
)
from sqlalchemy.types import TypeEngine

from .abstract import AbstractType, AbstractTypes, TypeFactory
from .properties import (
    BinaryProperties,
    BooleanProperties,
    CharProperties,
    FloatingPointProperties,
    IntegerProperties,
    StringProperties,
    TimestampProperties,
)

__all__ = ["SQLType", "SQLTypes"]


MYSQL = create_mock_engine("mysql://", executor=None).dialect
"""MySQL dialect."""

POSTGRESQL = create_mock_engine("postgresql://", executor=None).dialect
"""PostgreSQL dialect."""


class SQLType(AbstractType):
    """Base class for SQL datatypes."""

    sql_type: type[TypeEngine[Any]]
    """SQLAlchemy type for this type."""

    def __init__(
        self,
        length: int | None = None,
        dialect: Dialect | None = None,
        variant: TypeEngine[Any] | None = None,
        **kwargs: Any,
    ):
        """Initialize the SQL type."""
        super().__init__(length=length, **kwargs)
        self._dialect = dialect
        if variant:
            if not dialect:
                raise ValueError("dialect must be provided when variant is used")
        self._variant = variant

    @property
    def variant(self) -> TypeEngine[Any] | None:
        """Return the variant of the type."""
        return self._variant

    @property
    def dialect(self) -> Dialect | None:
        """Return the dialect of the type."""
        return self._dialect

    @property
    def type_string(self) -> str:
        """Return the string representation of the type, which is its compiled
        SQL string.
        """
        return self.type_object.compile(dialect=self.dialect)

    @property
    def type_object(self) -> TypeEngine[Any]:
        """Return a `~sqlalchemy.types.TypeEngine` object."""
        if self.length:
            params = {"length": self.length, **self.type_parameters}
        else:
            params = self.type_parameters
        st = self.__class__.sql_type(**params)
        if self.variant and self.dialect:
            st = st.with_variant(self.variant, self.dialect.name)
        return st


class SQLTypeFactory(TypeFactory):
    """Abstract factory for creating types."""

    def __init__(self) -> None:
        """Initialize the factory."""
        super().__init__(SQLType)


_TF = SQLTypeFactory()


class SQLTypes(AbstractTypes):
    """Type system for SQL databases."""

    type_classes = {
        "boolean": _TF.create(BooleanProperties, sql_type=BOOLEAN),
        "byte": _TF.create(IntegerProperties, sql_type=SMALLINT),
        "double": _TF.create(FloatingPointProperties, sql_type=DOUBLE),
        "short": _TF.create(IntegerProperties, sql_type=SMALLINT),
        "long": _TF.create(IntegerProperties, sql_type=BIGINT),
        "int": _TF.create(IntegerProperties, sql_type=INTEGER),
        "float": _TF.create(FloatingPointProperties, sql_type=FLOAT),
        "char": _TF.create(CharProperties, sql_type=CHAR),
        "string": _TF.create(StringProperties, sql_type=VARCHAR),
        "unicode": _TF.create(StringProperties, sql_type=Unicode),
        "text": _TF.create(StringProperties, sql_type=TEXT),
        "binary": _TF.create(BinaryProperties, sql_type=BINARY),
        "timestamp": _TF.create(TimestampProperties, sql_type=TIMESTAMP),
        "datetime": _TF.create(TimestampProperties, sql_type=DATETIME),
    }

    def __init__(self, dialect: Dialect | None = None):
        """Initialize the SQL type system."""
        self.dialect = dialect

    def create(self, type_name: str, **kwargs: Any) -> SQLType:
        """Create a SQL type."""
        variant = kwargs.pop("variant", None)
        return cast(
            SQLType, super().create(type_name, **{"dialect": self.dialect, "variant": variant, **kwargs})
        )
