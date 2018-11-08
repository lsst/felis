

class FelisType:
    pass


class Boolean(FelisType):
    pass


class Byte(FelisType):
    pass


class Short(FelisType):
    pass


class Int(FelisType):
    pass


class Long(FelisType):
    pass


class Float(FelisType):
    pass


class Double(FelisType):
    pass


class Char(FelisType):
    pass


class String(FelisType):
    pass


class Unicode(FelisType):
    pass


class Text(FelisType):
    pass


class Binary(FelisType):
    pass


class Timestamp(FelisType):
    pass


NAME_MAP = dict(
    boolean=Boolean,
    byte=Byte,
    short=Short,
    int=Int,
    long=Long,
    float=Float,
    double=Double,
    char=Char,
    string=String,
    unicode=Unicode,
    text=Text,
    binary=Binary,
    timestamp=Timestamp
)

TYPE_NAMES = NAME_MAP.keys()

NUMERIC_TYPES = {
    "byte",
    "short",
    "int",
    "long",
    "float",
    "double"
}

LENGTH_TYPES = {
    "char",
    "string",
    "unicode",
    "text",
    "binary"
}