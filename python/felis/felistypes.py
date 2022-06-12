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
# along with this program. If not, see <https://www.gnu.org/licenses/>.


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
    timestamp=Timestamp,
)


VOTABLE_MAP = dict(
    boolean="boolean",
    byte="unsignedByte",
    short="short",
    int="int",
    long="long",
    float="float",
    double="double",
    char="char",  # arraysize must be nonzero
    string="char",  # arraysize must be nonzero
    unicode="unicodeChar",  # arraysize must be nonzero
    text="unicodeChar",  # arraysize must be nonzero
    binary="unsignedByte",  # arraysize must be nonzero
    timestamp="char",  # arraysize must be nonzero
)

TYPE_NAMES = NAME_MAP.keys()

NUMERIC_TYPES = {"byte", "short", "int", "long", "float", "double"}

LENGTH_TYPES = {"char", "string", "unicode", "text", "binary"}

DATETIME_TYPES = {"timestamp"}
