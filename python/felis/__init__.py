# LSST Data Management System
# Copyright 2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    __version__ = "source"

DEFAULT_CONTEXT = {
    "@vocab": "http://lsst.org/felis/",
    "mysql": "http://mysql.com/",
    "postgres": "http://posgresql.org/",
    "oracle": "http://oracle.com/database/",
    "sqlite": "http://sqlite.org/",
    "fits": "http://fits.gsfc.nasa.gov/FITS/4.0/",
    "ivoa": "http://ivoa.net/rdf/",
    "votable": "http://ivoa.net/rdf/VOTable/",
    "tap": "http://ivoa.net/documents/TAP/",
    "tables": {
        "@container": "@list",
        "@type": "@id",
        "@id": "felis:Table"
    },
    "columns": {
        "@container": "@list",
        "@type": "@id",
        "@id": "felis:Column"
    }
}

DEFAULT_FRAME = {
    "@context": DEFAULT_CONTEXT,
    "@type": "felis:Schema",
}
