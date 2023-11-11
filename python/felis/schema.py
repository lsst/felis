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

import io
import json
import logging
import os

import yaml
from jsonschema.validators import Draft7Validator

import felis

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_FILE = os.path.join(os.path.dirname(felis.__file__), "felis_schema.json")


def validate(file: io.TextIOBase, schema_file: str = DEFAULT_SCHEMA_FILE) -> None:
    """Validate a YAML file against a JSON schema.

    After printing error messages, this function will raise an exception if the
    YAML file does not validate against the JSON schema.
    """
    logger.info(f"Validating {file.name}")
    data = yaml.safe_load(file)
    json_data = json.dumps(data)

    logger.debug(f"Using JSON schema {schema_file}")
    with open(schema_file, "r") as sf:
        schema = json.load(sf)

    validator = Draft7Validator(schema)

    # Get list of errors based on:
    # https://python-jsonschema.readthedocs.io/en/latest/errors/
    errors = sorted(validator.iter_errors(json.loads(json_data)), key=lambda e: e.path)

    if len(errors) == 0:
        logger.info(f"No validation errors in {file.name}")
    else:
        for error in errors:
            logger.error(f"{list(error.path)}: {error.message}")
        raise Exception("Errors occurred validating felis schema")
