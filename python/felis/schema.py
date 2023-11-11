import io
import json
import os

import yaml
from jsonschema.validators import Draft7Validator

import felis

DEFAULT_SCHEMA_FILE = os.path.join(os.path.dirname(felis.__file__), "felis_schema.json")

# NOTES:
# - Making names or ids unique: https://groups.google.com/g/json-schema/c/z34YqedG-1s


def validate(file: io.TextIOBase, schema_file: str = DEFAULT_SCHEMA_FILE) -> None:
    """Validate a YAML file against a JSON schema.

    This function will raise an exception if the YAML file does not validate
    against the JSON schema.
    """
    print(f"Loading YAML file: {file.name}")
    data = yaml.safe_load(file)
    json_data = json.dumps(data)

    print(f"Using JSON schema: {schema_file}")
    with open(schema_file, "r") as sf:
        schema = json.load(sf)

    print("Validating YAML file against JSON schema")
    validator = Draft7Validator(schema)

    # Get list of errors based on:
    # https://python-jsonschema.readthedocs.io/en/latest/errors/
    errors = sorted(validator.iter_errors(json.loads(json_data)), key=lambda e: e.path)

    if len(errors) == 0:
        print("No errors in YAML file!")
    else:
        for error in errors:
            print(f"{list(error.path)}: {error.message}")
        raise Exception("Errors occurred validating felis schema")
