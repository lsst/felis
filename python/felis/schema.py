import json
import os

# import jsonschema
import sys

import yaml
from jsonschema.validators import Draft7Validator

import felis

SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "@id": {"type": "string"},
        "description": {"type": "string"},
        "tables": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "@id": {"type": "string"},
                    "description": {"type": "string"},
                    "tap:table_index": {"type": "integer"},
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "@id": {"type": "string"},
                                "datatype": {"type": "string"},
                                "description": {"type": "string"},
                            },
                            "required": ["name", "@id", "datatype", "description"],
                        },
                    },
                },
                "required": ["name", "@id", "description", "columns"],
            },
        },
    },
    "required": ["name", "@id", "description", "tables"],
}

DEFAULT_SCHEMA_FILE = os.path.join(os.path.dirname(felis.__file__), "felis_schema.json")


def validate_yaml_file(fname: str, schema_file: str = DEFAULT_SCHEMA_FILE) -> int:
    """Validate a YAML file against a JSON schema."""
    print(f"Loading YAML file: {fname}")
    with open(fname, "r") as f:
        data = yaml.safe_load(f)
    json_data = json.dumps(data)

    print(f"Loading JSON schema: {schema_file}")
    with open(schema_file, "r") as sf:
        schema = json.load(sf)

    print("Validating YAML file against JSON schema")
    validator = Draft7Validator(schema)

    # https://python-jsonschema.readthedocs.io/en/latest/errors/
    errors = sorted(validator.iter_errors(json.loads(json_data)), key=lambda e: e.path)

    if len(errors) == 0:
        print("No errors in YAML file!")
    else:
        for error in errors:
            print(f"{list(error.path)}: {error.message}")

    return 0 if len(errors) == 0 else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("ERROR: Provide name of YAML file")

    sys.exit(validate_yaml_file(sys.argv[1]))
