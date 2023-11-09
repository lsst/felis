import json

# import jsonschema
import sys

import yaml
from jsonschema.validators import Draft7Validator

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


def validate_yaml_file(fname: str, schema: dict = SCHEMA) -> int:
    """Validate a YAML file against a JSON schema."""
    with open(fname, "r") as f:
        data = yaml.safe_load(f)

    json_data = json.dumps(data)

    validator = Draft7Validator(schema)

    # https://python-jsonschema.readthedocs.io/en/latest/errors/
    errors = sorted(validator.iter_errors(json.loads(json_data)), key=lambda e: e.path)

    rc = 0

    if len(errors) == 0:
        print("No errors!")
    else:
        for error in errors:
            print(f"{list(error.path)}: {error.message}")
        rc = 1

    return rc


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("ERROR: Provide name of YAML file")

    sys.exit(validate_yaml_file(sys.argv[1]))
