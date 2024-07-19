#################
Schema Validation
#################

Felis uses `Pydantic <https://docs.pydantic.dev/latest/>`__ to validate the schema and all of the objects that
it defines, including tables and columns.
Pydantic is a data validation library that uses Python type annotations to define the structure of the data
that is being validated.

Pydantic validators are defined as occuring either "before" conversion to the model objects or "after."
When a validation error is raised in the "before" stage, the "after" validators will not run.
This can be important to keep in mind, as fixing these "before" error may reveal others that occur in the
"after" stage.

After installing Felis, you can use the ``felis validate`` command to validate one or more schema files.
These should be in YAML format and conform to the
`Felis schema <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.Schema>`__.
The `command line documentation <cli.html#felis-validate>`_ provides more information on how to use this command.

Validation Errors
=================

If there are any errors in the schema, the validation process will raise an exception called a
`validation error <https://docs.pydantic.dev/latest/errors/validation_errors/>`_ containing a list of all the
errors that were found. These errors will be printed to the console, and the command line process will
return a non-zero exit code.
An error message will be printed to the log indicating how many errors were found.
For example:

::

    ERROR:felis:1 validation error for Schema

In the case where a field was included which is not present in the data model, such as a misspelled field
name, the error message will indicate the field name that was not recognized.
For example:

::

    tables.0.columns.0.not_a_field
      Extra inputs are not permitted [type=extra_forbidden, input_value=12345, input_type=int]
        For further information visit https://errors.pydantic.dev/2.8/v/extra_forbidden

This error message indicates that the field ``not_a_field`` was found in the schema but that this is not a
valid field according to the data model.
The error message also includes the value of the field and the type of the value.
The error occurred in the first table and the first column of the schema, indexed from zero.

Errors may also occur when a field has a value that is not valid.
For example:

::

    tables.0.columns.0.description
      String should have at least 3 characters [type=string_too_short, input_value='xx', input_type=str]
        For further information visit https://errors.pydantic.dev/2.8/v/string_too_short

Here the ``description`` field is too short; it must be at least 3 characters, which is indicated by the
``string_too_short`` error type.

An error will occur when an object is missing a required field or has other issues that prevent it from being
valid.
For example:

::

    tables.0.columns.0.@id
      Field required [type=missing, input_value={'name': 'customer_id', '...ue customer identifier'}, input_type=dict]
        For further information visit https://errors.pydantic.dev/2.8/v/missing

In this case, the column is missing an ``@id`` field, which is always required for every object in the schema.

Errors may also occur which reference an object like a column, as in:

::

    tables.0.columns.1
      Value error, Length must be provided for type 'string' in column '#customers.name' [type=value_error, input_value={'name': 'name', '@id': '...ame', 'nullable': False}, input_type=dict]
        For further information visit https://errors.pydantic.dev/2.8/v/value_error

This error indicates that the column is missing a length field, which is required for string columns.
The error occured in the second column of the first table.

Optional Checks
===============

There are a number of optional checks that may be performed during validation by turning on the corresponding
command line flags.
These include the following:

:``--check-description``: Check that all objects in the schema, including the schema itself, have a valid description field.
:``--check-redundant-datatypes``: Check that any column datatype overrides for specific database variants, such as ``mysql:datatype`` and ``postgresql:datatype``, do not appear to be redundant, resulting in identical SQL being emitted compared with the ``datatype``.
:``--check-tap-table-indexes``: Check that the ``tap:table_index`` field is unique for each table in the schema.
:``--check-tap-principal``: Check that the ``tap:principal`` field is set for at least one column in each table.

Simply include one or more of these flags in the validation command to enable them:

.. code-block:: bash

    felis validate --check-description --check-redundant-datatypes --check-tap-table-indexes --check-tap-principal schema.yaml

.. _validating-with-python-api:

Validating with the Python API
==============================

The Python API can also be used to validate a schema.
This is done by creating a ``Schema`` object and calling the ``model_validate`` method:

.. code-block:: python

    import yaml
    from felis.datamodel import Schema

    data = yaml.safe_load(open("schema.yaml", "r"))
    schema = Schema.model_validate(data)

This will raise an exception if the schema contains validation errors, printing them to the console like the
command line interface.
If there are no errors, the method will return the validated schema object, which can be used to create a
database or perform other operations.
