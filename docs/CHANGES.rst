#########
Changelog
#########

v28.0.0 (2024-11-26)
====================

New Features
------------

- Added a new ``tap_schema`` module designed to deprecate and eventually replace the ``tap`` module.
  This module provides utilities for translating a Felis schema into a TAP_SCHEMA representation.
  The command ``felis load-tap-schema`` can be used to activate this functionality. (`DM-45263 <https://jira.lsstcorp.org/browse/DM-45263>`_)
- Added a check to the data model which ensures that all constraint names are unique within the schema.
  TAP_SCHEMA uses these names as primary keys in its ``keys`` table, so they cannot be duplicated. (`DM-45623 <https://jira.lsstcorp.org/browse/DM-45623>`_)
- Added automatic ID generation for objects in Felis schemas when the ``--id-generation`` flag is included on the command line.
  This is supported for the ``create`` and ``validate`` commands.
  Also added a Schema validator function that checks if index names are unique. (`DM-45938 <https://jira.lsstcorp.org/browse/DM-45938>`_)


Bug Fixes
---------

- Fixed a bug where the error locations on constraint objects during validation were reported incorrectly.
  This was accomplished by replacing the ``create_constraints()`` function with a Pydantic `discriminated union <https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions-with-str-discriminators>`__. (`DM-46002 <https://jira.lsstcorp.org/browse/DM-46002>`_)


v27.0.0 (2024-04-17)
====================

New Features
------------

- Added option for setting TAP_SCHEMA index in ``load-tap`` command. (`DM-43683 <https://rubinobs.atlassian.net/browse/DM-43683>`__)
- Added option for creating a schema's database if it does not exist when running the ``create`` command. (`DM-43108 <https://rubinobs.atlassian.net/browse/DM-43108>`__)
- Added setting of log level and output file in the CLI (`DM-43040 <https://rubinobs.atlassian.net/browse/DM-43040>`__)
- Created a validator that will look for redundant datatype specifications.
  This is activated from the command line with the ``--check-redundant--datatypes`` flag. (`DM-41247 <https://rubinobs.atlassian.net/browse/DM-41247>`__)

API Changes
-----------

- Added a new ``metadata`` module for generating SQLAlchemy ``MetaData`` from a schema.
  This replaces the old module that was used in the ``create`` command for initializing the database. (`DM-43079 <https://rubinobs.atlassian.net/browse/DM-43079>`__)
- Made columns nullable by default in the Pydantic data model. (`DM-43753 <https://rubinobs.atlassian.net/browse/DM-43753>`__)
- Refactored the ``tap`` module to use the Pydantic data model. (`DM-42935 <https://rubinobs.atlassian.net/browse/DM-42935>`__)
- Moved database utilities into a separate package and refactored them. (`DM-44721 <https://rubinobs.atlassian.net/browse/DM-44721>`__)

An API Removal or Deprecation
-----------------------------

- Removed JSON-LD commands from the CLI and removed ``pyld`` from ``requirements.txt``. (`DM-43688 <https://rubinobs.atlassian.net/browse/DM-43668>`__)
- Removed outdated modules. (`DM-43597 <https://rubinobs.atlassian.net/browse/DM-43597>`__)

Documentation Improvements
--------------------------

- Updated Felis documentation infrastructure. (`DM-43787 <https://rubinobs.atlassian.net/browse/DM-43787>`__)
