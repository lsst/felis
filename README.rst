Felis
=====

|PyPI| |Python|

.. |PyPI| image:: https://img.shields.io/pypi/v/lsst-felis
    :target: https://pypi.org/project/lsst-felis
    :alt: PyPI

.. |Python| image:: https://img.shields.io/pypi/pyversions/lsst-felis
    :target: https://pypi.org/project/lsst-felis
    :alt: PyPI - Python Version

YAML Schema Definition Language for Databases

Overview
--------

Felis implements a `YAML <https://yaml.org/>`_ data format for describing
database schemas in a way that is independent of implementation
language and database variant. These schema definitions can be checked for
validity using an internal `Pydantic <https://docs.pydantic.dev/latest/>`_ data
model to ensure strict conformance to the format. SQL Data Definition language
(DDL) statements can be generated to instantiate corresponding database
objects, such as tables and columns, in a number of different database
variants, including MySQL, PostgreSQL, Oracle, and SQLite. The schema can also
be used to update the TAP schema information in a
`TAP <https://www.ivoa.net/documents/TAP/>`_ service.

The tool was developed internally by the
`Vera C. Rubin Observatory <https://rubinobservatory.org/>`_ as a way of
defining a "single source of truth" for database schemas and their metadata
which are maintained for the facility in the
`sdm_schemas github repository <https://github.com/lsst/sdm_schemas>`_. Though
developed in the context of astronomical data management and including some
optional features that are specific to
`IVOA standards <https://www.ivoa.net/documents/>`_, Felis is generic enough
that it can be used as a general tool to define, update, and manage database
schemas in a way that is independent of database variant or implementation
language such as SQL.

Installation and Usage
----------------------

Felis is designed to work with Python 3.11 and 3.12 and may be installed using
`pip <https://pypi.org/project/pip/>`_::

    pip install lsst-felis

The `felis` command-line tool that is installed with the package can be used to
perform various actions on the YAML schema files, including validating the
schema definitions, generating DDL statements for various databases, or
updating a TAP service with schema metadata. The command line help provides
documentation on all of these utilities::

    felis --help

Individual subcommands also have their own documentation::

    felis validate --help

For instance, this command can be used to validate a schema file::

    felis validate myschema.yaml

If the schema generates validation errors, then these will be printed to the
terminal. These errors may include missing required attributes, misspelled YAML
keys, invalid data values, etc.

Documentation
-------------

Detailed information on usage, customization, and design is available at the
`Felis documentation site <https://felis.lsst.io>`_.

Presentations
-------------

- `IVOA Inter Op 2018 <https://wiki.ivoa.net/internal/IVOA/InterOpNov2018Apps/Felis_ivoa-11_2018.pdf>`_ - "Felis: A YAML Schema Definition Language for Database Schemas" - `slides <https://wiki.ivoa.net/internal/IVOA/InterOpNov2018Apps/Felis_ivoa-11_2018.pdf>`__

Support
-------

Users may report issues or request features on the `Rubin Community Forum <https://community.lsst.org/c/support>`_ or by opening an issue on the
`Felis Issue Tracker <https://github.com/lsst/felis/issues>`_. Posts on the
forum should include the tag "felis."

Code of Conduct
---------------

Please see the
`LSST Project Code of Conduct <https://project.lsst.org/codesofconduct>`_ for
guidelines on communications and interactions.

License
-------

Felis is distributed under the
`GNU General Public License
<https://www.gnu.org/licenses/gpl-3.0.en.html>`_, version 3.0 or later.
