Felis
=====

|Tag| |PyPI| |Python| |Codecov|

.. |PyPI| image:: https://img.shields.io/pypi/v/lsst-felis
    :target: https://pypi.org/project/lsst-felis
    :alt: PyPI

.. |Python| image:: https://img.shields.io/pypi/pyversions/lsst-felis
    :target: https://pypi.org/project/lsst-felis
    :alt: PyPI - Python Version

.. |Codecov| image:: https://codecov.io/gh/lsst/felis/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/lsst/felis
    :alt: Codecov

.. |Tag| image:: https://img.shields.io/github/v/tag/lsst/felis
    :target: https://github.com/lsst/felis/tags
    :alt: Latest Tag

Overview
--------

Felis implements a `YAML <https://yaml.org/>`_ data format for describing
database schemas in a way that is independent of implementation
language and database variant. These schema definitions can be checked for
validity using an internal `Pydantic <https://docs.pydantic.dev/latest/>`_ data
model to ensure strict conformance to the format. SQL Data Definition language
(DDL) statements can be generated to instantiate corresponding database
objects, such as tables and columns, in a number of different database
variants, including MySQL, PostgreSQL, and SQLite. The schema can also
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

Documentation
-------------

Detailed information on usage, customization, and design is available at the
`Felis documentation site <https://felis.lsst.io>`_.

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
