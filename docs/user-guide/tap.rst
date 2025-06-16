###########################
Table Access Protocol (TAP)
###########################

.. warning::

    This feature is under active development, and the available functionality and command line options may
    change in future releases.

Felis can be used to generate records for
`TAP_SCHEMA <https://www.ivoa.net/documents/TAP/20180830/PR-TAP-1.1-20180830.html#tth_sEc4>`_  which describes
metadata for a `TAP service <https://www.ivoa.net/documents/TAP/>`_.
The TAP_SCHEMA database can either be updated directly, or SQL statements can be generated and then executed
manually.

The SQL can be generated or executed using the ``load-tap`` command.
A command like the following can be used to generate SQL statements and save them to a file:

.. code-block:: bash

    felis load-tap-schema --dry-run --engine-url=mysql:// $file --output-file tap_schema.sql

This SQL file may then be used for initialization of the TAP_SCHEMA database, e.g. within a Docker container.
(This procedure is not covered here.)

Felis can also update an existing TAP_SCHEMA database directly if a valid URL is provided:

.. code-block:: bash

    felis load-tap-schema --engine--url=mysql+mysqlconnector://user:password@host/TAP_SCHEMA

Felis can create an empty TAP_SCHEMA database using the ``init-tap`` command.

.. code-block:: bash

    felis init-tap-schema --engine-url=mysql+mysqlconnector://user:password@host:port

By default, this will create a database called ``TAP_SCHEMA``.
The ``--tap-schema-name`` option can be used to specify a different name for the TAP_SCHEMA database.

.. code-block:: bash

    felis init-tap-schema --engine-url=mysql+mysqlconnector://user:password@host:port --tap-schema-name=MY_TAP_SCHEMA

By default, the TAP_SCHEMA database which is created will include records describing its own schema.
The ``--no-insert-metadata`` flag can be used to omit them.

.. code-block:: bash

    felis init-tap-schema --engine-url=mysql+mysqlconnector://user:password@host:port --no-insert-metadata

Standards-conformant TAP services will generally make the TAP_SCHEMA data available at a ``/tables`` endpoint.
Felis does not provide the full functionality and configuration for standing up a TAP service.
For that, you will need to use a TAP service implementation like
`CADC TAP <https://github.com/opencadc/tap>`_.
