###########################
Table Access Protocol (TAP)
###########################

.. warning::

    This feature is under active development, and the available functionality and command line options may
    change in future releases.

Felis can be used to generate records for
`TAP_SCHEMA <https://www.ivoa.net/documents/TAP/20180830/PR-TAP-1.1-20180830.html#tth_sEc4>`_  which describes
metadata for a `TAP service <https://www.ivoa.net/documents/TAP/>`_.
The TAP_SCHEMA database can either be updated directly or SQL statements can be generated and then executed
manually.

The SQL can be generated, and optionally executed, by using the ``load-tap`` command.
To generate SQL statements and save them to a file, a command like the following can be used:

.. code-block:: bash

    felis load-tap --dry-run --engine-url=mysql:// $file > tap_schema.sql

This SQL file can then be used for initialization of the TAP_SCHEMA database, e.g. within a Docker container.
(This procedure is not covered here.)

Felis can also update an existing TAP_SCHEMA database directly if a valid URL is provided:

.. code-block:: bash

    felis load-tap --engine--url=mysql+mysqlconnector://user:password@host:port/TAP_SCHEMA

Felis can create an empty TAP_SCHEMA database using the ``init-tap`` command.

.. code-block:: bash

    felis init-tap --engine-url=mysql+mysqlconnector://user:password@host:port

By default, this will create a database called ``TAP_SCHEMA``.
If you want to use a different name, you can specify it with the ``--tap-schema-name`` option:

.. code-block:: bash

    felis init-tap --engine-url=mysql+mysqlconnector://user:password@host:port --tap-schema-name=MY_TAP_SCHEMA

Felis does not provide the full functionality and configuration for standing up a TAP service.
For that, you will need to use a TAP service implementation like
`CADC TAP <https://github.com/opencadc/tap>`_.
Standards-conformant TAP services will generally make the TAP_SCHEMA data available at a ``/tables`` endpoint.
