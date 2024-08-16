#############
SQL Databases
#############

Felis can create the corresponding database objects from a schema using the command line tool or the Python API.
This includes the schema itself and all of its tables, columns, indexes, and constraints.
The DDL to perform these actions can either be executed automatically or written to a file for later use.
An existing database can be used or a new one created, depending on the options provided.
When creating a new database rather than updating an existing one, the schema will be instantiated using
``CREATE DATABASE`` in MySQL and ``CREATE SCHEMA`` in PostgreSQL.
The user must have the necessary permissions to create databases in the target database server for this to
work.
SQLite does not support named schemas, so the schema name will be ignored, and generally the database file
will need to be created beforehand.

Using the Command Line Tool
===========================

The ``felis create`` command can be used to create a database from the command line.
For more information on the command line options for the ``create`` command, see the
`command line documentation <cli.html#felis-create>`_.

Dry Run Mode
------------

The dry run mode can be used to generate SQL without actually creating the database.
This can be useful for inspecting the SQL that will be generated before running it.

There are several different ways to run in dry run mode, one being to simply omit the ``--engine-url`` option:

.. code-block:: bash

    felis create schema.yaml

This will by default instantiate the database to an in-memory (transient) SQLite database.
Running such a command can be useful for finding errors in the schema file before attempting to create a
persistent database.

A specific database dialect such as MySQL can be selected in dry run mode using the ``--engine-url`` option:

.. code-block:: bash

    felis create --engine-url mysql:// schema.yaml

This will generate the SQL in MySQL format and print it to the console but will not actually create or update
a database.

The dry run mode may also be explicitly enabled using the ``--dry-run`` option:

.. code-block:: bash

    felis create --dry-run --engine-url mysql+pymysql://username:password@localhost schema.yaml

The URL in this case will be used to determine the database dialect and generate the appropriate SQL but will
otherwise be ignored.

The generated SQL may also be saved to a file using the ``--output-file`` option:

.. code-block:: bash

    felis create --engine-url mysql:// schema.yaml --output-file schema.sql

This SQL file could then be used to create the database at a later time using a tool such as the MySQL client.

Creating a Persistent Database
------------------------------

In order to create a persistent database, the ``--engine-url`` must be set to a valid database URL.
The URL format follows `SQLAlchemy engine conventions <https://docs.sqlalchemy.org/en/20/core/engines.html>`_:
``dialect+driver://username:password@host:port/database``.

The database URL has the following parameters:

- ``dialect``: The name of the database backend, such as ``sqlite``, ``mysql``, or ``postgresql``. The default is ``sqlite``.
- ``driver``: The name of the DBAPI to use, such as ``pymysql``, or ``psycopg2``. This is optional and the default driver for the dialect will be used if not specified.
- ``username``: The username to use when connecting to the database.
- ``password``: The password to use when connecting to the database.
- ``host``: The host for connection. Typically, this should be set to ``localhost`` if the database is running on the same machine.
- ``port``: The port for connection. This will use the default port for the dialect if not specified.
- ``database``: The name of the database to create. For MySQL, this should typically be left blank, and Felis will use the name of the schema for the database. For a PostgreSQL connection, this should be the name of the database in which the schema will be created.

The database URL may also be set using the ``FELIS_ENGINE_URL`` environment variable, in which case the
``--engine-url`` option can be omitted.

MySQL
^^^^^

To create a MySQL database from a schema file, the command would look similar to the following:

.. code-block:: bash

    felis create --engine-url mysql+pymysql://username:password@localhost schema.yaml

In this case, the database would already need to have been created or the command will fail.

Tables in MySQL will by default use the ``MyISAM`` storage engine, which does not support foreign keys.
The engine can be changed in the table object of the schema by setting ``mysql:engine`` to ``InnoDB`` or another valid table engine name.

PostgreSQL
^^^^^^^^^^

PostgreSQL databases can be created similarly by using ``psychopg2`` as the driver and ``postgresql`` as the
dialect:

.. code-block:: bash

    felis create --engine-url postgresql+psycopg2://username:password@localhost/database schema.yaml

Felis can be used to create the schema, but it cannot create the database itself, which must be included as
part of the URL; or the command will fail.
The database must be created beforehand using the ``CREATE DATABASE`` command in the PostgreSQL client.

SQLite
^^^^^^

To persist a SQLite database, first create an empty database on disk as follows:

.. code-block:: bash

    sqlite3 /tmp/my.db "VACUUM;"

Installation of SQLite is not covered; please refer to the `SQLite documentation <https://www.sqlite.org>`_ for more information.

The database objects can then be instantiated from a schema file:

.. code-block:: bash

    felis create --engine-url sqlite:////tmp/my.db schema.yaml

After it has been created, you may open the database file with a SQLite client to inspect the schema:

.. code-block:: bash

    sqlite3 /tmp/my.db

To show the tables which were instantiated, use the following command from within the SQLite client:

..

    .tables

SQLite will ignore the name of the schema, as it does not support named schemas or databases.

Initializing and Dropping Databases
-----------------------------------

Felis can create the schema's database, rather than use an existing one, with the ``--initialize`` option:

.. code-block:: bash

    felis create --engine-url mysql+pymysql://username:password@localhost --initialize schema.yaml

If the database exists already, this command would raise an error to protect against inadvertant updates. To
update an existing database, simply omit this option.

Initialization is unneeded for SQLite, as a new database file will be created automatically if it does not
exist, as in the following example:

.. code-block:: bash

    felis create --engine-url sqlite:///example.db schema.yaml

In this case, the ``--initialize`` flag will be silently ignored if present.

Felis can also drop an existing database first and then recreate it:

.. code-block:: bash

    felis create --engine-url mysql+pymysql://username:password@localhost --drop schema.yaml

If the database does not exist, then the ``--drop`` option will be ignored and the database will be created
normally.

The ``--initialize`` and ``--drop`` options are mutually exclusive, as dropping always initializes the
database. If they are used together then an error will be raised.

The commands to create or drop databases will require that the database user has the necessary permissions on
the server.

Using a Different Schema Name
-----------------------------

The name of the schema in the database will by default be the same as the ``name`` field in the YAML file, but
this can be overridden using the ``--schema-name`` option:

.. code-block:: bash

    felis create --engine-url mysql+pymysql://username:password@localhost --schema-name myschema schema.yaml

In this case, the schema in the database will be named ``myschema`` instead of the name from the file.

Using the Python API
====================

The Python API can also be used to create a database from a schema.
First, the schema object should be loaded from a YAML file, following the instructions in
:ref:`validating-with-python-api`.

Once the schema object has been successfuly created, the builder can be used to create the SQLAlchemy metadata
object:

.. code-block:: python

    from felis.metadata import MetaDataBuilder
    metadata = MetaDataBuilder(schema).build()

The metadata object can be used to create the database using standard SQLAlchemy commands.
For example, the following command will create an in-memory SQLite database in Python:

.. code-block:: python

    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

To create a MySQL database, the engine URL should be changed to something like this:

.. code-block:: python

    engine = create_engine("mysql+pymysql://username:password@localhost")
    metadata.create_all(engine)

The database will then be created on the MySQL server at ``localhost``.

Felis also provides the `DatabaseContext class <../dev/internals/felis.db.utils.DatabaseContext.html>`_ which
supports creation of the database or schema itself:

.. code-block:: python

        engine = create_engine("mysql+pymysql://username:password@localhost")
        ctx = DatabaseContext(metadata, engine)
        ctx.initialize()
        ctx.create_all()

An advantage of using this class is that it can automatically handle the creation of the database if it does
not already exist with the ``create_if_not_exists`` method or drop and recreate the database with the
``drop_and_create`` method.
