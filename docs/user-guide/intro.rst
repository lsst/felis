Introduction
------------

Data catalogs are a fundamental part of modern astronomical research, providing a means to describe available
data and search for data matching certain criteria.
Tabular data models are a common way to represent such catalogs, and a canonical format for describing them is
SQL Data Definition Language (DDL).
However, DDL does not provide a way to describe the semantics of the data, such as the meaning of each column,
the units of measurement, or the relationships between tables.
Felis provides a way to describe these semantics in a user-friendly YAML format.

Within astronomy, the `International Virtual Observatory Alliance <https://ivoa.net/>`__ (IVOA) has developed
a standard for describing tabular data models called
`Table Access Protocol <https://www.ivoa.net/documents/TAP/>`__ (TAP).
Metadata for a specific TAP service is typically provided in a schema called TAP_SCHEMA describing tables and
columns available in the service.
Felis provides a tool for translating its schema representation into TAP_SCHEMA, making a catalog's metadata
available through a standard TAP service.
Compatible TAP services can use this data to populate their ``/tables`` output.

Felis also provides a mechanism for instantiating a catalog from its schema representation into an (empty)
relational database including the table, constraint and column definitions.
This can be done using the command line interface or the Python API.
Supported databases include SQLite, MySQL/MariaDB, and PostgreSQL.
