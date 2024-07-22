##########
Data Types
##########

Felis provides an internal type system that is used to define the data types of columns in a schema.
These data types are mapped to the appropriate SQL types when instantiating a database and the corresponding
`VOTable <https://ivoa.net/documents/VOTable/>`__ data types when writing information to
`TAP_SCHEMA <https://www.ivoa.net/documents/TAP/20190927/REC-TAP-1.1.html#tth_sEc4>`_.

The following values are supported by the
`datatype <../dev/internals/felis.datamodel.Column.html#felis.datamodel.Column.datatype>`_ field of a column
object:

+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| Type      | Description                      | Notes                                                                                                                            |
+===========+==================================+==================================================================================================================================+
| boolean   | boolean value                    |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| byte      | 8-bit signed integer             | See footnote [1]_                                                                                                                |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| short     | 16-bit signed integer            |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| int       | 32-bit signed integer            |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| long      | 64-bit signed integer            |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| float     | 32-bit floating point number     | `IEEE 745 <https://en.wikipedia.org/wiki/IEEE_754>`_ single precision floating point number                                      |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| double    | 64-bit floating point number     | `IEEE 745 <https://en.wikipedia.org/wiki/IEEE_754>`_ double precision floating point number                                      |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| char      | fixed-length character string    |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| string    | variable-length character string |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| unicode   | variable-length Unicode string   | **Usage of this type is discouraged** as it maps to the obsolete UCS-2 encoding in VOTable, and is not usable for UTF-8 strings. |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| text      | variable-length text string      |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| binary    | variable-length binary blob      |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| timestamp | timestamp value                  |                                                                                                                                  |
+-----------+----------------------------------+----------------------------------------------------------------------------------------------------------------------------------+

.. [1] The Felis type ``byte`` currently maps to signed 8-bit integral types in the supported database back ends. VOTable, and therefore TAP_SCHEMA, only has an unsigned 8-bit type. The Felis mapping is under review at this time; in the mean time, if you are intentionally storing signed byte values in a database with a Felis-derived schema, we recommend the use of a VOTable type override in Felis, ``votable:datatype: short``, to ensure that the signedness of the values is preserved in TAP output.

Data Type Mappings
==================

Felis types are mapped to SQL types when creating a schema in a database and
`VOTable primitives <https://www.ivoa.net/documents/VOTable/20191021/REC-VOTable-1.4-20191021.html#ToC11>`_
when writing information to TAP_SCHEMA.
The following table shows these mapping:

+-----------+---------------+----------+------------------+--------------+
| Felis     | SQLite [2]_   | MySQL    | PostgreSQL       | VOTable      |
+===========+===============+==========+==================+==============+
| boolean   | BOOLEAN       | BOOLEAN  | BOOLEAN          | boolean      |
+-----------+---------------+----------+------------------+--------------+
| byte [1]_ | TINYINT       | TINYINT  | SMALLINT         | unsignedByte |
+-----------+---------------+----------+------------------+--------------+
| short     | SMALLINT      | SMALLINT | SMALLINT         | short        |
+-----------+---------------+----------+------------------+--------------+
| int       | INTEGER       | INTEGER  | INTEGER          | int          |
+-----------+---------------+----------+------------------+--------------+
| long      | BIGINT        | BIGINT   | BIGINT           | long         |
+-----------+---------------+----------+------------------+--------------+
| float     | FLOAT         | FLOAT    | FLOAT            | float        |
+-----------+---------------+----------+------------------+--------------+
| double    | DOUBLE        | DOUBLE   | DOUBLE PRECISION | double       |
+-----------+---------------+----------+------------------+--------------+
| char      | CHAR          | CHAR     | CHAR             | char         |
+-----------+---------------+----------+------------------+--------------+
| string    | VARCHAR       | VARCHAR  | VARCHAR          | char         |
+-----------+---------------+----------+------------------+--------------+
| unicode   | NVARCHAR      | NVARCHAR | VARCHAR          | unicodeChar  |
+-----------+---------------+----------+------------------+--------------+
| text      | TEXT          | LONGTEXT | TEXT             | uncodeChar   |
+-----------+---------------+----------+------------------+--------------+
| binary    | BLOB          | LONGBLOB | BYTEA            | unsignedByte |
+-----------+---------------+----------+------------------+--------------+
| timestamp | TIMESTAMP     | DATETIME | TIMESTAMP        | char         |
+-----------+---------------+----------+------------------+--------------+

.. [2] `SQLite data types <https://www.sqlite.org/datatype3.html>`__ use `storage classes <https://www.sqlite.org/datatype3.html#affinity>`__. The types listed here represent the emitted SQL type for the given Felis type in SQLAlchemy rather than the storage class and are also indicative of the generic SQL type that is emitted when no database dialect is specified.
