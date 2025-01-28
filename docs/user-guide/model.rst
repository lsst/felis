##########
Data Model
##########

Felis's `data model <../dev/internals.html#module-felis.datamodel>`__ is defined by a set of
`Pydantic <https://docs.pydantic.dev/latest/>`__ classes defining the semantics of tabular data.
In addition to the standard conceptual constructs of a relational database schema, such as tables and columns,
Felis provides a way to attach extra metadata to these elements, like units of measurement on columns.

******
Schema
******

A `schema <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.Schema>`__ is the top-level object
in the data model and represents a collection of tables along with their indexes and constraints.
A schema is defined by a name and will be instantiated as a
`schema object in PostgreSQL <https://www.postgresql.org/docs/13/ddl-schemas.html>`__ using ``CREATE SCHEMA``
and a `database object in MySQL <https://dev.mysql.com/doc/refman/8.4/en/database-use.html>`__ with ``CREATE
DATABASE``.

Schemas may contain the following fields:

:``name``: The name of this schema. This is the name that will be used to create the schema in the database.
:``@id``: An identifier for this schema. This may be used for relating schemas together at a higher level. Typically, the name of the schema can be used as the id.
:``description``: A textual description of this schema.
:``tables``: The list of tables in the schema. A schema must have one or more tables.
:``version``: Optional schema version description.

**************
Schema Version
**************

A `schema version <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.SchemaVersion>`__ can be used
to track changes to a schema over time.
The version is defined by either a simple string or a more complex version object.
Felis does not enforce any particular format for version strings.
A version string could simply be defined as ``v1``, for instance.

The `schema version <../dev/internals/felis.datamodel.SchemaVersion.html>`_ object has the following
attributes:

:``current``: The current version of the schema.
:``compatible``: A list of fully compatible versions.
:``read_compatible``: A list of versions that are read-compatible. A read compatible version is one where a database created with the older version can be read by the newer version. An example of a non-read compatible change would be removing a column from a table.

While Felis does not enforce any particular format for version strings, it is recommended to use a format that
can be compared using `semantic versioning <https://semver.org/>`__.

*****
Table
*****

A `table <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.Table>`__ represents a collection of
columns along with their indexes and constraints and has these attributes:

:``name``: The name of this table. This is the name that will be used to create the table in the database.
:``@id``: A unique identifier for this table.
:``description``: A textual description of this table.
:``columns``: The list of columns in the table.
:``primaryKey``: The ID of the table's primary key column or a list of IDs that make up a composite primary key.
:``constraints``: The list of constraints for the table. Refer to the :ref:`Constraint` section for more information.
:``indexes``: The list of indexes for the table. Refer to the :ref:`Index` section for more information.

A table may also have the following optional attributes:

:``tap:table_index``: The index of this table in a TAP schema. This is used to order the tables in public presentations of the schema.
:``mysql:charset``: The `MySQL character set <https://dev.mysql.com/doc/refman/8.4/en/charset-charsets.html>`__ for the table.
:``mysql:engine``: The `MySQL storage engine <https://dev.mysql.com/doc/refman/8.4/en/storage-engines.html>`__ for the table.

******
Column
******

A `column <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.Column>`__ represents a single field
in a table.
A column has a name and a data type and may have additional metadata like units of measurement.

Columns have the following primary attributes:

:``name``: The name of this column. This is the name that will be used to create the column in the database.
:``@id``: A unique identifier for this column.
:``description``: A textual description of this column.
:``datatype``: The data type of this column. Valid datatypes are defined by the `Felis type system <../dev/internals.html#module-felis.types>`__.
:``autoincrement``: A boolean flag indicating whether this column is auto-incrementing.
:``length``: The length of the column. This is only applicable to certain data types.
:``nullable``: A boolean flag indicating whether this column can be null.
:``precision``: The precision of the column. This is current only applicable to columns with the ``timestamp`` datatype.
:``value``: The default value for this column.

A column may also have the following optional properties:

:``ivoa:unit``: The `IVOA unit <https://ivoa.net/documents/VOUnits/>`__ of this column.
:``fits:tunit``: The `FITS TUNIT <https://fits.gsfc.nasa.gov/standard30/fits_standard30aa.pdf>`__ unit for this column [1]_.
:``ivoa:ucd``: The `IVOA UCD <http://www.ivoa.net/documents/latest/UCD.html>`__ for this column [1]_.
:``tap:column_index``: The index of this column in a TAP table. This is used to order the columns in public presentations of the table [2]_.
:``tap:principal``: A flag indicating whether this column is a "principal column" in a TAP table; principal columns represent a subset to be highlighted or used as a default in public presentations and query builders. This should be encoded as 0 or 1 [2]_.
:``tap:std``: A flag indicating whether this column is a representation of an element of an IVOA-standard data model. This should be encoded as 0 or 1 [2]_.
:``votable:arraysize``: The VOTable ``arraysize`` for this column [3]_.
:``votable:datatype``: The VOTable ``datatype`` for this column [3]_.
:``votable:xtype``: The VOTable ``xtype``, if any, for this column [3]_.
:``mysql:datatype``: A `MySQL data type <https://dev.mysql.com/doc/refman/8.4/en/data-types.html>`__ override for this column.
:``postgresql:datatype``: A `PostgreSQL data type <https://www.postgresql.org/docs/13/datatype.html>`__ override for this column.

.. [1] The ``ivoa:unit`` and ``fits:tunit`` fields are mutually exclusive. Only one of these fields should be set on a column object.
.. [2] `TAP Access Protocol (TAP) specification <https://www.ivoa.net/documents/TAP/>`__
.. [3] `VOTable specification <http://www.ivoa.net/documents/VOTable/>`__

*************
Column Groups
*************

A `column group <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.ColumnGroup>`__ represents a set of related columns in a table.
In addition to the standard column attributes, column groups have the following attributes:

:``ivoa:ucd``: The `IVOA UCD <http://www.ivoa.net/documents/latest/UCD.html>`__ for this column group.
:``columns``: The list of columns in this column group, which should be IDs of columns in the table. This is a required field.

The functionality of column groups is currently limited but may be expanded in future versions of Felis, in particular to support VOTable ``GROUP`` elements.

.. _Constraint:

**********
Constraint
**********

A `constraint <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.Constraint>`__ is a rule that
restricts the values in a table.
The most common type of constraint is a foreign key constraint.

Constraints have specific types specified by the ``@type`` field, which can be one of the following:

- `ForeignKeyConstraint <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.ForeignKeyConstraint>`__
- `UniqueConstraint <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.UniqueConstraint>`__
- `CheckConstraint <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.CheckConstraint>`__

All types of constraints accept the following properties:

:``name``: The name of the constraint. This is the name that will be used to create the constraint in the database.
:``description``: A textual description of this constraint.
:``@id``: A unique identifier for this constraint.
:``deferrable``: A boolean flag indicating whether this constraint is deferrable. This will emit a ``DEFERRABLE`` clause when creating the constraint in the database.
:``initially``: The initial deferrable state of the constraint. This will emit an ``INITIALLY <value>`` clause when creating the constraint in the database.
:``annotations``: A list of annotations for this constraint [4]_.
:``@type``: This is a special field which indicates the type of the constraint. The value of this field will determine the type of the constraint object that is created and what additional fields are allowed on the object.

.. [4] The annotations field is currently unused and may be removed in a future version of Felis.

Constraint Types
================

A `foreign key constraint <https://docs.sqlalchemy.org/en/20/glossary.html#term-foreign-key-constraint>`__ is
a rule that enforces referential integrity between two tables.
The constraint is defined by a column in the current table that references a column in another table.
Foreign key constraints may have the following additional attributes:

:``columns``: One or more column names in the current table that are part of the foreign key. This should be one or more ``@id`` values pointing to columns in the current table.
:``referencedColumns``: The columns referenced by the foreign key. This should be one or more ``@id`` values pointing to columns in another table.

A `check constraint <https://docs.sqlalchemy.org/en/20/glossary.html#term-check-constraint>`_ is a rule that
restricts the values in a column.
The constraint is defined by a SQL expression.
Check constraints may have the following additional attributes:

:``expression``: The SQL expression that defines the constraint.

A `unique constraint <https://docs.sqlalchemy.org/en/20/glossary.html#term-unique-constraint>`__ is a rule
that enforces uniqueness of values in a column or set of columns.
The constraint is defined by one or more columns in the table. Unique constraints may have the following
additional attributes:

:``columns``: One or more column names in the current table that are part of the unique constraint. This should be one or more ``@id`` values pointing to columns in the current table.

.. _Index:

*****
Index
*****

An `index <../dev/internals/felis.datamodel.Schema.html#felis.datamodel.Index>`__ is a data structure that
improves the speed of data retrieval operations on a table.
Indexes are defined by one or more columns in the table and have the following attributes:

:``name``: The name of the index. This is the name that will be used to create the index in the database.
:``description``: A textual description of this index.
:``@id``: A unique identifier for this index.
:``columns``: The list of columns in the table that are part of the index. This should be one or more ``@id`` values pointing to columns in the table [5]_.
:``expressions``: The list of SQL expressions that are part of the index. This is only applicable to indexes that are created using expressions [5]_.

.. [5] The ``columns`` and ``expressions`` fields are mutually exclusive. Only one of these fields should be set on an index object.

.. warning::
    Databases may be updated with additional indexes that are not defined in the schema. These indexes will not be reflected in the Felis schema.
