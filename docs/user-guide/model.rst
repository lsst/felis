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

*****************
Column References
*****************

Felis supports column references, allowing schemas to import and reference
columns from external schema files.
This feature enables modular schema design and reuse of common logical column
definitions across multiple schemas.
The motivation for this in the Rubin Observatory context is that we have many
schemas with a large amount of commonality between the defined tables and
columns, so obtaining the details of common columns from a single source of
truth avoids a great deal of labor-intensive and error-prone parallel editing
of multiple schema files.

These references are defined at the schema level and processed when the
schema is loaded.
The referenced external schemas are loaded and their column definitions become
available for use in the importing schema.

To use column references, define a ``resources`` section in your schema and use
it as follows:

.. code-block:: yaml

   name: my_schema
   description: "My schema that uses column references"

   resources:
     some_schema:
       uri: "file:///path/to/some_schema.yaml"

   tables:
     - name: my_table
       columnRefs:
         some_schema:         # Reference to the external schema from 'resources'
           some_table:        # Name of the table in the external schema
             some_column:     # Name of the column to import as-is into this table
             renamed_column:  # New name for the imported column (requires ref_name)
               ref_name: "another_column"  # Column name from the external schema
               overrides:  # Attribute overrides
                 datatype: "string"  # Override an allowed attribute

The ``resources`` section maps resource names, e.g., ``some_schema``, to their
locations.
The ``uri`` field specifies the location of the external schema file containing
the column definitions and may point to a local file path, URL, or other
supported scheme such as a ``resource://`` URI.
(A ``resource://`` URI can read YAML files from a Python package installed in
the current environment.)

The ``columnRefs`` structure in the referencing schema has a hierarchy of keys
which is organized as follows:

.. code-block:: yaml

   columnRefs:
     resource_name:    # Name of the resource from the 'resources' section
       table_name:     # Name of the table in the external schema
         column_name:  # Reference to a column in the external table
           # Empty key imports the column as-is
         local_name:   # New name for a column in the local schema (requires ref_name)
           ref_name: "some_column_from_resource"  # Column name in the external schema
           overrides:  # Optional overrides for column attributes
             description: "Custom description"  # Override description
             datatype: "char"  # Override datatype
             length: 50  # Override length
             tap:principal: 1  # Override TAP principal flag
             tap:column_index: 2  # Override TAP column index

As show above, when importing columns, you may override specific properties
using the ``overrides`` subsection of the column reference.

The following fields can be overridden from the original column definition:

- ``datatype``: Change the column's data type
- ``length``: Modify the length constraint
- ``description``: Provide a different description
- ``nullable``: Change the nullability constraint
- ``tap:principal``: Override the TAP principal flag
- ``tap:column_index``: Override the TAP column index

The value of the ``dereference_resources`` context parameter controls the
processing behavior when resources are referenced in a schema and the data
model is loaded:

- **False** (default): References are preserved in ``columnRefs`` and the derived
  columns are flagged as references
- **True**: References are resolved, the ``columnRefs`` section is set to
  empty, and the columns are added to the table's ``columns`` list

Setting ``dereference_resources`` to ``True`` may be useful for producing a
fully self-contained schema without any external references.

Here is example Python code showing how to load a schema with and without
dereferencing:

.. code-block:: python

   from felis import Schema

   # Load with references preserved
   schema = Schema.from_uri("my_schema.yaml")

   # Load with references fully resolved
   schema = Schema.from_uri("my_schema.yaml",
                           context={"dereference_resources": True})

The ``felis dump`` command-line tool also supports a corresponding
``--dereference-resources`` flag to control this behavior when serializing
schemas using the CLI.

If the schema is dereferenced, the ``columnsRefs`` section will not be written
out when the schema is serialized.
Instead, all of the referenced columns will be included in the ``columns`` list
of the respective tables as regular column definitions.
By default, the ``columnsRefs`` section will be preserved when serializing and
the referenced columns will not be duplicated in the ``columns`` list.

Below are two example YAML schema files demonstrating the use of column
references and the features described above.

**some_schema.yaml:**

.. code-block:: yaml

   name: some_schema
   description: "Some schema with column definitions"

   tables:
     - name: some_table
       columns:
         - name: id
           datatype: "long"
           description: "Unique identifier"
         - name: created_at
           datatype: "timestamp"
           description: "Creation timestamp"

**my_schema.yaml:**

.. code-block:: yaml

   name: my_schema
   description: "My schema which will reference external columns"

   resources:
     some_schema:
       uri: "file:///path/to/some_schema.yaml"

   tables:
     - name: users
       description: "Application users"
       columns:
         # Regular column definitions may also be included alongside references
         - name: username
           datatype: "string"
           description: "User login name"
       columnRefs:
         some_schema:
           some_table:
             id:  # Import as-is (empty key)
             created_at:
               overrides:
                 description: "When the user was created"  # Override description

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

Foreign Key Constraint
----------------------

A `foreign key constraint <https://docs.sqlalchemy.org/en/20/glossary.html#term-foreign-key-constraint>`__ is
a rule that enforces referential integrity between two tables.
The constraint is defined by a column in the current table that references a column in another table.
Foreign key constraints may have the following additional attributes:

:``columns``: One or more column names in the current table that are part of the foreign key. This should be one or more ``@id`` values pointing to columns in the current table.
:``referencedColumns``: The columns referenced by the foreign key. This should be one or more ``@id`` values pointing to columns in another table.
:``on_update``: The action to take when the referenced column is updated. See the  `data model documentation for on_update <../dev/internals/felis.datamodel.ForeignKeyConstraint.html#felis.datamodel.ForeignKeyConstraint.on_update>`__ for valid values [5]_.
:``on_delete``: The action to take when the referenced column is deleted. See the `data model documentation for on_delete <../dev/internals/felis.datamodel.ForeignKeyConstraint.html#felis.datamodel.ForeignKeyConstraint.on_delete>`__ for valid values [5]_.

.. [5] The ``on_update`` and ``on_delete`` fields are optional and will be omitted from the generated SQL if not set.
       The `SQLAlchemy documentation <https://docs.sqlalchemy.org/en/20/core/constraints.html#on-update-on-delete>`__ explains how these actions are implemented in DDL.
       Not all databases support all of these actions, so it is recommended to check the documentation for the specific database being used.

Check Constraint
----------------

A `check constraint <https://docs.sqlalchemy.org/en/20/glossary.html#term-check-constraint>`__ is a rule that
restricts the values in a column.
The constraint is defined by a SQL expression.
Check constraints may have the following additional attributes:

:``expression``: The SQL expression that defines the constraint.

Unique Constraint
-----------------

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
:``columns``: The list of columns in the table that are part of the index. This should be one or more ``@id`` values pointing to columns in the table [6]_.
:``expressions``: The list of SQL expressions that are part of the index. This is only applicable to indexes that are created using expressions [6]_.

.. [6] The ``columns`` and ``expressions`` fields are mutually exclusive.
       Only one of these fields should be set on an index object.

.. warning::
    Databases may be updated with additional indexes that are not defined in the schema. These indexes will not be reflected in the Felis schema.
