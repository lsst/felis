# Felis

## Introduction

Felis is a way of describing database catalogs, scientific and
otherwise, in a language and DBMS agnostic way. It's built on concepts
from JSON-LD/RDF and CSVW, but intended to provide a comprehensive way
to describe tabular data, using annotations on tables, columns, and
schemas, to document scientifically useful metadata as well as
implementation-specific metadata for database management systems, file
formats, and application data models.

When processing a felis description, we envision SQLAlchemy to be the
target implementation backend, so descriptions for Tables, Columns,
Foreign Keys, Constraints, and Indexes should generally map very closely
to SQLAlchemy parameters for those objects.

Liquibase descriptions were also consulted. Liquibase is oriented around
the concept of a changeset. It should be the case that a felis
description could be transformed into a Liquibase changeset without too
much effort.

## JSON-LD

JSON-LD is a way of representing data in a linked fashion. It is built
on the core concepts of [Linked
Data](https://www.w3.org/DesignIssues/LinkedData.html).

The rule we're most interested in for felis is the first rule:

> Use URIs as names for things

This rule, coupled with technologies in JSON-LD, allow us to identify
things in a well-defined manner using a syntax that is "very terse and
human readable". JSON-LD also provides algorithms to translate those
descriptions into objects that are easier to process by a computer.

Due of the emphasis put on linking data, it provides a natural way of
describing the fundamentally relational objects that make up a database.

Felis is influenced by work on CSVW, which uses JSON-LD to describe CSV
files. CSVW is oriented a bit more towards publishing data to the web,
and that doesn't quite capture the use case of describing tables,
especially those which haven't been created yet. Still, for services
which may return CSV files, a translation to CSVW will be
straightforward.

Some links that might be helpful for understanding
JSON-LD:

<http://arfon.org/json-ld-for-software-discovery-reuse-and-credit/index.html>
<https://w3c.github.io/json-ld-syntax/#basic-concept>

### IRIs and @context

Following from the first rule of Linked Data, JSON-LD uses IRIs
(Internationalized Resource Identifiers as described in \[RFC3987\]) for
unambiguous identification. This means the key in every annotation must
be an IRI.

The simplest possible schema, a schema with one table which contains a
point, represented in JSON, would look like the following:

``` sourceCode json
{
  "name": "MySchema",
  "tables": [
    {
      "name": "Point",
      "columns": [
        {
          "name": "ra",
          "datatype": "float"
        },
        {
          "name": "dec",
          "datatype": "float"
        }
      ]
    }
  ]
}
```

We can infer that this is probably describing a schema, but it's
possible the definitions are ambiguous. IRIs help with this:

``` sourceCode json
{
  "http://lsst.org/felis/name": "MySchema",
  "http://lsst.org/felis/tables": [
    {
      "http://lsst.org/felis/name": "Point",
      "http://lsst.org/felis/columns": [
        {
          "http://lsst.org/felis/name": "ra",
          "http://lsst.org/felis/datatype": "float"
        },
        {
          "http://lsst.org/felis/name": "dec",
          "http://lsst.org/felis/datatype": "float"
        }
      ]
    }
  ]
}
```

This provides unambiguous definitions to the semantics of each value,
but it's extremely wordy, compared to the natural JSON form.

To help with this, JSON-LD document has a context. Every Felis
description should as well. `@context` is similar to an XML namespace.

Used to define the short-hand names that are used throughout a JSON-LD
document. These short-hand names are called terms and help developers to
express specific identifiers in a compact manner.

``` sourceCode json
{
  "@context": "http://lsst.org/felis/",
  "name": "MySchema",
  "tables": [
    {
      "name": "Point",
      "columns": [
        {
          "name": "ra",
          "datatype": "float"
        },
        {
          "name": "dec",
          "datatype": "float"
        }
      ]
    }
  ]
}
```

This is fine, but the base vocabulary of Felis doesn't help much with
annotating columns with FITS or IVOA terms, for example. So we can add
to our context more vocabulary terms.

``` sourceCode json
{
  "@context": {
    "@vocab": "http://lsst.org/felis/",
    "ivoa": "http://ivoa.net/rdf/",
    "fits": "http://fits.gsfc.nasa.gov/FITS/4.0/"
  },
  "name": "MySchema",
  "tables": [
    {
      "name": "Point",
      "columns": [
        {
          "name": "ra",
          "datatype": "float",
          "ivoa:ucd": "pos.eq.ra;meta.main",
          "fits:tunit": "deg"
        },
        {
          "name": "dec",
          "datatype": "float",
          "ivoa:ucd": "pos.eq.dec;meta.main",
          "fits:tunit": "deg"
        }
      ]
    }
  ]
}
```

It's also fine to [externally define a context as
well](https://json-ld.org/spec/latest/json-ld/#interpreting-json-as-json-ld).
This reduced the boilerplate in a file, and allows the JSON appear even
simpler.

``` sourceCode json
{
  "name": "MySchema",
  "tables": [
    {
      "name": "Point",
      "columns": [
        {
          "name": "ra",
          "datatype": "float",
          "ivoa:ucd": "pos.eq.ra;meta.main",
          "fits:tunit": "deg"
        },
        {
          "name": "dec",
          "datatype": "float",
          "ivoa:ucd": "pos.eq.dec;meta.main",
          "fits:tunit": "deg"
        }
      ]
    }
  ]
}
```

Currently, vocabularies aren't formally defined for IVOA, FITS, MySQL,
Oracle, Postgres, SQLite. For now, we won't worry about that too much.
For most descriptions of tables, we will recommend a default context of
the following:

``` sourceCode json
{
  "@context": {
    "@vocab": "http://lsst.org/felis/",
    "mysql": "http://mysql.com/",
    "postgres": "http://posgresql.org/",
    "oracle": "http://oracle.com/database/",
    "sqlite": "http://sqlite.org/",
    "fits": "http://fits.gsfc.nasa.gov/FITS/4.0/"
    "ivoa": "http://ivoa.net/rdf/",
    "votable": "http://ivoa.net/rdf/VOTable/",
    "tap": "http://ivoa.net/documents/TAP/"
  }
}
```

### @id

The main way to reference objects within a JSON-LD document is by id.
The `@id` attribute of any object MUST be unique in that document. `@id`
is the main way we use to reference objects in a Felis description, such
as the columns referenced in an index, for example.

### As YAML

For describing schemas at rest, we recommend YAML, since we assume it
will be edited by users.

The table in YAML, with an externally defined context, would appear as
the following:

``` sourceCode yaml
---
name: MySchema
tables:
- name: Point
  columns:
  - name: ra
    datatype: float
    ivoa:ucd: pos.eq.ra;meta.main
    fits:tunit: deg
  - name: dec
    datatype: float
    ivoa:ucd: pos.eq.dec;meta.main
    fits:tunit: deg
```

JSON-LD keywords, those which start with `@` like `@id`, need to be
quoted in YAML.

## Tabular Data Models

This section defines the objects which make up the model.

The annotations provide information about the columns, tables, and
schemas they are defined in. The values of an annotation may be a list,
object, or atomic values. To maximize portability, it's recommended to
use atomic values everywhere possible. A list or a structured object,
for example, may need to be serialized in target formats that only allow
key-value metadata on column and table objects. This would include
storage in a database as well.

### Schemas

A schema is a group of tables.

A schema comprises a group of annotated tables and a set of annotations
that relate to that group of tables. The core annotations of a schema
are:

  - `name` \
    The name of this schema. In implementation terms, this typically
    maps to:

>   - A schema in a `CREATE SCHEMA` statement in Postgres.
>   - A database in a `CREATE DATABASE` statement in MySQL. There is
>     also a synonym for this statement under `CREATE SCHEMA`.
>   - A user in a `CREATE USER` statement in Oracle
>   - A SQLite file, which might be named according to `[name].db`

  - `@id` \
    An identifier for this group of tables. This may be used for
    relating schemas together at a higher level. Typically, the name of
    the schema can be used as the id.

  - `description` \
    A textual description of this schema

  - `tables` \
    The list of tables in the schema. A schema MUST have one or more tables.

  - `version` \
    Optional schema version description.

Schemas MAY in addition have any number of annotations which provide
information about the group of tables. Annotations on a group of tables
may include:

  - DBMS-specific information for a schema, especially for creating a
    schema.
  - IVOA metadata about the table
  - Column Groupings
  - Links to other schemas which may be related
  - Reference URLs
  - Provenance

### Schema versioning

Database schemas usually evolve over time and client software has to depend on
the knowledge of the schema version and possibly compatibility of different
schema versions. Felis supports specification of versions and their possible
relations but does not specify how exactly compatibility checks have to be
implemented. It is the client responsibility to interpret version numbers and
to define compatibility rules.

In simplest form the schema version can be specified as a value for the
`version` attribute and it must be a string:

    version: "4.2.0"

This example uses semantic version format, but in general any string or number
can be specified here.

In the extended form version can be specified using nested attributes:

  - `current` \
    Specifies current version defined by the schema, must be a string.

  - `compatible` \
    Specifies a list of versions that current schema is fully-compatible with,
    all items must be strings.

  - `read_compatible` \
    Specifies a list of versions that current schema is read-compatible with,
    all items must be strings.

Naturally, compatibility behavior depends on the code that implements reading
and writing of the data. An example of version specification using the extended
format:

    version:
      current: "v42"
      compatible: ["v41", "v40"]
      read_compatible: ["v39", "v38"]

### Tables

A Table within a Schema. The core annotations of a table are:

  - `name` \
    The name of this table. In implementation terms, this typically maps
    to a table name in a `CREATE TABLE` statement in a
    MySQL/Oracle/Postgres/SQLite.

  - `@id` \
    an identifier for this table

  - `description` \
    A textual of this table

  - `columns` \
    the list of columns in the table. A table MUST have one or more
    columns and the order of the columns within the list is significant
    and MUST be preserved by applications.

  - `primaryKey` \
    A column reference that holds either a single reference to a column
    id or a list of column id references for compound primary keys.

  - `constraints` \
    the list of constraints for the table. A table MAY have zero or more
    constraints. Usually these are Forein Key constraints.

  - `indexes` \
    the list of indexes in the schema. A schema MAY have zero or more
    indexes.

Tables MAY in addition have any number of annotations which provide
information about the table. Annotations on a table may include:

  - DBMS-specific information for a table, such as storage engine.
  - IVOA metadata about the table, such as utype
  - Links to other tables which may be related
  - Provenance

### Columns

Represents a column in a table. The core annotations of a column are:

  - `name` \
    the name of the column.

  - `@id` \
    an identifier for this column

  - `description` \
    A textual description of this column

  - `datatype` \
    the expected datatype for the value of the column. This is the
    canonical datatype, but may often be overridden by additional
    annotations for DBMS or format-specific datatypes.

  - `value` \
    the default value for a column. This is used in DBMS systems that
    support it, and it may also be used when processing a table.

  - `length` \
    the length for this column. This is used in types that support it,
    namely `char`, `string`, `unicode`, `text`, and `binary`.

  - `nullable` \
    if the column is nullable. When set to `false`, this will cause a
    `NOT NULL` to be appended to SQL DDL. false. A missing value is
    assumed to be equivalent to `true`. If the value is set to `false`
    and the column is referenced in the `primaryKey` property of a
    table, then an error should be thrown during the processing of the
    metadata.

  - `autoincrement` \
    If the column is the primary key or part of a primary key, this may
    be used to specify autoincrement behavior. We derive semantics from
    [SQLAlchemy.](https://docs.sqlalchemy.org/en/rel_1_1/core/metadata.html#sqlalchemy.schema.Column.params.autoincrement)

Columns MAY in addition have any number of annotations which provide
information about the column. Annotations on a table may include:

  - DBMS-specific information for a table, such as storage engine.
  - IVOA metadata about the table, such as utype
  - Links to other tables which may be related
  - Provenance

### Indexes

<div class="warning">

<div class="admonition-title">

Warning

</div>

This section is under development

</div>

An index that is annotated with a table. An index is typically
associated with one or more columns from a table, but it may consist of
expressions involving the columns of a table instead.

The core annotations of an index are:

  - `name` \
    The name of this index. This is optional.

  - `@id` \
    an identifier for this index

  - `description` \
    A textual description of this index

  - `columns` \
    A column reference property that holds either a single reference to
    a column description object within this schema, or an list of
    references. *This annotation is mutually exclusive with the
    expressions annotation.*

  - `expressions` \
    A column reference property that holds either a single column
    expression object, or a list of them. *This annotation is mutually
    exclusive with the columns annotation.*

### Constraints

<div class="warning">

<div class="admonition-title">

Warning

</div>

This section is under development

</div>

  - `name` \
    The name of this constraint. This is optional.

  - `@id` \
    an identifier for this constraint

  - `@type` \
    One of `ForeignKey`, `Unique`, `Check`. *Required.*

  - `description` \
    A description of this constraint

  - `columns` \
    A column reference property that holds either a single reference to
    a column description object within this schema, or an list of
    references.

  - `referencedColumns` \
    A column reference property that holds either a single reference to
    a column description object within this schema, or an list of
    references. Used on *ForeignKey* Constraints.

  - `expression` \
    A column expression object. Used on *Check* Constraints.

  - `deferrable` \
    If `true`, emit DEFERRABLE or NOT DEFERRABLE when issuing DDL for
    this constraint.

  - `initially` \
    If set, emit INITIALLY when issuing DDL for this constraint.

### References

<div class="warning">

<div class="admonition-title">

Warning

</div>

This section is under development

</div>

References are annotated objects which hold a reference to a single
object, usually a Column or a Column Grouping. While a reference to a
column might normally be just an `@id`, we create a special object so
that the reference itself may be annotated with additional information.
This is mostly useful in the case of Column Groupings.

In VOTable, this is similar to the `FIELDref` and `PARAMref` objects.
It's also similar a `GROUP` nested in a `GROUP`, which provides an
implicit reference where the nested GROUP would have an implicit
reference to the parent.

  - `name` \
    The name of this reference

  - `@id` \
    an identifier for this reference

  - `description` \
    A description of the reference

  - `reference` \
    The id of the object being referenced

### Column Groupings

<div class="warning">

<div class="admonition-title">

Warning

</div>

This section is incomplete

</div>

Groupings are annotated objects that contain one or more references to
other objects.

  - `name` \
    The name of this table. In implementation terms, this typically maps
    to a table name in a `CREATE TABLE` statement in a
    MySQL/Oracle/Postgres/SQLite.

  - `@id` \
    an identifier for this grouping, so that it may be referenced.

  - `description` \
    A description of the grouping

  - `reference` \
    A reference to another column grouping, if applicable.

  - `columnReferences` \
    A list of column references in the table. A Column Grouping MUST
    have one or more column
references.

## Datatypes

| Type    | C++    | Python | Java     | JDBC     | SQLAlchemy\[1\]     | Notes       |
| ------- | ------ | ------ | -------- | -------- | ------------------- | ----------- |
| boolean | bool   | bool   | boolean  | BOOLEAN  | BOOLEAN             |             |
| byte    | int8   | int    | byte     | TINYINT  | SMALLINT            | [2](#note2) |
| short   | int16  | int    | short    | SMALLINT | SMALLINT            |             |
| int     | int32  | int    | int      | INTEGER  | INTEGER             |             |
| long    | int64  | int    | long     | BIGINT   | BIGINT              |             |
| float   | float  | float  | float    | FLOAT    | FLOAT               |             |
| double  | double | float  | double   | DOUBLE   | FLOAT(precision=53) |             |
| char    | string | str    | String   | CHAR     | CHAR                | [3](#note3) |
| string  | string | str    | String   | VARCHAR  | VARCHAR             | [3](#note3) |
| unicode | string | str    | String   | NVARCHAR | NVARCHAR            | [3](#note3) |
| text    | string | str    | String   | CLOB     | CLOB                |             |
| binary  | string | bytes  | byte\[\] | BLOB     | BLOB                |             |

| Type    | MySQL    | SQLite   | Oracle        | Postgres         | Avro    | Parquet     | Notes       |
| ------- | -------- | -------- | ------------- | ---------------- | ------- | ----------- | ----------- |
| boolean | BIT(1)   | BOOLEAN  | NUMBER(1)     | BOOLEAN          | boolean | BOOLEAN     | [5](#note5) |
| byte    | TINYINT  | TINYINT  | NUMBER(3)     | SMALLINT         | int     | INT\_8      |             |
| short   | SMALLINT | SMALLINT | NUMBER(5)     | SMALLINT         | int     | INT\_16     |             |
| int     | INT      | INTEGER  | INTEGER       | INT              | int     | INT\_32     |             |
| long    | BIGINT   | BIGINT   | NUMBER(38, 0) | BIGINT           | long    | INT\_64     |             |
| float   | FLOAT    | FLOAT    | FLOAT         | FLOAT            | float   | FLOAT       |             |
| double  | DOUBLE   | DOUBLE   | FLOAT(24)     | DOUBLE PRECISION | double  | DOUBLE      |             |
| char    | CHAR     | CHAR     | CHAR          | CHAR             | string  | UTF8/STRING |             |
| string  | VARCHAR  | VARCHAR  | VARCHAR2      | VARCHAR          | string  | UTF8/STRING |             |
| unicode | NVARCHAR | NVARCHAR | NVARCHAR2     | VARCHAR          | string  | UTF8/STRING |             |
| text    | LONGTEXT | TEXT     | CLOB          | TEXT             | string  | UTF8/STRING |             |
| binary  | LONGBLOB | BLOB     | BLOB          | BYTEA            | bytes   | BYTE\_ARRAY |             |

| Type    | xsd          | VOTable          | Notes       |
| ------- | ------------ | ---------------- | ----------- |
| boolean | boolean      | boolean          |             |
| byte    | byte         | unsignedByte     | [3](#note3) |
| short   | short        | short            |             |
| int     | int          | int              |             |
| long    | long         | long             |             |
| float   | float        | float            |             |
| double  | double       | double           |             |
| char    | string       | char\[\]         | [3](#note3) |
| string  | string       | char\[\]         | [3](#note3) |
| unicode | string       | unicodeChar\[\]  | [3](#note3) |
| text    | string       | unicodeChar\[\]  |             |
| binary  | base64Binary | unsignedByte\[\] | [6](#note6) |

**Notes:**

  - \[1\] This is the default SQLAlchemy Mapping. It's expected
    implementations processing felis descriptions will use
    [with\_variant](https://docs.sqlalchemy.org/en/latest/core/type_api.html#sqlalchemy.types.TypeEngine.with_variant)
    to construct types based on the types outlined for specific database
    engines.
  - \[2\] SQLAlchemy has no "TinyInteger", so you need to override, or
    the default is SMALLINT
  - \[3\] The length is an additional parameter elsewhere for VOTable
    types
  - \[4\] This is a single byte value between 0-255, not a member of a
    byte array. It's preferable to not use this type.
  - \[5\] [Parquet Logical types from
    Thrift](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
  - \[6\] There's also hexBinary, but it was not considered as the
    target format is usually human-readable XML

## DBMS Extensions

DBMS Extension Annotations may be used to override defaults or provide a
way to describe non-standard parameters for creating objects in a
database or file.

[The SQLAlchemy documentation on
dialects](https://docs.sqlalchemy.org/en/latest/dialects/mysql.html) is
a good reference for where most of these originate from, and what we
might implement.

Typically, DDL must be executed only after a schema (Postgres/MySQL),
user (Oracle), or file (SQLite) has already been created. Tools SHOULD
take into account the name of the schema defined in a felis description,
but parameters for creating the schema object are beyond the scope of a
felis description, because those parameters will likely be
instance-dependent and may contain secrets, as in the case of Oracle.

### MySQL

This properties are defined within the context of `http://mysql.com/`.
If using the the recommended default context, this means the `engine`
property for a table would translate to `mysql:engine`, for example.

#### Table

  - `engine` \
    The engine for this database. Usually `INNODB` would is the default
    for most instances of MySQL. `MYISAM` provides better performance.

  - `charset` \
    The charset for this table. `latin1` is a typical default for most
    installations. `utf8mb4` is probably a more sensible default.

#### Column

  - `datatype` \
    The MySQL specific datatypes for a column.

### Oracle

This properties are defined within the context of
`http://oracle.com/database/`. If using the the recommended default
context, this means the `datatype` property for a column would translate
to `oracle:datatype`, for example.

In the future, we could think about adding support for temporary tables
and specifiying Sequences for column primary keys.

#### Table

  - `compress` \
    If this table is to use Oracle compression, set this to `true` or
    some other value

#### Index

  - `bitmap` \
    If an index should be a bitmap index in Oracle, set this to `true`.

### SQLite

This properties are defined within the context of `http://sqlite.org/`.
If using the the recommended default context, this means the `datatype`
property for a column would translate to `sqlite:datatype`, for example.

## Processing Metadata

> \*\*This section is under development

## Creating annotated tables

> \*\*This section is under development

## Metadata Compatibility

*This section is non-normative.*

As mentioned before, to maximize portability, it's recommended to use
atomic values everywhere possible. A list or a structured object, for
example, may need to be serialized as a string (usually JSON) for target
formats that only allow key-value metadata on column and table objects.
This would include un-mapped storage to a database table.

In the case where all annotations are pure atoms, we can represent the
annotations in virtually every format or model which allows a way to
store key-value metadata on table and columns. This includes parquet
files and afw.table objects.

We assume that atomic values of an annotation will likely be stored as
string in most formats. This means libraries processing the metadata may
need to translate a formatted number back to a float or double. Most of
this can probably be automated with a proper vocabulary for Felis.

### Formats and Models

\*\*This section is under development

#### afw.table

A few of the metadata values for tables and columns are storable on in
the properties of a schema (table) or field.

#### YAML/JSON

This is the most natural format. Note that `@id` fields must be quoted
in a YAML file.

#### FITS

A convention and vocabulary for FITS header keywords is being developed.
In general, a FITS keyword includes a name, a value, and a comment.

#### Avro

As Avro is very similar to YAML and JSON

#### Parquet

Parquet files allow key-value metadata on column and table objects,
though all values must be strings.

#### Relational Databases

Relational database do not necessarily have facilities to directly
annotate columns and tables. However, we

#### VOTable

The annotations for columns and tables should be reused where possible.
The Column Groupings are based off of the `GROUP` element in VOTable.

#### HDF5 and PyTables

PyTables is an opinionated way of representing tabular data in HDF5.

## Examples

    ---
    name: sdqa
    description: The SDQA Schema
    tables:
    - name: sdqa_ImageStatus
      "@id": "#sdqa_ImageStatus"
      description: Unique set of status names and their definitions, e.g. 'passed', 'failed',
        etc.
      columns:
      - name: sdqa_imageStatusId
        "@id": "#sdqa_ImageStatus.sdqa_imageStatusId"
        datatype: short
        description: Primary key
        mysql:datatype: SMALLINT
      - name: statusName
        "@id": "#sdqa_ImageStatus.statusName"
        datatype: string
        description: One-word, camel-case, descriptive name of a possible image status
          (e.g., passedAuto, marginallyPassedManual, etc.)
        length: 30
        mysql:datatype: VARCHAR(30)
      - name: definition
        "@id": "#sdqa_ImageStatus.definition"
        datatype: string
        description: Detailed Definition of the image status
        length: 255
        mysql:datatype: VARCHAR(255)
      primaryKey: "#sdqa_ImageStatus.sdqa_imageStatusId"
      mysql:engine: MyISAM

    - name: sdqa_Metric
      "@id": "#sdqa_Metric"
      description: Unique set of metric names and associated metadata (e.g., 'nDeadPix';,
        'median';, etc.). There will be approximately 30 records total in this table.
      columns:
      - name: sdqa_metricId
        "@id": "#sdqa_Metric.sdqa_metricId"
        datatype: short
        description: Primary key.
        mysql:datatype: SMALLINT
      - name: metricName
        "@id": "#sdqa_Metric.metricName"
        datatype: string
        description: One-word, camel-case, descriptive name of a possible metric (e.g.,
          mSatPix, median, etc).
        length: 30
        mysql:datatype: VARCHAR(30)
      - name: physicalUnits
        "@id": "#sdqa_Metric.physicalUnits"
        datatype: string
        description: Physical units of metric.
        length: 30
        mysql:datatype: VARCHAR(30)
      - name: dataType
        "@id": "#sdqa_Metric.dataType"
        datatype: char
        description: Flag indicating whether data type of the metric value is integer
          (0) or float (1).
        length: 1
        mysql:datatype: CHAR(1)
      - name: definition
        "@id": "#sdqa_Metric.definition"
        datatype: string
        length: 255
        mysql:datatype: VARCHAR(255)
      primaryKey: "#sdqa_Metric.sdqa_metricId"
      constraints:
      - name: UQ_sdqaMetric_metricName
        "@id": "#UQ_sdqaMetric_metricName"
        "@type": Unique
        columns:
        - "#sdqa_Metric.metricName"
      mysql:engine: MyISAM

    - name: sdqa_Rating_ForAmpVisit
      "@id": "#sdqa_Rating_ForAmpVisit"
      description: Various SDQA ratings for a given amplifier image. There will approximately
        30 of these records per image record.
      columns:
      - name: sdqa_ratingId
        "@id": "#sdqa_Rating_ForAmpVisit.sdqa_ratingId"
        datatype: long
        description: Primary key. Auto-increment is used, we define a composite unique
          key, so potential duplicates will be captured.
        mysql:datatype: BIGINT
      - name: sdqa_metricId
        "@id": "#sdqa_Rating_ForAmpVisit.sdqa_metricId"
        datatype: short
        description: Pointer to sdqa_Metric.
        mysql:datatype: SMALLINT
      - name: sdqa_thresholdId
        "@id": "#sdqa_Rating_ForAmpVisit.sdqa_thresholdId"
        datatype: short
        description: Pointer to sdqa_Threshold.
        mysql:datatype: SMALLINT
      - name: ampVisitId
        "@id": "#sdqa_Rating_ForAmpVisit.ampVisitId"
        datatype: long
        description: Pointer to AmpVisit.
        mysql:datatype: BIGINT
        ivoa:ucd: meta.id;obs.image
      - name: metricValue
        "@id": "#sdqa_Rating_ForAmpVisit.metricValue"
        datatype: double
        description: Value of this SDQA metric.
        mysql:datatype: DOUBLE
      - name: metricSigma
        "@id": "#sdqa_Rating_ForAmpVisit.metricSigma"
        datatype: double
        description: Uncertainty of the value of this metric.
        mysql:datatype: DOUBLE
      primaryKey: "#sdqa_Rating_ForAmpVisit.sdqa_ratingId"
      constraints:
      - name: UQ_sdqaRatingForAmpVisit_metricId_ampVisitId
        "@id": "#UQ_sdqaRatingForAmpVisit_metricId_ampVisitId"
        "@type": Unique
        columns:
        - "#sdqa_Rating_ForAmpVisit.sdqa_metricId"
        - "#sdqa_Rating_ForAmpVisit.ampVisitId"
      indexes:
      - name: IDX_sdqaRatingForAmpVisit_metricId
        "@id": "#IDX_sdqaRatingForAmpVisit_metricId"
        columns:
        - "#sdqa_Rating_ForAmpVisit.sdqa_metricId"
      - name: IDX_sdqaRatingForAmpVisit_thresholdId
        "@id": "#IDX_sdqaRatingForAmpVisit_thresholdId"
        columns:
        - "#sdqa_Rating_ForAmpVisit.sdqa_thresholdId"
      - name: IDX_sdqaRatingForAmpVisit_ampVisitId
        "@id": "#IDX_sdqaRatingForAmpVisit_ampVisitId"
        columns:
        - "#sdqa_Rating_ForAmpVisit.ampVisitId"
      mysql:engine: MyISAM

    - name: sdqa_Rating_CcdVisit
      "@id": "#sdqa_Rating_CcdVisit"
      description: Various SDQA ratings for a given CcdVisit.
      columns:
      - name: sdqa_ratingId
        "@id": "#sdqa_Rating_CcdVisit.sdqa_ratingId"
        datatype: long
        description: Primary key. Auto-increment is used, we define a composite unique
          key, so potential duplicates will be captured.
        mysql:datatype: BIGINT
      - name: sdqa_metricId
        "@id": "#sdqa_Rating_CcdVisit.sdqa_metricId"
        datatype: short
        description: Pointer to sdqa_Metric.
        mysql:datatype: SMALLINT
      - name: sdqa_thresholdId
        "@id": "#sdqa_Rating_CcdVisit.sdqa_thresholdId"
        datatype: short
        description: Pointer to sdqa_Threshold.
        mysql:datatype: SMALLINT
      - name: ccdVisitId
        "@id": "#sdqa_Rating_CcdVisit.ccdVisitId"
        datatype: long
        description: Pointer to CcdVisit.
        mysql:datatype: BIGINT
        ivoa:ucd: meta.id;obs.image
      - name: metricValue
        "@id": "#sdqa_Rating_CcdVisit.metricValue"
        datatype: double
        description: Value of this SDQA metric.
        mysql:datatype: DOUBLE
      - name: metricSigma
        "@id": "#sdqa_Rating_CcdVisit.metricSigma"
        datatype: double
        description: Uncertainty of the value of this metric.
        mysql:datatype: DOUBLE
      primaryKey: "#sdqa_Rating_CcdVisit.sdqa_ratingId"
      constraints:
      - name: UQ_sdqaRatingCcdVisit_metricId_ccdVisitId
        "@id": "#UQ_sdqaRatingCcdVisit_metricId_ccdVisitId"
        "@type": Unique
        columns:
        - "#sdqa_Rating_CcdVisit.sdqa_metricId"
        - "#sdqa_Rating_CcdVisit.ccdVisitId"
      indexes:
      - name: IDX_sdqaRatingCcdVisit_metricId
        "@id": "#IDX_sdqaRatingCcdVisit_metricId"
        columns:
        - "#sdqa_Rating_CcdVisit.sdqa_metricId"
      - name: IDX_sdqaRatingCcdVisit_thresholdId
        "@id": "#IDX_sdqaRatingCcdVisit_thresholdId"
        columns:
        - "#sdqa_Rating_CcdVisit.sdqa_thresholdId"
      - name: IDX_sdqaRatingCcdVisit_ccdVisitId
        "@id": "#IDX_sdqaRatingCcdVisit_ccdVisitId"
        columns:
        - "#sdqa_Rating_CcdVisit.ccdVisitId"
      mysql:engine: MyISAM

    - name: sdqa_Threshold
      "@id": "#sdqa_Threshold"
      description: Version-controlled metric thresholds. Total number of these records
        is approximately equal to 30 x the number of times the thresholds will be changed
        over the entire period of LSST operations (of order of 100), with most of the
        changes occuring in the first year of operations.
      columns:
      - name: sdqa_thresholdId
        "@id": "#sdqa_Threshold.sdqa_thresholdId"
        datatype: short
        description: Primary key.
        mysql:datatype: SMALLINT
      - name: sdqa_metricId
        "@id": "#sdqa_Threshold.sdqa_metricId"
        datatype: short
        description: Pointer to sdqa_Metric table.
        mysql:datatype: SMALLINT
      - name: upperThreshold
        "@id": "#sdqa_Threshold.upperThreshold"
        datatype: double
        description: Threshold for which a metric value is tested to be greater than.
        mysql:datatype: DOUBLE
      - name: lowerThreshold
        "@id": "#sdqa_Threshold.lowerThreshold"
        datatype: double
        description: Threshold for which a metric value is tested to be less than.
        mysql:datatype: DOUBLE
      - name: createdDate
        "@id": "#sdqa_Threshold.createdDate"
        datatype: timestamp
        description: Database timestamp when the record is inserted.
        value: CURRENT_TIMESTAMP
        mysql:datatype: TIMESTAMP
      primaryKey: "#sdqa_Threshold.sdqa_thresholdId"
      indexes:
      - name: IDX_sdqaThreshold_metricId
        "@id": "#IDX_sdqaThreshold_metricId"
        columns:
        - "#sdqa_Threshold.sdqa_metricId"
      mysql:engine: MyISAM
