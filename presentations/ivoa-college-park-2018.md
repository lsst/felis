class: center, middle, white-text

background-image: url(title_page.png)
background-size: contain
color: white

# Felis - A way of describing catalogs

.footnote[IVOA Interop College Park Maryland - 11/9/2018]

---
background-image: url(presentation_page.png)
background-size: contain

# Why?
First and foremost, LSST needs a way to describe our 
[Scientific Data Model.](https://confluence.lsstcorp.org/display/DM/The+Science+Data+Model+and+its+Standardization)

Historically, we had a way of describing catalogs using pseudo-XML
annotations in SQL comments. That's in our [`cat`](https://github.com/lsst/cat) package.

The DDL was MySQL specific. Parsing properly was a bit rough.

That was insufficient, so some requirements were extracted based on needs of the project, 
including the development and operational environments.

---
background-image: url(presentation_page.png)
background-size: contain

# The Scientific Data Model:
* must contain sufficient information for a physical SQL schema definition to be derived from it, 
given a choice of SQL flavor (e.g., MariaDB, Oracle, PostgreSQL).
* is written in YAML
* must contain information that itemizes how it satisfies the DPDD requirements for the content of 
the data model.  For example, each SDM element that realizes a data item from the DPDD might 
contain a field that references the appropriate DPDD Identifier 
* Elements MUST be described by a unique identifier ("SDM Identifier") that can be used 
programmatically in applications that consume the SDM YAML definition 
* MAY contain additional elements beyond those required by the DPDD

---
background-image: url(presentation_page.png)
background-size: contain

# The LSST Operational Environment

* We have _at least_ three database technologies in use:
 MySQL(_+ Qserv_), Oracle, and SQLite. 
  - Testing has also been done in BigQuery and Postgres.
 
* We are headed towards _at least_ three tabular data formats: 
FITS, HDF5, and Parquet. Maybe Avro.

* We are using _at least_ three lanaguages - C++, Python, and Java.
  - And _at least_ three tabular libraries: afw.table, pandas, and astropy

---
background-image: url(presentation_page.png)
background-size: contain

## JSON-LD, CSVW, and linking things
CSVW is an alternate way of annotating tables, actually CSV data, and JSON-LD is the way CSVW 
describes data. 

CSVW is oriented towards data publishing, but a lot of Felis was derived from a combination of 
CSVW, SQLAlchemy, and Liquibase, in order to provide a catalog description that can be used to 
create database tables.

### Alternatives
VOTable could help with some of the problems, but we'd still need a  convention for describing 
indexes, constraints, override types, type mappings, and more. That would probably look like a 
lot of INFO, PARAM, and FIELDref elements.


---
background-image: url(presentation_page.png)
background-size: contain

# Core Objects

* `Schema` - which contains tables
* `Table`, which contains Columns, Primary Key/Constraints, and Indexes
* `Column` - A database column
* `Index` - A database index
* `Constraint` - A database constraint
* `Reference` - An annotated reference to an object
* `Grouping` - An annotated grouping of references

---
background-image: url(presentation_page.png)
background-size: contain

# Default Data Type Mappings
Trying to get default database types based on C++/Python/Java (or even VOTable) types is kind of
tough, especially across all the languages and formats. So Felis includes some default data type
 mappings.

---
background-image: url(presentation_page.png)
background-size: contain

# Vocabularies

* Would be nice to formally define SQL or dialect vocabularies

* Need some formalization on a few IVOA-specific types
 - UCDs - `ucd:ucd` -> `"ucd":"http://ivoa.net/rdf/"`
 - utype - `votable:utype` -> `"votable":"http://ivoa.net/rdf/votable/`
 - xtype - `votable:xtype` -> ...
 - unit - `votable:unit` -> 

* Need to declare a minimal set of FITS-specific headers
 - `fits:tunit` instead of `votable:unit`?
 - Of course, every item has a comment: `fits:tunit/comment`

... But for now we cheat.

---
background-image: url(presentation_page.png)
background-size: contain

# Cheating the Context

```json
{
  "@context": {
    "@vocab": "http://lsst.org/felis/",
    "mysql": "http://mysql.com/",
    "postgres": "http://postgresql.org/",
    "oracle": "http://oracle.com/database/",
    "sqlite": "http://sqlite.org/",
    "fits": "http://fits.gsfc.nasa.gov/FITS/4.0/",
    "ivoa": "http://ivoa.net/",
    "votable": "http://ivoa.net/documents/VOTable/"
  }
}
```

---
background-image: url(presentation_page.png)
background-size: contain

**Example**
```yaml
---
name: sdqa
description: The SDQA Schema
tables:
- name: sdqa_Metric
  "@id": "#sdqa_Metric"
  description: Unique set of metric names and associated metadata (e.g.,...
  columns:
  - name: sdqa_metricId
    "@id": "#sdqa_Metric.sdqa_metricId"
    datatype: short
    description: Primary key.
  - name: metricName
    "@id": "#sdqa_Metric.metricName"
    datatype: string
    description: One-word, camel-case, descriptive name of a possible metr...
    length: 30
  ...
  primaryKey: "#sdqa_Metric.sdqa_metricId"
  constraints:
  - name: UQ_sdqaMetric_metricName
    "@id": "#UQ_sdqaMetric_metricName"
    "@type": Unique
    columns:
    - "#sdqa_Metric.metricName"
  mysql:engine: MyISAM
```


---
background-image: url(presentation_page.png)
background-size: contain

### Future Directions

**Populating TAP_SCHEMA**

This is the immediate goal for LSST and Felis. It's a core requirement from the LSST Scientific 
Data Model.

**Metadata in Parquet**

Use the native `List<KeyValue>` objects in Parquet on Table and Column objects to store 
metadata. When presented with more complicated objects (Groupings), serialize them to JSON under
their proper name.  

**VOTable JSON**

A VOTable Vocabulary can allow us to mix in VOTable semantics in JSON without defining an 
explicit mapping. JSON-LD, with it's well-defined [processing algorithms and API](https://www.w3
.org/TR/json-ld-api/), can enable transformation of JSON into user-friendly formats for 
user consumption and processing.


**CSVW**

CSVW is another options for annotating CSV data. [It is used by Google for describing 
datasets](https://developers.google
.com/search/docs/data-types/dataset). Felis is vert compatible with CSVW


---
background-image: url(presentation_page.png)
background-size: contain

# More info

https://felis.lsst.io and https://github.com/lsst-dm/felis
