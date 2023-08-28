# Primitive Felis-manipulation scripts

Very simple AWK scripts for querying Felis file content and
performing bulk-change operations.

Planned to be replaced by YAML-syntax-aware operations once the
data engineering developer becomes available.

*WARNING:* These scripts may produce unexpected and nonsensical
output if the input YAML doesn't conform very precisely to the
unwritten conventions for existing sdm_schemas Felis files.

Users *MUST* validate any Felis changes carefully before committing.

Gregory Dubois-Felsmann, August 2023

## Querying for Attributes

The `report.awk` script allows specifying a regular expression to use to
select a subset of "@id" values in the file (typically, column names),
and a list of regular expression to use to select attributes.  The output
(to stdout) is one row per selected "record" (e.g., per column in the data
model), containing the @id value followed by the values of the selected
attributes for that record.

Example:

`awk -v 'target=DiaObj.*lux' -v 'attrs=tunit ucd' -f report.awk`

## Modifying Attributes

The `modify.awk` script allows specifying a regular expression to use to
select a subset of "@id" values in the file (typically, column names), a
regular expression to use to select a single attribute, and a value for
that attribute.  The output is an exact copy of the input, except that
for all "records" (e.g., columns in the data model) matching the @id
selection expression, any existing value for the selected attribute is
overwritten.  Optionally, for "records" for which no attribute matches
the selection expression, a new attribute is written at the end of the
record, using the selection expression as the full name for the attribute
(and therefore, when used in this mode, the selection expression must
be the exact value of the desired attribute's name, not some other
matching regex.

Note that due to the quick-and-dirty implementation, strange things may
happen if attributes' names contain meaningful regex metacharacters,
notably ".".

Example:

`awk -v 'target=DiaObj.*lux' -v 'attr=ucd' -v 'newval=phot.flux' -f report.awk`

## Renumbering Records

A future version will include a script for assigning sequential
column_index values to a selected set of records.
