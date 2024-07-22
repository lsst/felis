########
Examples
########

The `SDM Schemas github repository <https://github.com/lsst/sdm_schemas>`_ contains Felis schema files used by
the Rubin Observatory, which will be used as examples to illustrate the features of Felis.
The following is an excerpt from the
`DP0.2 DC2 schema <https://github.com/lsst/sdm_schemas/blob/main/yml/dp02_dc2.yaml>`_:

.. code-block:: yaml
   :linenos:

   ---
   name: dp02_dc2_catalogs
   "@id": "#dp02_dc2_catalogs"
   description: "Data Preview 0.2 contains the image and catalog products of the Rubin Science
   Pipelines v23 processing of the DESC Data Challenge 2 simulation, which covered 300 square
   degrees of the wide-fast-deep LSST survey region over 5 years."
   tables:
   - name: Object
     "@id": "#Object"
      description: "Properties of the astronomical objects detected and measured on the deep coadded images."
      tap:table_index: 1
      columns:
      - name: objectId
        "@id": "#Object.objectId"
        datatype: long
        description: Unique id. Unique ObjectID
        ivoa:ucd: meta.id;src;meta.main
      - name: coord_ra
        "@id": "#Object.coord_ra"
        datatype: double
        description: Fiducial ICRS Right Ascension of centroid used for database indexing
        fits:tunit: deg
        ivoa:ucd: pos.eq.ra;meta.main

Lines 2-6 define the schema name, id, and description.
Name and id are required for all objects in Felis schemas.
The description is optional but highly recommended for documentation purposes.

Next is a list of table definitions, starting with the Object table on line 18.
Each table definition includes a name, id, description, and a list of columns, and may also include TAP-specific metadata.

A table is comprised of one or more columns which must must have a name, id, and datatype, and an optional
(but highly recommended to include) description.
The `Column <../dev/internals/felis.datamodel.Column.html>`_ class provides a full list of available fields,
including TAP and VOTable-specific metadata.

Both fields shown here have an `IVOA UCD <https://www.ivoa.net/documents/cover/UCD-20050812.html>`_ field,
which is a "vocabulary for describing astrononomical data quantities," describing the semantics of the fields.
The first column in the Object table is ``objectId``, which is a long integer field defining a unique identifier
for records in the table.
The ``meta.id`` word in the column's UCD flags the field semantically as an identifier.
The second exerted column is ``coord_ra``, which is a measurement field including units of measurement.

Felis also supports table constraints, such as foreign keys.
The `DP0.2 DC2 schema <https://github.com/lsst/sdm_schemas/blob/main/yml/dp02_dc2.yaml>`_ includes a foreign
key constraint on the ``ccdVisitId`` field of the ``Source`` table, defined as follows:

.. code-block:: yaml
   :linenos:

   constraints:
   - name: CcdV_Src
     "@type": "ForeignKey"
     "@id": "#FK_Source_ccdVisitId_CcdVisit_ccdVisitId"
     description: Link CCD-level images to associated Sources
     columns:
     - "#Source.ccdVisitId"
     referencedColumns:
     - "#CcdVisit.ccdVisitId"

The ``ccdVisitId`` field in the ``Source`` table is linked to the ``ccdVisitId`` field in the ``CcdVisit``
table.

Felis schemas support many additional features. Refer to the `model documentation <model>`_ for a complete list.
