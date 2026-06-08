Command Line Interface
======================

.. click:: felis.cli:cli
   :prog: felis
   :nested: full

Browser prototype
================

The ``browser`` command generates a static HTML site for browsing schema
structure (schemas, tables, and columns):

.. code-block:: bash

   felis browser --output-dir ./site tests/data/test.yml tests/data/sales.yaml

You can use shell expansion to provide file lists:

.. code-block:: bash

   felis browser --output-dir ./site ../sdm_schemas/yml/*.yaml
