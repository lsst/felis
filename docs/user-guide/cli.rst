Command Line Interface
======================

.. click:: felis.cli:cli
   :prog: felis
   :nested: full

Schema browser
==============

The ``browser`` command generates a static HTML site for browsing schema
structure: schemas, tables, columns, and constraints.

.. code-block:: bash

   felis browser --output-dir ./site tests/data/test.yml tests/data/sales.yaml

You can use shell expansion to provide file lists:

.. code-block:: bash

   felis browser --output-dir ./site ../sdm_schemas/yml/*.yaml

The output directory contains:

- ``index.html``: the home page, listing all schemas.
- ``schemas/<schema>/index.html``: one page per schema, listing its tables.
- ``schemas/<schema>/<table>.html``: one page per table, showing its columns,
  primary key, indexes, and constraints.
- ``assets/``: the stylesheet and scripts shared by all pages, including the
  data for the navigation sidebar.

The site needs no web server; open ``index.html`` directly in a browser.
