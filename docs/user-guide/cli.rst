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
  primary key, indexes, and constraints. The columns table can be sorted,
  searched, and exported as CSV, using DataTables loaded from a CDN (without
  internet access it is shown as a plain table).
- ``assets/``: the stylesheet and scripts shared by all pages, including the
  data for the navigation sidebar.

The site needs no web server; open ``index.html`` directly in a browser.

To customize the appearance, add your own stylesheets with ``--stylesheet``.
They are copied into ``assets/`` and applied after the built-in stylesheet,
in the order given, so later ones take precedence:

.. code-block:: bash

   felis browser --output-dir ./site --stylesheet theme.css ../sdm_schemas/yml/*.yaml

The built-in stylesheet defines its colors, fonts and spacing as CSS variables
in ``:root``, so a theme can usually just redefine those.

To change the font and text size of the tables:

.. code-block:: css

   :root {
     --font-table: Verdana, Tahoma, sans-serif;
     --font-size-table: 0.75rem;
   }

To change the color of links:

.. code-block:: css

   :root {
     --color-link: #0b7a80;
   }

``--font-body`` sets the font for the whole site. The tables use it too unless
``--font-table`` is also set.

Only the stylesheets themselves are copied, so any fonts or images they use
must be referenced by absolute URL.
