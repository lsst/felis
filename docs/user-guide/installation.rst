############
Installation
############

Felis is deployed to PyPI on a weekly release schedule.

To install the latest release, use the following command:

.. code-block:: bash

    pip install lsst-felis

A specific release can be installed using a command similar to the following:

.. code-block:: bash

    pip install 'lsst-felis==27.2024.2700'

This is just an example.
If you want to install a different version, replace the version number from above.

To install Felis directly from the Github ``main`` branch, use the following command:

.. code-block:: bash

    pip install 'lsst-felis @ git+http://github.com/lsst/felis.git@main'

Felis may also be added as a dependency of your Python project by putting the following in ``requirements.txt``:

.. code-block:: text

    lsst-felis

Felis requires Python 3.11 or later and is regularly tested on Linux, macOS, and Windows WSL.
