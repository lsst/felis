############
Installation
############

Felis is deployed to PyPI on a weekly release schedule.
To install the latest release, use the following command:

.. code-block:: bash

    pip install lsst-felis

To install a specific release, use a command similar to the following:

.. code-block:: bash

    pip install 'lsst-felis==27.2024.2700'

This is just an example. If you want to install a different version, replace the version number from above
with the one you want to install.

To install Felis from the Github main branch, use the following command:

.. code-block:: bash

    pip install 'lsst-felis @ git+http://github.com/lsst/felis.git@main'

To depend on Felis in your project, add the following to your ``requirements.txt`` file:

.. code-block:: text

    lsst-felis

Felis requires Python 3.11 or later and is tested on Linux, macOS, and Windows WSL.
