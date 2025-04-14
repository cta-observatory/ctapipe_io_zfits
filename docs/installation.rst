Installation
============

User Installation
-----------------

Install from PyPI using pip:

.. code-block:: shell

   $ pip install ctapipe_io_zfits

or from conda-forge

.. code-block:: shell

   $ conda install ctapipe_io_zfits


Developer Setup
---------------

As a developer, clone the repository, create a virtual environment
and then install the package in development mode:

.. code-block:: shell

   $ git clone git@gitlab.cta-observatory.org:cta-computing/documentation/python-project-template
   $ cd python-project-template
   $ python -m venv venv
   $ source venv/bin/activate
   $ pip install -e '.[all]'

The same also works with conda, create a conda env instead of a venv above.
