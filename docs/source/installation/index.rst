==============================
Installation and Configuration
==============================

Installation
============

The entire package can be installed in any Python environment
(v3.10 or later) via pip:

.. code-block:: python

    pip install microlensing_photometry

This allows all modules of the pipeline to be used as Python
libraries.

In order to run the pipeline scripts, it is easiest to clone this repository:

.. code-block::

    git clone https://github.com/LCOGT/microlensing_photometry

The package includes a ```pyproject.toml``` file which can be used to
streamline the creation of a new Python virtual environment in which
to run the software.

.. code-block:: python

    > python3 -m venv venv
    > source venv/bin/activate
    venv> pip install poetry
    venv> cd microlensing_photometry/
    venv> poetry install

Once this is done, all pipeline scripts can be run from within the virtual environment
using:

.. code-block:: python

    > source venv/bin/activate
    venv> poetry run python <path/name_of_script> <args>

External Package Dependencies
=============================

The data management software depends on ```fpack``` to handle compressed
data.  This is available from `HEASARC <https://heasarc.gsfc.nasa.gov/docs/software/fitsio/fpack/fpack.html>`_.

Configuration
=============

All applications within this package are configured using YAML files.
Template configuration files are included within the repository for reference
- these can be found in:

.. code-block::

    ./microlensing_photometry/data_management/configuration/
                            |─ example_data_download.yaml


and

.. code-block::

    ./microlensing/image_reduction/configuration/
                |- example_reduction_configuration.yaml
                |- example_reduction_manager_configuration.yaml


These should be copied to a location outside the directory containing
the package's code to avoid confusion or accidental uploads to Github.

Each local config file should then be updated as follows to match the
user's local set-up.

Setting up the data reduction directory structure
=================================================

The pipeline applications require an initial set of data directories
to be available:

.. code-block::

    <root_path>/<data_reduction_dir>/
                |- <data_download_dir>
                |- <config_dir>
                |- <log_dir>

For more information about the required directories, see :doc:`Data Directory Structure <../data_structure/index>`.

Prefect Server
==============

The pipelines in this package use the `Prefect <https://www.prefect.io/prefect/open-source>`_
library to orchestrate the workflow (for more information, see
:doc:`Prefect Workflows <../prefect/index>`).  Before you run any of the
pipelines you need to start the prefect server as follows:

.. code-block::

    > source /<path>/venv/bin/activate
    venv> prefect server start
