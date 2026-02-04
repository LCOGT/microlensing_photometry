==============================
Installation and Configuration
==============================

.. toctree::
  :maxdepth: 2
  :hidden:

Installation
============

The entire package can be installed in any Python environment
(v3.10 or later) via pip:

```pip install microlensing_photometry```

This allows all modules of the pipeline to be used as Python
libraries.

In order to run the pipeline scripts, it is easiest to clone this repository:

```git clone https://github.com/LCOGT/microlensing_photometry```

Configuration
=============

All applications within this package are configured using YAML files.
Template configuration files are included within the repository for reference
- these can be found in:

```
./microlensing_photometry/data_management/configuration/
                            |- example_data_download.yaml
```

and

```
./microlensing/image_reduction/configuration/
                |- example_reduction_configuration.yaml
                |- example_reduction_manager_configuration.yaml
```

These should be copied to a location outside the directory containing
the package's code to avoid confusion or accidental uploads to Github.

Each local config file should then be updated as follows to match the
user's local set-up.
