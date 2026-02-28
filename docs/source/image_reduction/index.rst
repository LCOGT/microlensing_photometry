========================
Image Reduction Workflow
========================

The package's image reduction application contains the libraries and
pipeline workflow to run aperture photometry on imaging dataset.

The application includes two core pipeline workflows, both in the
```infrastructure/``` subdirectory:

* ```aperture_pipeline.py```: Performs aperture photometry for a single
        dataset, consisting of multiple images obtained with the
        same class of imaging instrument and the same filter.
* ```reduction_manager.py```: Orchestrates the reduction of multiple
        imaging datasets in parallel.

Reducing a Single Dataset
=========================

Configuring a Single Dataset for Reduction
------------------------------------------

All images for a single dataset's reduction are collected into a
single directory, referred to as ```red_dir```.  In addition to the
FITS image files, the directory also needs to contain a copy of
```microlensing_photometry/configuration/example_reduction_configuration.yaml```,
named ``reduction_config.yaml``.  Note that it is important that this file has this name.
This file needs to be edited to provide the parameters specific to
that dataset.

.. code-block:: yaml

    target:
      name: 'TEST'
      RA: '17:30:25.5'      # Sexigesimal string
      Dec: '-25:30:30.5'    # Sexigesimal string
    photometry:
      aperture_arcsec: 2.0
    tom:
      upload: True
      config_file: /path/to/config.yaml
      data_label: 'TEST'

The ```aperture_arcsec``` parameter determines the radius of the
aperture that will be used in the photometry.

The parameters in the ```tom``` dictionary control whether the
timeseries photometry for the target object will be uploaded to
a TOM system once the pipeline has completed its reduction.
This function can be switched on or off via the ```upload``` parameter.
The ```config_file``` parameter gives the full path to the
user's ```tom_config.yaml``` file.  This should be a local copy of
```/microlensing_photometry/image_reduction/configuration/example_tom_config.yaml```,
updated to contain the URL of the TOM system and the user's account details.

Running the Reduction
---------------------

With the configuration file in place, the dataset can be reduced by
passing the full path to the configuration file to the ```aperture_pipeline.py```
workflow.

Note that the prefect server has to be running before starting the download
workflow (see :doc:`Prefect Workflows <../prefect/index>` for details).

.. code-block:: python

    venv> cd microlensing_photometry/
    venv> poetry run python image_reduction/infrastructure/aperture_pipeline.py <path_to_dataset_dir>

The process of the pipeline as it runs can be monitored from the Prefect dashboard.

The pipeline also writes detailed logging output for each stage to
the ```red_dir``` in a file called ```aperture_pipeline.log```.

Dataset Locks
-------------

It should be noted that, to avoid accidently starting multiple
reductions of the same dataset, the ```aperture_pipeline.py``` workflow
automatically locks an active ```red_dir``` until it has finished
processing.


Reducing Multiple Datasets
==========================

For larger reduction tasks, ```reduction_manager.py``` provides a
convenient way to parallelize the processing of multiple datasets.

This process assumes that each dataset has a custom reduction configuration
file already present in the ```red_dir```, as this will be created
in normal operations by the ```data_download.py``` pipeline.  If not,
each dataset should be configured as for a single dataset reduction.

Configuring Multiple Datasets for Reduction
-------------------------------------------

The template reduction manager pipeline configuration should be
copied to the user's local configuration directory:

.. code-block:: bash

    cp ./microlensing_photometry/image_reduction/configuration/example_reduction_manager_configuration.yaml <root_path>/<data_reduction_dir>/<config_dir>/reduction_manager_config.yaml

The parameters can then be configured as follows:

.. code-block:: yaml

    log_dir: '/path/to/logging/directory/'
    data_reduction_dir: '/path/to/top-level/reduction/directory/'
    software_dir: '/path/to/software/installation/directory/'
    instrument_list: ['sinistro', 'qhy']
    dataset_selection:
      group: 'file'
      file: '/path/to/datasets/file/reduce_datasets.txt'
      start_date: 'None'
      end_date: 'None'
      ndays: 0
    max_parallel: 5

The directory path parameters should be the full path strings to
the data directories as described in :doc:`Data Directory Structure <../data_structure/index>`).
The software directory path should point to the top-level of the
package's own installation i.e. ```<path>/microlensing_photometry/```.

Entries in the ```instrument_list``` parameter will be used to
identify the subdirectories of each target's directory that should
be searched for data to process.  That is, if the list includes
```sinistro```, the pipeline will scan all target directories with
```target/sinistro/``` subdirectories for datasets to process.

The pipeline allows the user to restrict the number of parallel
reductions that can be triggered at any one time using the
```max_parallel``` parameter.

The reduction manager can be configured to process different groups
of data using the ```data_selection``` dictionary, and the
following options are supported.

The ```reduction_manager.py``` respects dataset locks,
and will not duplicate a reduction under any configuration, so
locked datasets will not be processed.

* **group: 'all'**

    The pipeline will scan all available targets for unlocked
    datasets to process.  The other ```data_selection```
    configuration parameters will be ignored.

* **group: 'date'**

    The pipeline will scan all available targets for unlocked
    datasets containing data obtained between the configured
    ```start_date``` and ```end_date```.  The format of both
    date strings should be '%Y-%m-%d'.

* **group: 'recent'**

    The pipeline will scan all available targets for unlocked
    datasets with data obtained within ```ndays``` of the current
    date, where ```ndays``` can be an integer or float.  This
    is used to set appropriate ```start_date``` and ```end_date```
    parameters, overriding any given in the configuration.

* **group: 'file'**

    The pipeline will load a file containing the paths of
    specific datasets to be processed.  Note that it will still exclude
    any locked datasets.  The ```file``` parameter should give the
    full path to this file, which should be in ASCII text, and
    contain a list of relative paths with respect to the ```data_reduction_dir```, e.g.

.. code-block::

    OGLE-2024-BLG-0034/sinistro/gp
    OGLE-2024-BLG-0034/sinistro/ip

Running Multiple Reductions
---------------------------

The workflow can then be used to trigger parallelized pipeline runs by
passing the full path to the configuration file to
```reduction_manager.py```.
As before, the prefect server has to be running before starting the download
workflow (see :doc:`Prefect Workflows <../prefect/index>` for details).


.. code-block:: python

    venv> cd microlensing_photometry/
    venv> poetry run python image_reduction/infrastructure/reduction_manager.py <path_to_reduction_manager_config.yaml>

The process of the pipeline as it runs can be monitored from the Prefect dashboard.

The workflow also writes detailed logging output for each stage to
the pipeline's ```log_dir``` in a file called ```arcon_pipeline.log```.
