========================
Data Directory Structure
========================

The pipelines require a few directories to exist in order to download,
sort and process data products.

The expected directory layout is as follows:

.. code-block::

    <root_path>/<data_reduction_dir>/
                |- <data_download_dir>
                |- <config_dir>
                |- <log_dir>

The directories can have any name you prefer provided they follow this structure,
since these paths are parameters within the YAML configuration files
for each pipeline, all of which are expected to be located in the ```<config_dir>```.

When the data_download application is run, data are temporarily stored in
```<data_download_dir>```.  They are then sorted into subdirectories of ```<data_reduction_dir>```
with a subdirectory being created for each target.  This directory
collects all datasets relating to that target, from multiple
telescopes, instruments, filters/gratings etc.  This allows for
easier cross-dataset calibrations, e.g. color measurements.

The layout of a single target's data directory is expected to have
the following structure:

.. code-block::

    <root_path>/<data_reduction_dir>/<target_name>/
                    |- instrument_class1/
                            |- dataset1
                            |- dataset2

Here is an example of this layout for a real target:

.. code-block::

    /home/userid/data_reduction/OGLE-2026-BLG-0001/
                    |- sinistro/
                            |- ip/
                            |- gp/

This design allows data to be combined from multiple instruments of
the same class where appropriate.  For example, all Sinistro cameras
on the LCO Network are designed to be sufficiently similar that
images in the same filter from multiple cameras can be processed
together as a single dataset.

All applications within the pipeline produce extensive log files,
which will all be output to ```<log_dir>```.
