===============
Data Management
===============

The package's data management application is designed to handle:

* the retrieval of observational data from online archives
* data sorting
* preparation of datasets for reduction.

These functions are combined into the ```data_download.py``` workflow.

Configuration
=============

All necessary parameters for this application are set in its YAML
configuration file.  The template configuration file should be copied
to the config directory of the local installation, i.e.

.. code-block::

    cp ./microlensing_photometry/data_management/configuration/example_data_download.yaml <root_path>/<data_reduction_dir>/<config_dir>/data_download.yaml

The parameters in this file should then be edited to reflect the
local setup as follows.

Data archives
-------------

The data_download pipeline can orchestrate downloads from multiple
data archives sequentially.  This is configured by adding configuration
dictionary entries to the list of ```archives```.  The following
example illustrates the configuration necessary to download data
from LCO's own archive.

.. code-block:: yaml

    archives:
        lco:
            name: 'LCO'
            active: True
            url: 'https://archive-api.lco.global/'
            token_envvar: 'LCO_TOKEN'
            proposals:
                - 'proposal_ID_string'
            reduction_level:
                image: 91
                spectrum: 91

        archive2:
            <etc>

The parameters required for a valid ```archive``` dictionary are:

* ```name```: (string) Name of the archive
* ```active```: (Python boolean) Switch to (de)activate downloads from this archive
* ```url```: (URL string with https prefix) Root URL for the archive's API
* ```token_envvar```: (string) Name of the environment variable containing the
        user's authentication token for the archive's API.
        Important: NOT the authentication token itself.
* ```proposals```: (list of strings) List of proposal ID strings from
        the observatory hosting the data archive; these are used to
        search the archive from data from that archive.
* ```reduction_level```: (integers) Flags indicating the specific
        data products to be downloaded in the case of either imaging
        or spectroscopic data products.

Setting environment variables for authentication tokens can be
done directly from the command prompt, e.g. (under bash):

.. code-block:: bash

    export LCO_TOKEN="my_token_here"

However, this will need to be reset for each terminal session.
To ensure the environment variable is available for every terminal
session, the same syntax can be added to the user's ```~/.bash_profile``` file.

Alternatively, you can edit the Python virtual environment's ```activate``` bash
script to add this syntax.  This can be found in ```<my_env_path>/bin/activate```.

Add an ```unset``` command to the ```deactivate``` section of the script:

.. code-block:: bash

    deactivate () {
    # reset old environment variables
    if [ -n "${_OLD_VIRTUAL_PATH:-}" ] ; then
        PATH="${_OLD_VIRTUAL_PATH:-}"
        export PATH
        unset _OLD_VIRTUAL_PATH
    fi
    ...
        if [ ! "${1:-}" = "nondestructive" ] ; then
    # Self destruct!
        unset -f deactivate
    fi

    # Unsetting microlensing_pipeline parameters:
    unset LCO_TOKEN
    }

Then add the parameter export line at the end of the ```activate``` script:

.. code-block:: bash

    ...
    # This should detect bash and zsh, which have a hash command that must
    # be called to get it to forget past commands.  Without forgetting
    # past commands the $PATH changes we made may not be respected
    if [ -n "${BASH:-}" -o -n "${ZSH_VERSION:-}" ] ; then
        hash -r 2> /dev/null
    fi

    # Set parameters for microlensing_photometry package
    LCO_TOKEN="my_token_string"
    export LCO_TOKEN

The user account tokens for any data archive are provided by that archive,
and should be accessible by visiting the website for the archive and checking
the user's account settings.

Data Directory Structure Configuration
--------------------------------------

The next section of the ``data_download.yaml``` file tells the pipeline
where to download and sort data to, and where to output the logs.
Each of the following parameters is a string that should be set to the full directory
path on the user's local filesystem:

.. code-block:: yaml

    log_dir: '<log_dir_path>'
    frame_list: '<log_dir_path>/<name_of_framelist_file>.txt'
    data_download_dir: '<data_download_dir_path>'
    data_reduction_dir: '<top_level_data_reduction_dir_path>'

For example:

.. code-block:: yaml

    log_dir: '/data/userid/data_reduction/logs/'
    frame_list: '/data/userid/data_reduction/logs/frame_list.txt'
    data_download_dir: '/data/userid/data_reduction/incoming/'
    data_reduction_dir: '/data/userid/data_reduction/'
