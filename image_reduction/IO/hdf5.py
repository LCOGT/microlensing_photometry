import h5py
import numpy as np
import os
from image_reduction.infrastructure import logs as lcologs

def fetch_dataset_list(phot_storage_path):
    """
    Function to retrieve the current list of datasets
    """

    if os.path.isfile(phot_storage_path):
        with h5py.File(phot_storage_path, "r") as phot_store:
            dataset_list = phot_store['file'].asstr()[:]
    else:
        dataset_list = []

    return dataset_list

def store_image_photometry(phot_storage_path, image_photometry):
    """
    Function to append the photometry for a single image in a layer in the HDF5 file

    :param phot_storage_path:  Path to HDF5 file
    :param image_photometry: Array of photometry measurements from a single image
    """

    phot_data = np.column_stack(
            (image_photometry['aperture_sum'], image_photometry['aperture_sum_err'])
        )

    with h5py.File(phot_storage_path, "a") as phot_store:

        nrows = len(image_photometry)

        # If photometry table extension doesn't exist, create one
        if "timeseries_photometry" not in phot_store.keys():
            dset = phot_store.create_dataset(
                "timeseries_photometry",
                shape=(nrows, 2),
                maxshape=(nrows, None),
                dtype=np.float64,
                chunks=(5000, 2),  # Optimized disk block size (~120 KB)
                data=phot_data
            )

        else:
            dset = phot_store['timeseries_photometry']

            current_cols = dset.shape[1]
            new_cols = 2

            # Expand the dataset on disk to fit the new rows
            dset.resize((nrows, current_cols + new_cols))

            # Stream the new array directly to the newly allocated disk space
            dset[:, current_cols: current_cols + new_cols] = phot_data

        # Optional: Force system to flush internal buffers to disk
        phot_store.flush()

def create_photometry_store(phot_storage_path, star_catalog, obs_set, log):
    """
    Function to initialize the HDF5 file that stores the timeseries photometry

    :param phot_storage_path:  Path to HDF5 file
    :param star_catalog: StarCatalog object containing all known objects in the field of view
    :param obs_set: ObservationSet object for the current dataset
    :param log: Logger object

    Returns:
        Output HDF5 file
    """

    # Declare a variable-length string type for HDF5:
    string_type = h5py.string_dtype(encoding="utf-8")
    clean_strings = [str(x) for x in obs_set.table['file']]
    image_list = np.array(clean_strings, dtype=object)

    # Build the source catalog
    source_table = np.c_[
        star_catalog.sources['gaia_id'], star_catalog.sources['ra'], star_catalog.sources['dec'],
        star_catalog.sources['x'], star_catalog.sources['y']
    ]

    with h5py.File(phot_storage_path, "w") as f:
        d1 = f.create_dataset(
            'source_catalog',
            source_table.shape,
            dtype='float64',
            data=source_table
        )

        d2 = f.create_dataset(
            'HJD',
            len(obs_set.table['HJD']),
            dtype='float64',
            data=obs_set.table['HJD'].data
        )

        d3 = f.create_dataset(
            'file',
            len(obs_set.table['file']),
            dtype=string_type,
            data=image_list
        )

        f.close()

        lcologs.log(
            'Created photometry storage file ' + phot_storage_path,
            'info',
            log=log
        )

def update_photometry_store(phot_storage_path, obs_set, log):
    """
    Function to update the HDF5 file that stores the timeseries photometry with additional
    data for images added in subsequent pipeline runs

    :param phot_storage_path:  Path to HDF5 file
    :param star_catalog: StarCatalog object containing all known objects in the field of view
    :param obs_set: ObservationSet object for the current dataset
    :param log: Logger object

    Returns:
        Output HDF5 file
    """

    with h5py.File(phot_storage_path, "r+") as f:

        d1 = f['HJD']
        d2 = f['file']

        d2 = obs_set.table['HJD'].data
        d3 = obs_set.table['file'].data

        f.flush()

        lcologs.log(
            'Updated photometry storage filelist',
            'info',
            log=log
        )

def output_photometry(
        star_catalog,
        obs_set,
        flux,
        err_flux,
        raw_flux,
        raw_err_flux,
        pscales,
        epscales,
        file_path,
        log=None):
    """
    Function to output a dataset photometry table to an HD5 file

    Parameters:
        star_catalog  StarCatalog object containing all known objects in the field of view
        obs_set ObservationSet object for the current dataset
        flux    array  Normalized fluxes
        err_flux array Normalized flux uncertainties
        raw_flux array Raw flux measurements
        raw_err_flux array Raw flux uncertainties
        pscales array Photometric scale factor for each image and star
        epscales array Uncertainty on the scale factor per image and star
        file_path str Path to output file

    Returns:
        Output HDF5 file
    """

    ## THIS NEEDS TO UPDATE THE HJD, FILE LIST AND PHOTOMETRY WHEN THE CODE IS RUN MORE THAN ONCE

    lcologs.log(
        'Outputting timeseries photometry to ' + file_path,
        'info',
        log=log
    )

    # Build the source catalog
    source_table = np.c_[
        star_catalog.sources['gaia_id'], star_catalog.sources['ra'], star_catalog.sources['dec'],
        star_catalog.sources['x'], star_catalog.sources['y']
    ]

    with h5py.File(file_path, "w") as f:
        d1 = f.create_dataset(
            'source_catalog',
            source_table.shape,
            dtype='float64',
            data=source_table
        )

        d4 = f.create_dataset(
            'HJD',
            len(obs_set.table['HJD']),
            dtype='float64',
            data=obs_set.table['HJD'].data
        )

        d5 = f.create_dataset(
            'flux',
            flux.shape,
            dtype='float64',
            data=flux
        )

        d6 = f.create_dataset(
            'err_flux',
            err_flux.shape,
            dtype='float64',
            data=err_flux
        )

        d6 = f.create_dataset(
            'raw_flux',
            raw_flux.shape,
            dtype='float64',
            data=raw_flux
        )

        d7 = f.create_dataset(
            'raw_err_flux',
            raw_err_flux.shape,
            dtype='float64',
            data=raw_err_flux
        )

        d8 = f.create_dataset(
            'pscale',
            pscales.shape,
            dtype='float64',
            data=pscales
        )

        d9 = f.create_dataset(
            'epscale',
            epscales.shape,
            dtype='float64',
            data=epscales
        )

    f.close()
