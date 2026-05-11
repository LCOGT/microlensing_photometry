import h5py
import numpy as np
from astropy.coordinates import SkyCoord
from astropy import units as u
from image_reduction.astrometry import wcs as lcowcs
from image_reduction.infrastructure import logs as lcologs

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
