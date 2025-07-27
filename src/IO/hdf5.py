import h5py
import numpy as np
from astropy.coordinates import SkyCoord
from astropy import units as u
from src.astrometry import wcs as lcowcs
from src.infrastructure import logs as lcologs

def output_photometry(catalog, obs_set, flux, err_flux, pscales, epscales, file_path, log=None):
    """
    Function to output a dataset photometry table to an HD5 file

    Parameters:
        catalog  Gaia catalog object containing all known objects in the field of view
        obs_set ObservationSet object for the current dataset
        flux    array  Normalized fluxes
        err_flux array Normalized flux uncertainties
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
    source_id = catalog['source_id'].data
    source_radec = SkyCoord(ra=catalog['ra'], dec=catalog['dec'], unit=(u.degree, u.degree))
    wcs_positions = np.c_[catalog['ra'], catalog['dec']]
    im_wcs = lcowcs.build_wcs_from_obs_set(obs_set)
    positions = np.zeros((len(catalog), len(obs_set.table), 2))
    for i in range(1,len(obs_set.table),1):
        xx, yy = im_wcs[i].world_to_pixel(source_radec)
        positions[:,i,0] = xx
        positions[:,i,1] = yy
    positions = np.array(positions)

    with h5py.File(file_path, "w") as f:
        d1 = f.create_dataset(
            'source_id',
            source_id.shape,
            dtype='int64',
            data=source_id
        )

        d2 = f.create_dataset(
            'source_wcs',
            wcs_positions.shape,
            dtype='float64',
            data=wcs_positions
        )

        d3 = f.create_dataset(
            'positions',
            positions.shape,
            dtype='float64',
            data=positions
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

        d7 = f.create_dataset(
            'pscale',
            pscales.shape,
            dtype='float64',
            data=pscales
        )

        d8 = f.create_dataset(
            'epscale',
            epscales.shape,
            dtype='float64',
            data=epscales
        )

    f.close()
