import h5py
import numpy as np
from astropy.coordinates import SkyCoord
from astropy import units as u

def output_photometry(catalog, timestamps, wcs_data, flux, err_flux, exptime, pscales, epscales, file_path):
    """
    Function to output a dataset photometry table to an HD5 file

    Parameters:
        catalog  Gaia catalog object containing all known objects in the field of view
        Time list   MJD timestamps for all images in dataset
        wcs_data list  WCS objects for each image
        flux    array  Normalized fluxes
        err_flux array Normalized flux uncertainties
        exptime list    Exposure times for each time
        pscales array Photometric scale factor for each image and star
        epscales array Uncertainty on the scale factor per image and star
        file_path str Path to output file

    Returns:
        Output HDF5 file
    """

    # Build the source catalog
    source_id = catalog['source_id'].data
    source_radec = SkyCoord(ra=catalog['ra'], dec=catalog['dec'], unit=(u.degree, u.degree))
    wcs_positions = np.c_[catalog['ra'], catalog['dec']]
    positions = np.zeros((len(catalog), len(wcs_data), 2))
    for i,im_wcs in enumerate(wcs_data):
        xx, yy = im_wcs.world_to_pixel(source_radec)
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
            'timestamps',
            timestamps.shape,
            dtype='float64',
            data=timestamps
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
            'exptime',
            exptime.shape,
            dtype='float64',
            data=exptime
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
