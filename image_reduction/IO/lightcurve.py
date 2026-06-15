from prefect import task
from image_reduction.astrometry import crossmatching
from image_reduction.infrastructure import logs as lcologs
from image_reduction.photometry import conversions
import argparse
from astropy.coordinates import SkyCoord
from astropy import units as u
from astropy.table import Table, Column
import numpy as np

@task
def aperture_timeseries(params, star_catalog, obs_set, dataset, log=None):
    """
    Function to plot an aperture photometry timeseries from an HDF5 output file

    Parameters
    ----------
    params    dict      Program arguments:
        'target_ra', 'target_dec', 'filter', 'lc_path'
    star_catalog StarCatalog Source table for the dataset
    obs_set ObservationSet Set of frames in the dataset
    dataset   object      AperturePhotometryDataset
    log  Logger     Logger object

    Outputs
    -------
    ASCII format lightcurve file without suffix (path with root filename only)
    """

    # Target coordinates can be in sexigesimal or decimal degree format, so handle both
    try:
        target_ra = float(params['target_ra'])
        target_dec = float(params['target_dec'])
        target = SkyCoord(target_ra, target_dec, frame='icrs', unit=(u.deg, u.deg))
    except ValueError:
        target = SkyCoord(
            params['target_ra'],
            params['target_dec'],
            frame='icrs',
            unit=(u.hourangle, u.deg)
        )
    target_ra = target.ra.deg
    target_dec = target.dec.deg
    lcologs.log(
        'Searching for photometry of target at RA=' + str(target_ra) + ', Dec=' + str(target_dec) + 'deg',
        'info',
        log=log
    )

    # Search the catalog for the nearest entry
    star_idx, entry = crossmatching.find_nearest(
        star_catalog.sources, target_ra, target_dec, log=log
    )

    # If a valid entry exists, extract the lightcurve and output
    if entry:
        lc, tom_lc = get_lightcurve(
            obs_set, dataset.flux, dataset.flux_err, star_idx, params['filter'], log=log
        )

        if lc:
            lc.write(params['lc_path']+'.dat', format='ascii', overwrite=True)

            tom_lc.write(params['lc_path']+'.csv', format='csv', overwrite=True)

            lcologs.log('Output lightcurve data to ' + params['lc_path'], 'info', log=log)
            success = True
        else:
            success = False
    else:
        lcologs.log('No matching star found in source catalog', 'warning', log=log)
        success = False

    return success

def get_lightcurve(obs_set, flux, flux_err, star_idx, filter, log=None):
    """

    Parameters
    ----------
    obs_set ObservationSet  Information on all images in the dataset
    flux    arr     Timeseries flux array
    flux_err arr    Uncertainties on flux array
    star_idx int    Index (not ID) of star in source catalog

    Returns
    -------
    lc  Table  Lightcurve data for the star with columns MJD, flux, err_flux, mag, err_mag
    """

    # Extract the star's photometry from the array:
    star_flux = flux[star_idx, :]
    star_flux_err = flux_err[star_idx, :]

    # Check for valid flux and err_flux measurements for this star's lightcurve
    valid_flux1 = ~np.isnan(star_flux)
    valid_flux2 = (star_flux > 0.0)
    valid_flux = np.logical_and(valid_flux1, valid_flux2)
    valid_err_flux1 = ~np.isnan(star_flux_err)
    valid = np.logical_and(valid_flux, valid_err_flux1)

    if valid.any():

        # Convert to magnitudes for convenience
        mag, err_mag, _, _ = conversions.flux_to_mag(star_flux[valid], star_flux_err[valid])

        # Filter out data points with excessive error bars from the TOM-compatible
        # CSV output, since they distort the plot axes.  Retain the points in the DAT output.
        valid_mag_err = (err_mag < 0.5)

        # Dat format lightcurve for interactive inspection
        lc = Table([
            Column(name='HJD', data=obs_set.table['HJD'][valid]),
            Column(name='flux', data=star_flux[valid]),
            Column(name='err_flux', data=star_flux_err[valid]),
            Column(name='mag', data=mag),
            Column(name='err_mag', data=err_mag),
            Column(name='file', data=obs_set.table['file'][valid])
        ])

        # TOM-compatible format lightcurve
        nvalid2 = valid_mag_err.sum()
        tom_lc = Table([
            Column(name='time', data=obs_set.table['HJD'][valid_mag_err]),
            Column(name='filter', data=np.array([filter] * nvalid2)),
            Column(name='magnitude', data=mag[valid_mag_err]),
            Column(name='error', data=err_mag[valid_mag_err]),
        ])

        lcologs.log('Returned DAT lightcurve with ' + str(valid.sum()) + ' valid datapoints', 'info', log=log)
        lcologs.log(
        'Returned CSV lightcurve with ' + str(nvalid2) + ' valid datapoints, after data with large errors excluded',
        'info', log=log
        )

        return lc, tom_lc

    # Handle case of no valid measurements
    else:
        lcologs.log('No valid measurements in lightcurve', 'warning', log=log)

        return None, None

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('in_path', help='Path to aperture photometry HDF5 file')
    parser.add_argument('target_ra', help='RA of target star in degrees')
    parser.add_argument('target_dec', help='Dec of target star in degrees')
    parser.add_argument('filter', help='Filter used for observations')
    parser.add_argument('out_path', help='Path to output lightcurve file [no file suffix]')
    args = parser.parse_args()

    # Decant the information into a dictionary to allow for easier integration with the pipeline
    params = {
        'phot_file': args.in_path,
        'target_ra': args.target_ra,
        'target_dec': args.target_dec,
        'filter': args.filter,
        'lc_path': args.out_path
    }

    return params

if __name__ == '__main__':
    params = get_args()
    status = aperture_timeseries(params)
