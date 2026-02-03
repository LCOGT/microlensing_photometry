from prefect import task
from microlensing_photometry.photometry import aperture_photometry
from microlensing_photometry.infrastructure import logs as lcologs
from microlensing_photometry.logistics import GaiaCatalog as GC
import argparse
from astropy.coordinates import SkyCoord
from astropy import units as u

@task
def aperture_timeseries(params, log=None):
    """
    Function to plot an aperture photometry timeseries from an HDF5 output file

    Parameters
    ----------
    params    dict      Program arguments:
        'phot_file', 'target_ra', 'target_dec', 'filter', 'lc_path'

    Outputs
    -------
    ASCII format lightcurve file without suffix (path with root filename only)
    """

    # Load the photometry dataset
    dataset = aperture_photometry.AperturePhotometryDataset(file_path=params['phot_file'])

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
    star_idx, entry = GC.find_nearest(dataset.source_wcs, target_ra, target_dec, log=log)

    # If a valid entry exists, extract the lightcurve and output
    if entry:
        lc, tom_lc = dataset.get_lightcurve(star_idx, params['filter'], log=log)

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

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('in_path', help='Path to aperture photometry HDF5 file')
    parser.add_argument('target_ra', help='RA of target star in degrees')
    parser.add_argument('target_dec', help='Dec of target star in degrees')
    parser.add_argument('filter', help='Filter used for observations')
    parser.add_argument('out_path', help='Path to output lightcurve file')
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