from prefect import task
from image_reduction.photometry import aperture_photometry
from image_reduction.infrastructure import logs as lcologs
import argparse
import numpy as np
from os import path
from image_reduction.infrastructure.data_classes import StarCatalog
import image_reduction.infrastructure.observations as lcoobs
import image_reduction.photometry.aperture_photometry as lcoapphot
from image_reduction.IO import lightcurve

def find_nearest_star_by_pixel(star_catalog, target_x, target_y, radius=5.0, log=None):
    """
    Function to identify the star nearest to the pixel position given in the reference image

    Parameters
    ----------
    star_catalog    Star catalog object
    target_x        float Target x-pixel position
    target_y        float Target y-pixel position
    radius          [optional] float Selection radius in pixels, default=5
    log             [optional] Logger object

    Outputs
    -------
    star_idx int    Index of star within the catalog
    match_position  Tuple of the (x,y) coordinates of the nearest matching object in the
                    first image of the set
    """

    # The first image in a dataset is normally used as the reference.  Use this to
    # calculate the separation of all objects from the target location
    separations = np.sqrt(
        #(positions[:,image_index,0] - target_x)**2 + (positions[:,image_index,1] - target_y)**2
        (star_catalog.sources['x'] - target_x)**2 + (star_catalog.sources['y'] - target_y)**2
    )

    # Find the closest match within the search radius
    idx = np.argsort(separations)

    if separations[idx[0]] <= radius:
        star_idx = idx[0]
        match_position = (star_catalog.sources['x'][idx[0]], star_catalog.sources['y'][idx[0]])
        lcologs.log(
            'Nearest matching star is =' + str(star_idx) + ' at ' + repr(match_position) + 'pix',
            'info',
            log=log
        )
        return star_idx, match_position

    else:
        lcologs.log(
            'Nearest star =' + str(idx[0]) + ' which lies at a separation of ' + str(round(separations[idx[0]],3)) + 'pix',
            'info',
            log=log
        )
        lcologs.log(
            'No matching star found within search radius=' + str(radius) + ' pix',
            'warning',
            log=log
        )
        return -1, None
@task
def aperture_timeseries_on_pixel(params, log=None):
    """
    Function to plot an aperture photometry timeseries from an HDF5 output file based on a targets
    pixel position

    Parameters
    ----------
    params    dict      Program arguments:
        'phot_file', 'x', 'y', 'filter', 'lc_path'

    Outputs
    -------
    ASCII format lightcurve file without suffix (path with root filename only)
    """

    log = lcologs.start_log(params['red_dir'], 'lc_by_pix')

    # Load the photometry dataset
    star_catalog_path = path.join(params['red_dir'], '..', 'star_catalog.fits')
    star_catalog = StarCatalog(file_path=star_catalog_path, log=log)

    obs_set = lcoobs.get_observation_metadata.fn(params['red_dir'], log=log)

    dataset = lcoapphot.AperturePhotometryDataset()
    dataset.load_norm_flux(params['red_dir'])

    # Target coordinates need to be in pixel coordinates
    target_x = float(params['target_x'])
    target_y = float(params['target_y'])

    lcologs.log(
        'Searching for photometry of target at x=' + str(target_x) + ', y=' + str(target_y) + 'pix',
        'info',
        log=log
    )

    # Search the catalog for the nearest entry
    star_idx, entry = find_nearest_star_by_pixel(
        star_catalog,
        target_x,
        target_y,
        radius=params['radius'],
        log=log
    )

    # If a valid entry exists, extract the lightcurve and output
    if star_idx >= 0:
        lc, tom_lc = lightcurve.get_lightcurve(
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

    lcologs.close_log(log)

    return success

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('red_dir', help='Path to reduction directory')
    parser.add_argument('target_x', help='X-position of target star in pixels')
    parser.add_argument('target_y', help='Y-position of target star in pixels')
    parser.add_argument('radius', help='Search radius in pixels')
    parser.add_argument('filter', help='Filter used for observations')
    parser.add_argument('out_path', help='Path to output lightcurve file [no file suffix]')
    args = parser.parse_args()

    # Decant the information into a dictionary to allow for easier integration with the pipeline
    params = {
        'red_dir': args.red_dir,
        'target_x': float(args.target_x),
        'target_y': float(args.target_y),
        'radius': float(args.radius),
        'filter': args.filter,
        'lc_path': args.out_path
    }

    return params


if __name__ == '__main__':
    params = get_args()
    status = aperture_timeseries_on_pixel(params)
