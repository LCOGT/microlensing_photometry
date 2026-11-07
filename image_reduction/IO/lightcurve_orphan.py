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
from astropy.table import Table, vstack, Column
from astropy.io import ascii

def find_nearest_orphan_star_by_pixel(orphan_data, image_name, target_x, target_y, radius=5.0, log=None):
    """
    Function to identify the star nearest to the pixel position given in the reference image

    Parameters
    ----------
    orphan_data  Table  Orphan flux measurements
    image_name   str    Name of current image
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

    # The data from all images is compined into one table, so identify those entries for
    # the current image
    jdx = np.where(orphan_data['file'] == image_name)[0]

    # The first image in a dataset is normally used as the reference.  Use this to
    # calculate the separation of all objects from the target location
    separations = np.sqrt(
        (orphan_data['x'] - target_x)**2 + (orphan_data['y'] - target_y)**2
    )

    # Find the closest match within the search radius
    idx = np.argsort(separations)

    lcologs.log(image_name + ' target position at ' + str(target_x) + ', ' + str(target_y), 'info', log=log)

    if separations[idx[0]] <= radius and idx[0] in jdx:
        star_idx = orphan_data['id'][idx[0]]
        match_position = (orphan_data['x'][idx[0]], orphan_data['y'][idx[0]])
        lcologs.log(
            'For ' + image_name + ' the nearest matching star is =' + str(star_idx) + ' at ' + repr(match_position) + 'pix',
            'info',
            log=log
        )
        return star_idx, match_position

    else:
        lcologs.log(
            'For ' + image_name + ' the nearest star =' + str(idx[0]) + ' which lies at a separation of ' + str(round(separations[idx[0]],3)) + 'pix',
            'info',
            log=log
        )
        lcologs.log(
            'No matching star found for ' + image_name + ' within search radius=' + str(radius) + ' pix',
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
    obs_set = lcoobs.get_observation_metadata.fn(params['red_dir'], log=log)

    dataset = lcoapphot.AperturePhotometryDataset()
    dataset.load_orphan_data(params['red_dir'])

    # Target coordinates need to be in pixel coordinates
    target_positions = ascii.read(params['positions_file'], format="commented_header", header_start=4, data_start=5)

    # Build lightcurve by finding the closest matching object to the position
    # in each image
    lc = Table([])
    for k, image_name in enumerate(obs_set.table['file']):

        # Fetch the target x,y coordinates in this image
        idx = np.where(target_positions['filename'] == image_name)[0]
        if len(idx) > 0:
            target_x = target_positions['x'][idx][0]
            target_y = target_positions['y'][idx][0]

            # Search the orphan data for this image for the nearest entry
            star_idx, entry = find_nearest_orphan_star_by_pixel(
                dataset.orphan_data,
                image_name,
                target_x,
                target_y,
                radius=params['radius'],
                log=log
            )

            # If a valid entry exists, extract the lightcurve and output
            if star_idx >= 0:
                star_data = Table([
                    Column(name='file', data=[dataset.orphan_data[star_idx]['file']]),
                    Column(name='HJD', data=[obs_set.table['HJD'][k]]),
                    Column(name='id', data=[dataset.orphan_data[star_idx]['id']]),
                    Column(name='x', data=[dataset.orphan_data[star_idx]['x']]),
                    Column(name='y', data=[dataset.orphan_data[star_idx]['y']]),
                    Column(name='flux', data=[dataset.orphan_data[star_idx]['aperture_sum']]),
                    Column(name='flux_err', data=[dataset.orphan_data[star_idx]['aperture_sum_err']]),
                ])
                lc = vstack([lc, star_data])

    if len(lc) > 0:
        lc.write(params['lc_path']+'.dat', format='ascii', overwrite=True)
        lcologs.log('Output lightcurve data to ' + params['lc_path'], 'info', log=log)
        success = True
    else:
        lcologs.log('No matching star found in source catalog', 'warning', log=log)
        success = False

    lcologs.close_log(log)

    return success

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('red_dir', help='Path to reduction directory')
    parser.add_argument('positions_file', help='File of target positions')
    parser.add_argument('radius', help='Search radius in pixels')
    parser.add_argument('filter', help='Filter used for observations')
    parser.add_argument('out_path', help='Path to output lightcurve file [no file suffix]')
    args = parser.parse_args()

    # Decant the information into a dictionary to allow for easier integration with the pipeline
    params = {
        'red_dir': args.red_dir,
        'positions_file': args.positions_file,
        'radius': float(args.radius),
        'filter': args.filter,
        'lc_path': args.out_path,
    }

    return params


if __name__ == '__main__':
    params = get_args()
    status = aperture_timeseries_on_pixel(params)
