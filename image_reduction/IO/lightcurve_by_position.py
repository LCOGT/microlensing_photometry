from prefect import task
from image_reduction.photometry import aperture_photometry
from image_reduction.infrastructure import logs as lcologs
import argparse
import numpy as np
from os import path


def find_nearest_star_by_pixel(positions, target_x, target_y, radius=5.0, image_index=0, log=None):
    """
    Function to identify the star nearest to the pixel position given in the reference image

    Parameters
    ----------
    dataset.positions   HDF array with positions of detected sources in all images
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
        (positions[:,image_index,0] - target_x)**2 + (positions[:,image_index,1] - target_y)**2
    )

    # Find the closest match within the search radius
    idx = np.argsort(separations)

    if separations[idx[0]] <= radius:
        star_idx = idx[0]
        match_position = (positions[idx[0],image_index,0], positions[idx[0],image_index,1])
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
        return None, None
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

    log = lcologs.start_log(path.dirname(params['phot_file']), 'lc_by_pix')

    # Load the photometry dataset
    dataset = aperture_photometry.AperturePhotometryDataset(file_path=params['phot_file'])

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
        dataset.positions,
        target_x,
        target_y,
        radius=params['radius'],
        image_index=params['image_index'],
        log=log
    )

    # If a valid entry exists, extract the lightcurve and output
    if star_idx:
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

    lcologs.close_log(log)

    return success

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('in_path', help='Path to aperture photometry HDF5 file')
    parser.add_argument('target_x', help='X-position of target star in pixels')
    parser.add_argument('target_y', help='Y-position of target star in pixels')
    parser.add_argument('radius', help='Search radius in pixels')
    parser.add_argument('image_index', help='Index of image to use as match reference in HDF file')
    parser.add_argument('filter', help='Filter used for observations')
    parser.add_argument('out_path', help='Path to output lightcurve file [no file suffix]')
    args = parser.parse_args()

    # Decant the information into a dictionary to allow for easier integration with the pipeline
    params = {
        'phot_file': args.in_path,
        'target_x': float(args.target_x),
        'target_y': float(args.target_y),
        'radius': float(args.radius),
        'image_index': int(args.image_index),
        'filter': args.filter,
        'lc_path': args.out_path
    }

    return params


if __name__ == '__main__':
    params = get_args()
    status = aperture_timeseries_on_pixel(params)
