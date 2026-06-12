from prefect import flow
import astropy.units as u
import os
from astropy.coordinates import SkyCoord
from astropy.io import fits
import argparse
import numpy as  np
import yaml

import image_reduction.infrastructure.observations as lcoobs
import image_reduction.infrastructure.logs as lcologs
import image_reduction.photometry.aperture_photometry as lcoapphot
import image_reduction.photometry.photometric_scale_factor as lcopscale
from image_reduction.IO import parquet, lightcurve, tom_utils
from image_reduction.infrastructure.data_classes import StarCatalog

@flow
def reduce_dataset(args):
    """
    Pipeline to run an aperture photometry reduction for a single dataset.

    Parameters
    ----------
    args    Object      Parameters of the dataset to be reduced

    Returns
    -------
    None
    """

    ### SET-UP
    # Start logging
    log = lcologs.start_log(args.directory, 'aperture_pipeline')

    # Load reduction configuration
    config_file = os.path.join(args.directory, 'reduction_config.yaml')
    config = yaml.safe_load(open(config_file))

    # Timeseries photometry, source catalog and other results are stored in parquet-format subdirectories.
    parquet.make_output_directories(args.directory, log=log)

    # Get observation set; this provides the list of images and associated information
    obs_set = lcoobs.get_observation_metadata.fn(args, log=log)

    # Establish label for the dataset
    config['tom']['data_label'] = config['tom']['data_label'] + '_' + obs_set.table['filter'][0]

    # Use the header of the first image in the directory to
    # identify the expected target coordinates, assuming
    # that the frame is centered on the target
    field_ra = obs_set.table['RA'][0]
    field_dec: object = obs_set.table['Dec'][0]
    if ':' in str(field_ra):
        target = SkyCoord(ra=field_ra, dec=field_dec, unit=(u.hourangle, u.degree), frame='icrs')
    else:
        target = SkyCoord(ra=field_ra, dec=field_dec, unit=(u.degree, u.degree), frame='icrs')

    lcologs.log(
        'Extracting Gaia targets for field centered on ' + repr(field_ra) + ', ' + repr(field_dec),
        'info',
        log=log
    )

    # Use the first image in the dataset as the reference image, unless one is specified
    # in the reduction config
    if 'reference_image' in config['photometry'].keys():
        reference_image_name = config['photometry']['reference_image']
    else:
        reference_image_name = obs_set.table['file'][0]
    lcologs.log('Reference image is ' + reference_image_name,'info', log=log)

    ### STAR CATALOG
    # Load a pre-existing star catalog if one is available.  If it is, it will include known
    # Gaia objects; otherwise returns None.
    star_catalog_path = os.path.join(args.directory, '..', 'star_catalog.fits')
    star_catalog = StarCatalog(file_path=star_catalog_path, log=log)

    # If no star catalog is available, create one starting with known Gaia objects
    if not star_catalog.sources:
        star_catalog.create_from_Gaia_catalog(args, target, log=log)

        ### REFERENCE IMAGE
        # Perform object detection and astrometry on the reference image, and extend the star catalog
        # to include uncatalogued objects
        agent = lcoapphot.AperturePhotometryAnalyst(
            reference_image_name, args.directory, star_catalog, obs_set, config, log=log
        )
        star_catalog = agent.run_image_astrometry(star_catalog, log)
        star_catalog.combine_source_catalogs(
            agent.image_new_wcs, agent.image_source_catalog, agent.dir_path, log
        )
        lcologs.log('Completed star catalog with objects detected in the reference image', 'info', log=log)

    ### TIME SERIES PHOTOMETRY
    # Loop over all images
    # Perform astrometry and photometer at all (transformed) locations in the star catalog
    # unless photometry already exists, or is required
    for i,im in enumerate(obs_set.table['file']):
        image_path = os.path.join(args.directory,im)

        if obs_set.table['processed'][i] == 0 or args.update_phot:
            lcologs.log('Aperture photometry for ' + im + ', ' \
                        + str(i + 1) + ' out of ' + str(len(obs_set.table['file'])),
                        'info', log=log)

            with fits.open(image_path) as hdul:

                # Perform astrometry on the image
                agent = lcoapphot.AperturePhotometryAnalyst(im, args.directory, star_catalog, obs_set, config, log=log)
                star_catalog = agent.run_image_astrometry(star_catalog, log)
                hdul = agent.store_new_wcs_in_image(hdul, log)

                # If astrometry was successful, we can photometer the image
                if agent.status == 'OK':
                    agent.run_image_photometry(log, debug=True)
                    agent.store_photometry(args.directory, log)
                    lcologs.log(' -> Performed aperture photometry', 'info', log=log)
                else:
                    agent.sources['aperture_sum'] = np.array([np.nan]*len(star_catalog.sources))
                    agent.sources['aperture_sum_err'] = np.array([np.nan]*len(star_catalog.sources))
                    agent.store_photometry(args.directory, log)
                    lcologs.log(' -> WARNING: No photometry possible', 'info', log=log)

                # Store the updated HDUlist and close
                hdul.writeto(image_path, overwrite=True)
                hdul.close()

                # Update processed status in obs_set
                obs_set.table['processed'][i] = 1
            del hdul

        else:
            lcologs.log('Photometry exists for ' + im + ', ' \
                        + str(i + 1) + ' out of ' + str(len(obs_set.table['file'])),
                        'info', log=log)

    lcologs.log('Completed photometry stage for all images; loading whole dataset', 'info', log=log)

    # Save updated results from newly processed images
    obs_set_file = os.path.join(args.directory, 'data_summary.txt')
    obs_set.save(obs_set_file, log=log)

    # Load timeseries photometry for all images
    dataset = lcoapphot.AperturePhotometryDataset()
    dataset.load_phot_store(args.directory, len(star_catalog.sources), obs_set)

    ### Photometric Correction
    # Calculate the photometric scale factor and use it to compute corrected lightcurves.
    if len(obs_set.table) > 0:
        dataset = lcopscale.calculate_pscale(
            reference_image_name, obs_set, dataset, log=log
        )

        # Output normalized timeseries photometry for the whole frame
        parquet.output_norm_flux(args.directory, dataset)

        # Output the lightcurve of the object closest to the center of the field of view
        phot_file_path = os.path.join(args.directory, 'aperture_photometry.parquet')
        lc_root_file_name = config['target']['name'] + '_' + obs_set.table['filter'][0] + '_lc'
        params = {
            'phot_file': phot_file_path,
            'target_ra': config['target']['RA'],
            'target_dec': config['target']['Dec'],
            'filter': config['tom']['data_label'],
            'lc_path': os.path.join(args.directory, lc_root_file_name)
        }
        lc_status = lightcurve.aperture_timeseries.fn(
            params, star_catalog, obs_set, dataset, log=log
        )

        # TOM lightcurve upload
        if config['tom']['upload'] and lc_status:
            params = {
                'file_path': os.path.join(args.directory, lc_root_file_name + '.csv'),
                'data_label': config['tom']['data_label'],
                'target_name': config['target']['name'],
                'tom_config_file': config['tom']['config_file']
            }
            tom_utils.upload_lightcurve.fn(params, log=log)

        elif not lc_status:
            lcologs.log(
                'No lightcurve for TOM upload',
                'warning',
                log=log
            )

        elif not config['tom']['upload']:
            lcologs.log(
                'TOM upload switched OFF in configuration',
                'warning',
                log=log
            )
    else:
        lcologs.log(
            'Empty observations table, cannot calculate photometry timeseries',
            'error',
            log=log
        )

    # Wrap up
    log.info('Aperture photometry reduction completed')
    lcologs.close_log(log)


def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('directory', help='Path to data directory of FITS images')
    parser.add_argument('--update_phot', help='Force re-photometry of all frames', default=False, action='store_true')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()
    print(args)
    reduce_dataset(args)
