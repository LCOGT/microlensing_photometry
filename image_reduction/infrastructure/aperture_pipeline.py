from prefect import flow, task
import astropy.units as u
import os
from astropy.coordinates import SkyCoord
from astropy.io import fits
import argparse
import copy
import yaml

import image_reduction.infrastructure.observations as lcoobs
import image_reduction.infrastructure.logs as lcologs
import image_reduction.photometry.aperture_photometry as lcoapphot
import image_reduction.photometry.photometric_scale_factor as lcopscale
from image_reduction.IO import fits_table_parser, hdf5, lightcurve, tom_utils
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

    # Start logging
    log = lcologs.start_log(args.directory, 'aperture_pipeline')

    # Load reduction configuration
    config_file = os.path.join(args.directory, 'reduction_config.yaml')
    config = yaml.safe_load(open(config_file))

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

    # Load a pre-existing star catalog if one is available.  If it is, it will include known
    # Gaia objects; otherwise returns None.
    star_catalog_path = os.path.join(args.directory, '..', 'star_catalog.fits')
    star_catalog = StarCatalog(file_path=star_catalog_path, log=log)

    # If no star catalog is available, create one starting with known Gaia objects
    if not star_catalog.sources:
        star_catalog.create_from_Gaia_catalog(args, target, log=log)

    phot_catalogs = {}   # List of photometry catalogs for all images

    for im in obs_set.table['file']:
        image_path = os.path.join(args.directory,im)
        lcologs.log('Aperture photometry for ' + im, 'info', log=log)

        with fits.open(image_path) as hdul:

            # Check whether there is an existing photometry table available.
            phot_table_index = fits_table_parser.find_phot_table(
                hdul, 'LCO MICROLENSING APERTURE PHOTOMETRY')

            # If photometry has already been done, read the table
            if not args.update_phot and phot_table_index >= 0:
                phot_catalogs[im] = copy.deepcopy(fits_table_parser.fits_rec_to_table(hdul[phot_table_index]))
                lcologs.log(' -> Loaded existing photometry catalog', 'info', log=log)

            # If no photometry table is available, perform photometry.
            # If the star catalog is incomplete and the astrometric fit is successful, this
            # extends (and completes) the sources table.
            else:
                agent = lcoapphot.AperturePhotometryAnalyst(im, args.directory, star_catalog, config, log=log)
                star_catalog = agent.run_image_astrometry(star_catalog, log)

                # If astrometry was successful, we can photometer the image
                if agent.status == 'OK':
                    agent.run_image_photometry(log)
                    phot_catalogs[im] = copy.deepcopy(agent.sources)

                    lcologs.log(' -> Performed aperture photometry', 'info', log=log)
                else:
                    phot_catalogs[im] = None
                    lcologs.log(' -> WARNING: No photometry possible', 'info', log=log)

            hdul.close()
            del hdul
    lcologs.log('Photometered all images', 'info', log=log)

    # Calculate the photometric scale factor and use it to compute corrected lightcurves.
    if len(obs_set.table) > 0:
        pscales, epscales, flux, err_flux, raw_flux, raw_err_flux = lcopscale.calculate_pscale(obs_set, phot_catalogs, log=log)

        # Output timeseries photometry for the whole frame
        phot_file_path = os.path.join(args.directory, 'aperture_photometry.hdf5')
        hdf5.output_photometry(
            star_catalog,
            obs_set,
            flux,
            err_flux,
            raw_flux,
            raw_err_flux,
            pscales,
            epscales,
            phot_file_path,
            log=log
        )

        # Output the lightcurve of the object closest to the center of the field of view
        lc_root_file_name = config['target']['name'] + '_' + obs_set.table['filter'][0] + '_lc'
        params = {
            'phot_file': phot_file_path,
            'target_ra': config['target']['RA'],
            'target_dec': config['target']['Dec'],
            'filter': config['tom']['data_label'],
            'lc_path': os.path.join(args.directory, lc_root_file_name)
        }
        lc_status = lightcurve.aperture_timeseries.fn(params, log=log)

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
    reduce_dataset(args)
