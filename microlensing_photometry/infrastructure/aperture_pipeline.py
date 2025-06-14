import copy
import astropy.units as u
import numpy as np
import os
from astropy.coordinates import SkyCoord
from astropy.io import fits
import argparse
from tqdm import tqdm
import copy
import yaml

import microlensing_photometry.infrastructure.observations as lcoobs
import microlensing_photometry.infrastructure.logs as lcologs
import microlensing_photometry.photometry.aperture_photometry as lcoapphot
import microlensing_photometry.photometry.photometric_scale_factor as lcopscale
import microlensing_photometry.logistics.GaiaTools.GaiaCatalog as GC
from microlensing_photometry.IO import fits_table_parser, hdf5, lightcurve

def run(args):
    """
    Pipeline to run an aperture photometry reduction for a single dataset.
    :param args:
    :return:
    """

    # Start logging
    log = lcologs.start_log(args.directory, 'aperture_pipeline')

    # Load reduction configuration
    config_file = os.path.join(args.directory, 'reduction_config.yaml')
    config = yaml.safe_load(open(config_file))

    # Get observation set; this provides the list of images and associated information
    obs_set = lcoobs.get_observation_metadata(args, log=log)

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

    # Load or query for known Gaia objects within this field
    gaia_catalog = GC.collect_Gaia_catalog(
        target.ra.deg,
        target.dec.deg,
        20,
        row_limit = 10000,
        catalog_name='Gaia_catalog.dat',
        catalog_path=args.directory,
        log=log
    )

    if not gaia_catalog:
        lcologs.lco(
            'No Gaia catalog could be retrieved for this field, either locally or online',
            'error',
            log=log
        )
        lcologs.close_log(log)
        raise IOError('No Gaia catalog could be retrieved for this field, either locally or online')

    coords = SkyCoord(
        ra=gaia_catalog['ra'].data,
        dec=gaia_catalog['dec'].data,
        unit=(u.degree, u.degree),
        frame='icrs'
    )

    cutout_region = [target.ra.deg-0/60.,target.dec.deg-0/60.,250]

    cats = {}   # List of star catalogs for all images
    bad_agent = []
    nstars = {}

    for im in tqdm(obs_set.table['file']):#[::1]:
        image_path = os.path.join(args.directory,im)
        lcologs.log('Aperture photometry for ' + im, 'info', log=log)

        with fits.open(image_path) as hdul:

            hdr0 = copy.deepcopy(hdul[0].header)

            nstars[im] = len(hdul[1].data)

            # Check whether there is an existing photometry table available.
            phot_table_index = fits_table_parser.find_phot_table(
                hdul, 'LCO MICROLENSING APERTURE PHOTOMETRY')

            # If photometry has already been done, read the table
            if phot_table_index >= 0:
                cats[im] = copy.deepcopy(fits_table_parser.fits_rec_to_table(hdul[phot_table_index]))
                lcologs.log(' -> Loaded existing photometry catalog', 'info', log=log)

            # If no photometry table is available, perform photometry:
            else:
                agent = lcoapphot.AperturePhotometryAnalyst(im, args.directory, gaia_catalog)
                if agent.status == 'OK':
                    cats[im] = agent.aperture_photometry_table
                    lcologs.log(' -> Performed aperture photometry', 'info', log=log)
                else:
                    cats[im] = None
                    lcologs.log(' -> WARNING: No photometry possible', 'info', log=log)

            hdul.close()
            del hdul
    lcologs.log('Photometered all images', 'info', log=log)

    if len(obs_set.table) > 0:
        lcologs.log('Computing photometric scale factors', 'info', log=log)

        # Create the aperture lightcurves.  Default empty array is used to fill in
        # the data cube for images where no photometry was possible
        nodata = np.empty(len(gaia_catalog))
        nodata.fill(np.nan)

        apsum = np.array(
            [cats[im]['aperture_sum'] if cats[im] else nodata for im in obs_set.table['file']]
        )
        eapsum = np.array(
            [cats[im]['aperture_sum_err'] if cats[im] else nodata for im in obs_set.table['file']]
        )

        lcs = apsum.T
        elcs = eapsum.T

        #Compute the phot scale based on <950 stars
        pscales = lcopscale.photometric_scale_factor_from_lightcurves(lcs[50:1000])
        epscales = (pscales[2]-pscales[0])/2
        flux = lcs/pscales[1]
        err_flux = (elcs**2/pscales[1]**2+lcs**2*epscales**2/pscales[1]**4)**0.5

        # Output timeseries photometry for the whole frame
        phot_file_path = os.path.join(args.directory, 'aperture_photometry.hdf5')
        hdf5.output_photometry(
            gaia_catalog,
            obs_set,
            flux,
            err_flux,
            pscales,
            epscales,
            phot_file_path,
            log=log
        )

        # Output the lightcurve of the object closest to the center of the field of view
        params = {
            'phot_file': phot_file_path,
            'target_ra': config['target']['RA'],
            'target_dec': config['target']['Dec'],
            'lc_path': os.path.join(args.directory, config['target']['name'] + '_lc.dat')
        }
        lc_status = lightcurve.aperture_timeseries(params, log=log)

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
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()
    run(args)
