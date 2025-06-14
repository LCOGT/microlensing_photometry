import copy
import astropy.units as u
import numpy as np
import matplotlib.pyplot as plt
import os
from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
from astropy.io import fits
import argparse
from tqdm import tqdm
import gc
import copy

import microlensing_photometry.infrastructure.observations as lcoobs
import microlensing_photometry.photometry.aperture_photometry as lcoapphot
import microlensing_photometry.photometry.photometric_scale_factor as lcopscale
import microlensing_photometry.logistics.GaiaTools.GaiaCatalog as GC
from microlensing_photometry.IO import fits_table_parser, hdf5

parser = argparse.ArgumentParser()
parser.add_argument('directory', help='Path to data directory of FITS images')
args = parser.parse_args()

do_dia_phot = False

# Get observation set; this provides the list of images and associated information
obs_set = lcoobs.get_observation_metadata(args)

# Use the header of the first image in the directory to
# identify the expected target coordinates, assuming
# that the frame is centered on the target
field_ra = obs_set.table['RA'][0]
field_dec = obs_set.table['Dec'][0]
if ':' in str(field_ra):
    target = SkyCoord(ra=field_ra, dec=field_dec, unit=(u.hourangle, u.degree), frame='icrs')
else:
    target =  SkyCoord(ra=field_ra, dec=field_dec, unit=(u.degree, u.degree), frame='icrs')
print('Extract Gaia targets for field centered on ' + repr(field_ra) + ', ' + repr(field_dec))

# Load or query for known Gaia objects within this field
gaia_catalog = GC.collect_Gaia_catalog(target.ra.deg, target.dec.deg,20,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path=args.directory)

if not gaia_catalog:
    raise IOError('No Gaia catalog could be retrieved for this field, either locally or online')

coords = SkyCoord(ra=gaia_catalog['ra'].data, dec=gaia_catalog['dec'].data, unit=(u.degree, u.degree), frame='icrs')

cutout_region = [target.ra.deg-0/60.,target.dec.deg-0/60.,250]


### First run wcs+aperture
cats = {}   # List of star catalogs for all images
bad_agent = []
#ims = []
#errors = []
nstars = {}

for im in tqdm(obs_set.table['file']):#[::1]:
    image_path = os.path.join(args.directory,im)
    print('Aperture photometry for ' + im)

    with fits.open(image_path) as hdul:

        hdr0 = copy.deepcopy(hdul[0].header)

        nstars[im] = len(hdul[1].data)

        # Check whether there is an existing photometry table available.
        phot_table_index = fits_table_parser.find_phot_table(hdul, 'LCO MICROLENSING APERTURE PHOTOMETRY')

        # If photometry has already been done, read the table
        if phot_table_index >= 0:
            cats[im] = copy.deepcopy(fits_table_parser.fits_rec_to_table(hdul[phot_table_index]))
            print(' -> Loaded existing photometry catalog')

        # If no photometry table is available, perform photometry:
        else:
            agent = lcoapphot.AperturePhotometryAnalyst(im, args.directory, gaia_catalog)
            if agent.status == 'OK':
                cats[im] = agent.aperture_photometry_table
                print(' -> Performed aperture photometry')
            else:
                cats[im] = None
                print(' -> WARNING: No photometry possible')

        hdul.close()
        del hdul

if len(obs_set.table) > 0:
    #Create the aperture lightcurves

    f(x) if x is not None else ''
    apsum = np.array([cats[i]['aperture_sum'] for i in range(len(obs_set)) if cats[i] else ])
    eapsum = np.array([cats[i]['aperture_sum_err'] for i in range(len(obs_set)) if cats[i]])

    lcs = apsum.T
    elcs = eapsum.T

    #Compute the phot scale based on <950 stars
    pscales = lcopscale.photometric_scale_factor_from_lightcurves(lcs[50:1000])
    epscales = (pscales[2]-pscales[0])/2
    flux = lcs/pscales[1]
    err_flux = (elcs**2/pscales[1]**2+lcs**2*epscales**2/pscales[1]**4)**0.5

    #np.save('ap_phot.npy',np.c_[flux,err_flux])

    file_path = os.path.join(args.directory, 'aperture_photometry.hdf5')
    hdf5.output_photometry(
        gaia_catalog,
        np.array(Time),
        new_wcs,
        flux,
        err_flux,
        np.array(exptime),
        pscales,
        epscales,
        file_path
    )
