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

import microlensing_photometry.photometry.aperture_photometry as lcoapphot
import microlensing_photometry.photometry.dia_photometry as lcodiaphot
import microlensing_photometry.photometry.photometric_scale_factor as lcopscale
import microlensing_photometry.logistics.GaiaTools.GaiaCatalog as GC
import microlensing_photometry.astrometry.wcs as lcowcs
import microlensing_photometry.photometry as lcophot
from microlensing_photometry.IO import fits_table_parser, hdf5

parser = argparse.ArgumentParser()
parser.add_argument('directory', help='Path to data directory of FITS images')
args = parser.parse_args()

do_dia_phot = False

#directory = '/media/bachelet/Data/Work/Microlensing/OMEGA/Photometry/OB20240034/ip/data/'

# Build list of images in the reduction directory
images = [i for i in os.listdir(args.directory) if ('.fits' in i) & ('.fz' not in i)]

# Use the header of the first image in the directory to
# identify the expected target coordinates, assuming
# that the frame is centered on the target
hdr = fits.getheader(os.path.join(args.directory,images[0]))
ra = hdr['RA']
dec = hdr['DEC']
print('Extract Gaia targets for field centered on ' + repr(ra) + ', ' + repr(dec))

coo = SkyCoord(ra=ra,dec=dec,unit='deg')
if ':' in str(ra):
    target = SkyCoord(ra=ra, dec=dec, unit=(u.hourangle, u.degree), frame='icrs')
else:
    target =  SkyCoord(ra=ra, dec=dec, unit=(u.degree, u.degree), frame='icrs')

gaia_catalog = GC.collect_Gaia_catalog(target.ra.deg, target.dec.deg,20,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path=args.directory)

if not gaia_catalog:
    raise IOError('No Gaia catalog could be retrieved for this field, either locally or online')

coords = SkyCoord(ra=gaia_catalog['ra'].data, dec=gaia_catalog['dec'].data, unit=(u.degree, u.degree), frame='icrs')

cutout_region = [target.ra.deg-0/60.,target.dec.deg-0/60.,250]


### First run wcs+aperture
new_wcs = []
cats = []   # List of star catalogs for all images
Time = []
exptime = []
bad_agent = []
#ims = []
#errors = []
nstars = []
l1fwhm = []

for im in tqdm(images):#[::1]:
    image_path = os.path.join(args.directory,im)
    print('Aperture photometry for ' + im)

    with fits.open(image_path) as hdul:

        hdr0 = copy.deepcopy(hdul[0].header)

        #image = fits.open(directory+im)
        #ims.append(hdul[0].data)
        #errors.append(hdul[3].data)
        nstars.append(len(hdul[1].data))
        l1fwhm.append(hdr0['L1FWHM'])

        # Check whether there is an existing photometry table available.
        phot_table_index = fits_table_parser.find_phot_table(hdul, 'LCO MICROLENSING APERTURE PHOTOMETRY')
        # If photometry has already been done, read the table
        success = True
        if phot_table_index >= 0:
            cats.append(copy.deepcopy(fits_table_parser.fits_rec_to_table(hdul[phot_table_index])))
            print(' -> Loaded existing photometry catalog')

        # If no photometry table is available, perform photometry:
        else:
            agent = lcoapphot.AperturePhotometryAnalyst(im, args.directory, gaia_catalog)
            if agent.status == 'OK':
                cats.append(agent.aperture_photometry_table)
                print(' -> Performed aperture photometry')
            else:
                success = False

        # Store FITS header information only if photometry is successful
        if success:
            new_wcs.append(copy.deepcopy(WCS(hdul[-2].header)))
            Time.append(hdr0['MJD-OBS'])
            exptime.append(hdr0['EXPTIME'])

        hdul.close()
        del hdul

if len(cats) > 0:
    #Create the aperture lightcurves
    apsum = np.array([cats[i]['aperture_sum'] for i in range(len(Time))])
    eapsum = np.array([cats[i]['aperture_sum_err'] for i in range(len(Time))])

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

breakpoint()
if do_dia_phot:

    #Location of the DIA cutout
    cutout_region = [ra,dec,250]
    kernel_size = 17
    #Choose the ref as the max number of stars detected by BANZAI, could be updated of course
    ref = np.argmax(nstars)

    dia_phots = []
    dia_kers = []
    dia_ekers = []
    #breakpoint()
    for ind,im in enumerate(tqdm(images[:])):#[::1]:

            agent = lcodiaphot.DIAPhotometryAnalyst( images[ref],args.directory,images[ind], args.directory,cats[ref], cats[ind],
                     cutout_region,kernel_size)
            dia_phots.append(agent.dia_photometry)
            dia_kers.append(agent.kernel)
            dia_ekers.append(agent.kernel_errors)

    breakpoint()
    #Create de DIA lightcurves
    ppp = [np.sum(i) for i in dia_kers]

    lcs = []
    elcs = []

    for ind,star in enumerate(tqdm(dia_phots[0]['id'])):
        #if star==17505:
        #    breakpoint()
        cible = np.where(cats[ref]['id'] == star)[0][0]
        phph = [cats[ref]['aperture_sum'][cible]+dia_phots[i][ind]['aperture_sum']/ppp[i]/exptime[ref] for i in range(len(dia_phots))]
        ephph = [np.sqrt(cats[ref]['aperture_sum_err'][cible]**2
            +dia_phots[i][ind]['aperture_sum_err']**2/ppp[i]**2/exptime[ref]**2
            +dia_phots[i][ind]['aperture_sum']**2/ppp[i]**4*np.sum(dia_ekers[i]**2)/exptime[ref]**2) for i in range(len(dia_phots))]

        lcs.append(phph)
        elcs.append(ephph)



    breakpoint()
