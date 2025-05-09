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


parser = argparse.ArgumentParser()
parser.add_argument('directory', help='Path to data directory of FITS images')
args = parser.parse_args()

#directory = '/media/bachelet/Data/Work/Microlensing/OMEGA/Photometry/OB20240034/ip/data/'

ra= 266.04333333
dec=-39.11322222

from astropy.coordinates import SkyCoord
coo = SkyCoord(ra=ra,dec=dec,unit='deg')

target =  SkyCoord(ra=ra, dec=dec, unit=(u.degree, u.degree), frame='icrs')


gaia_catalog = GC.collect_Gaia_catalog(ra,dec,20,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path='./')
coords = SkyCoord(ra=gaia_catalog['ra'].data, dec=gaia_catalog['dec'].data, unit=(u.degree, u.degree), frame='icrs')
 
images = [i for i in os.listdir(args.directory) if ('.fits' in i) & ('.fz' not in i)]

cutout_region = [ra-0/60.,dec-0/60.,250]


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

for im in tqdm(images[::10]):#[::1]:
    image_path = os.path.join(args.directory,im)

    with fits.open(image_path) as hdul:

        # Q: Not sure why this is limited to one filter, likely for testing

        if hdul[0].header['FILTER'] == 'ip':
            #image = fits.open(directory+im)
            #ims.append(hdul[0].data)
            #errors.append(hdul[3].data)
            nstars.append(len(hdul[1].data))
            l1fwhm.append(copy.deepcopy(hdul[0].header['L1FWHM']))

            # Attempt to retrieve header information and the aperture photometry
            # catalog from a previous run.  If this is not present, perform
            # aperture photometry on the image
            try:
                new_wcs.append(copy.deepcopy(WCS(hdul[-2].header)))
                Time.append(copy.deepcopy(hdul[0].header['MJD-OBS']))
                exptime.append(copy.deepcopy(hdul[0].header['EXPTIME']))
                cats.append(copy.deepcopy(np.array(hdul[-1].data.tolist())))
                print(cats[-1])
            except:

                agent = lcoapphot.AperturePhotometryAnalyst(im, args.directory, gaia_catalog)

                new_wcs.append(copy.deepcopy(agent.image_new_wcs))
                Time.append(copy.deepcopy(agent.image_layers[0].header['MJD-OBS']))
                exptime.append(copy.deepcopy(agent.image_layers[0].header['EXPTIME']))
                cats.append(copy.deepcopy(np.array(agent.image_layers[-1].data.tolist())))
                print(cats[-1])
        else:
            pass

        del hdul[0].data
        hdul.close()
        del hdul
        gc.collect()

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

np.save('ap_phot.npy',np.c_[flux,err_flux])

breakpoint()

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
