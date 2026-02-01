import astropy.units as u
import numpy as np
import os
from astropy.wcs import WCS
from astropy.io import fits

from tqdm import tqdm

import microlensing_photometry.photometry.aperture_photometry as lcoapphot
import microlensing_photometry.photometry.dia_photometry as lcodiaphot
import microlensing_photometry.photometry.photometric_scale_factor as lcopscale
from microlensing_photometry.logistics import GaiaCatalog as GC

directory = '/media/bachelet/Data/Work/Microlensing/OMEGA/Photometry/OB20240034/ip/data/'

ra= 266.04333333
dec=-39.11322222

from astropy.coordinates import SkyCoord
coo = SkyCoord(ra=ra,dec=dec,unit='deg')

target =  SkyCoord(ra=ra, dec=dec, unit=(u.degree, u.degree), frame='icrs')


gaia_catalog = GC.collect_Gaia_catalog(ra,dec,20,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path='./')
coords = SkyCoord(ra=gaia_catalog['ra'].data, dec=gaia_catalog['dec'].data, unit=(u.degree, u.degree), frame='icrs')
 
images = [i for i in os.listdir(directory) if ('.fits' in i) & ('.fz' not in i)]

cutout_region = [ra-0/60.,dec-0/60.,250]


### First run wcs+aperture
new_wcs = []
cats = []
Time = []
exptime = []
bad_agent = []
ims = []
errors = []
counts = []
l1fwhm = []

for im in tqdm(images[:]):#[::1]:

    image = fits.open(directory+im)

    if image[0].header['FILTER'] == 'ip':
        #image = fits.open(directory+im)
        ims.append(image[0].data)
        errors.append(image[3].data)    
        counts.append(len(image[1].data))
        l1fwhm.append(image[0].header['L1FWHM'])
        try:
            if 'LCO' not in image[-2].header['EXTNAME']:
                bb = tgsb
            new_wcs.append(WCS(image[-2].header))
            Time.append(image[0].header['MJD-OBS'])
            exptime.append(image[0].header['EXPTIME'])
            cats.append(image[-1].data)
            
        except:
            
            agent = lcoapphot.AperturePhotometryAnalyst(im,directory,gaia_catalog)

            new_wcs.append(agent.image_new_wcs)
            Time.append(agent.image_layers[0].header['MJD-OBS'])
            exptime.append(agent.image_layers[0].header['EXPTIME'])
            image = fits.open(directory+im)
            cats.append(image[-1].data)
           
        
    else:
        pass
        
    del image[0].data

    image.close()  
   
   
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
ref = np.argmax(counts)

dia_phots = []
dia_kers = []
dia_ekers = []
#breakpoint()
for ind,im in enumerate(tqdm(images[:])):#[::1]:

        agent = lcodiaphot.DIAPhotometryAnalyst( images[ref],directory,images[ind], directory,cats[ref], cats[ind],
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
