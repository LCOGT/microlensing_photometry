from photutils.aperture import aperture_photometry
from photutils.aperture import CircularAnnulus, CircularAperture
from photutils.aperture import ApertureStats
from astropy.io import fits
from astropy.wcs import WCS
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np

from microlensing_photometry.astrometry import wcs as lcowcs

class AperturePhotometryAnalyst(object):
    """
    Class of worker to perform WCS refinement and aperture photometry for one image

    Attributes
    ----------
    image_path : a path+name to the data
    gaia_catalog : the Gaia catalog of the field


    Returns
    -------
    new_wcs : an updated astropy WCS object
    """
    def __init__(self,image_path,gaia_catalog):

        self.image_path = image_path
        self.image_layers = fits.open(self.image_path)
        self.gaia_catalog = gaia_catalog

        ### To fix with config files
        self.image_data = self.image_layers[0].data
        self.image_errors = self.image_layers[3].data
        self.image_original_wcs = WCS(self.image_layers[0].header)

        self.process_image()

    def process_image(self):

        self.find_star_catalog()
        self.refine_wcs()
        self.run_aperture_photometry()

    def find_star_catalog(self):

        if self.image_layers[1].header['EXTNAME']=='CAT':
            #BANZAI image
            self.star_catalog = np.c_[self.image_layers[1].data['x'],
                                      self.image_layers[1].data['y']]
        else:
            ### run starfinder
            pass

    def refine_wcs(self):

        wcs2 = lcowcs.refine_image_wcs(self.image_data , self.star_catalog,
                                       self.image_original_wcs, self.gaia_catalog,
                                       star_limit = 1000)

        self.image_new_wcs = wcs2

    def run_aperture_photometry(self):

        skycoord = SkyCoord(ra=self.gaia_catalog['ra'], dec=self.gaia_catalog['dec'], unit=(u.degree, u.degree))
        xx, yy = self.image_new_wcs.world_to_pixel(skycoord)
        positions = np.c_[xx,yy]

        phot_table = run_aperture_photometry(self.image_data, self.image_errors, positions, 3)
        exptime = self.image_layers[0].header['EXPTIME']

        phot_table['aperture_sum'] /= exptime
        phot_table['aperture_sum_err'] /= exptime

        self.aperture_photometry_table = phot_table

def run_aperture_photometry(image, error, positions,radius):
    """
    Aperture photometry on a image

    Parameters
    ----------
    image : the image data
    error : the error data (2D)
    positions: [X,Y] positions of the stars to place aperture
    radius: aperture radius

    Returns
    -------
    phot_table : the photometric table
    """
    aperture = CircularAperture(positions, r=radius)
    annulus_aperture = CircularAnnulus(positions, r_in=radius+3, r_out=radius+5)


    aperstats = ApertureStats(image, annulus_aperture)

    bkg_mean = aperstats.mean

    phot_table = aperture_photometry(image, aperture, error=error)
    total_bkg = aperture.area * bkg_mean

    phot_table['aperture_sum'] -= total_bkg

    return phot_table