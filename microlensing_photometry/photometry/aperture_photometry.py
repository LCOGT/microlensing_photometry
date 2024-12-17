from photutils.aperture import aperture_photometry
from photutils.aperture import CircularAnnulus, CircularAperture
from photutils.aperture import ApertureStats
from astropy.io import fits
from astropy.wcs import WCS
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np
import sys

from microlensing_photometry.astrometry import wcs as lcowcs
from microlensing_photometry.infrastructure import logs as lcologs

class AperturePhotometryAnalyst(object):
    """
    Class of worker to perform WCS refinement and aperture photometry for one image

    Attributes
    ----------

    image_name : str, a name to the data
    image_path : str, a path+name to the data
    gaia_catalog : astropy.Table, the Gaia catalog of the field

    """

    def __init__(self,image_name, image_path,gaia_catalog,log_path='./logs'):

        self.log = lcologs.start_log(log_path, image_name)
        self.log.info('Initialize Aperture Photometry Analyst on '+image_name+' at this location'+image_path)

        self.image_path = image_path+image_name

        try:

            self.image_layers = fits.open(self.image_path)
            self.log.info('Image found and open successfully!')

        except Exception as error:

            self.log.info('Image not found: aboard Aperture Photometry! Details below')
            self.log.error(f"Aperture Photometry Error: %s, %s" % (error, type(error)))

            sys.exit()

        self.gaia_catalog = gaia_catalog

        ### To fix with config files
        self.image_data = self.image_layers[0].data
        self.image_errors = self.image_layers[3].data
        self.image_original_wcs = WCS(self.image_layers[0].header)

        self.process_image()

        lcologs.close_log(self.log)

    def process_image(self):
        """
        Process the image following the various steps
        """

        self.log.info('Start Image Processing')

        self.find_star_catalog()
        self.refine_wcs()
        self.run_aperture_photometry()

    def find_star_catalog(self):
        """
        Find the star catalog for an image. It uses BANZAI outputs if available, otherwise compute it.
        """

        if self.image_layers[1].header['EXTNAME']=='CAT':
            #BANZAI image
            self.log.info('Find and use the BANZAI catalog for the entire process')

            self.star_catalog = np.c_[self.image_layers[1].data['x'],
                                      self.image_layers[1].data['y']]
        else:
            ### run starfinder
            pass

    def refine_wcs(self):
        """
        Starting from approximate WCS solution, this function refine the WCS solution with the Gaia catalog.
        """

        try:
            wcs2 = lcowcs.refine_image_wcs(self.image_data , self.star_catalog,
                                       self.image_original_wcs, self.gaia_catalog,
                                       star_limit = 1000)

            self.log.info('WCS successfully updated')

        except Exception as error:

            self.log.info('Problems with WCS update: aboard Aperture Photometry! Details below')
            self.log.error(f"Aperture Photometry Error: %s, %s" % (error, type(error)))

            sys.exit()
        self.image_new_wcs = wcs2

    def run_aperture_photometry(self):
        """
        Run aperture photometry on the image using the star catalog of Gaia for time been.
        """

        try:

            skycoord = SkyCoord(ra=self.gaia_catalog['ra'], dec=self.gaia_catalog['dec'], unit=(u.degree, u.degree))
            xx, yy = self.image_new_wcs.world_to_pixel(skycoord)
            positions = np.c_[xx,yy]

            phot_table = run_aperture_photometry(self.image_data, self.image_errors, positions, 5)
            exptime = self.image_layers[0].header['EXPTIME']

            phot_table['aperture_sum'] /= exptime
            phot_table['aperture_sum_err'] /= exptime

            self.aperture_photometry_table = phot_table

            self.log.info('Aperture Photometry successfully estimated')

        except Exception as error:

            self.log.info('Problems with the aperture photometry: aboard Aperture Photometry! Details below')
            self.log.error(f"Aperture Photometry Error: %s, %s" % (error, type(error)))

def run_aperture_photometry(image, error, positions,radius):
    """
    Aperture photometry on a image, using an error image, and fixed stars positions.

    Parameters
    ----------
    image : array, the image data
    error : array, the error data (2D)
    positions: array, [X,Y] positions of the stars to place aperture
    radius: float, the aperture radius use to extract flux

    Returns
    -------
    phot_table : astropy.Table, the photometric table
    """

    aperture = CircularAperture(positions, r=radius)
    annulus_aperture = CircularAnnulus(positions, r_in=radius+3, r_out=radius+5)

    aperstats = ApertureStats(image, annulus_aperture)

    bkg_mean = aperstats.mean

    phot_table = aperture_photometry(image, aperture, error=error)
    total_bkg = aperture.area * bkg_mean

    phot_table['aperture_sum'] -= total_bkg

    return phot_table