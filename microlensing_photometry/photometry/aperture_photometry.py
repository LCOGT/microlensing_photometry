from photutils.aperture import aperture_photometry
from photutils.aperture import CircularAnnulus, CircularAperture
from photutils.aperture import ApertureStats
from astropy.io import fits
from astropy.wcs import WCS
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table, Column
import numpy as np
import sys
import os
import h5py

from microlensing_photometry.astrometry import wcs as lcowcs
from microlensing_photometry.infrastructure import logs as lcologs
from microlensing_photometry.photometry import conversions

class AperturePhotometryAnalyst(object):
    """
    Class of worker to perform WCS refinement and aperture photometry for one image

    Attributes
    ----------

    image_name : str, a name to the data
    image_path : str, a path+name to the data
    gaia_catalog : astropy.Table, the Gaia catalog of the field

    """

    def __init__(self, image_name, image_path, gaia_catalog, log_path='./logs'):

        self.log = lcologs.start_log(log_path, image_name)
        self.log.info('Initialize Aperture Photometry Analyst on '+image_name+' at this location '+image_path)

        self.image_path = os.path.join(image_path, image_name)

        self.status = 'OK'

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
        import time
        start = time.time()
        self.log.info('Start Image Processing')

        self.find_star_catalog()
        print(time.time()-start)
        self.refine_wcs()
        print(time.time()-start)

        if self.status == 'OK':
            self.run_aperture_photometry()
            print(time.time()-start)

            self.save_new_products_in_image()

    def find_star_catalog(self):
        """
        Find the star catalog for an image. It uses BANZAI outputs if available, otherwise compute it.
        """

        if self.image_layers[1].header['EXTNAME']=='CAT':
            #BANZAI image
            self.log.info('Find and use the BANZAI catalog for the entire process')
            self.star_catalog = np.c_[self.image_layers[1].data['x'],
                                      self.image_layers[1].data['y'],
                                      self.image_layers[1].data['flux']]
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
                                       star_limit = 5000)

            self.image_new_wcs = wcs2

            if wcs2:
                self.log.info('WCS successfully updated')
            else:
                self.status = 'ERROR'
                self.log.error('Problems with WCS update: image skipped')
        except Exception as error:
            self.status = 'ERROR'
            self.log.info('Problems with WCS update: aboard Aperture Photometry! Details below')
            self.log.error(f"Aperture Photometry Error: %s, %s" % (error, type(error)))

            #sys.exit()

    def run_aperture_photometry(self):
        """
        Run aperture photometry on the image using the star catalog of Gaia for time been.
        """

        try:

            skycoord = SkyCoord(ra=self.gaia_catalog['ra'], dec=self.gaia_catalog['dec'], unit=(u.degree, u.degree))
            xx, yy = self.image_new_wcs.world_to_pixel(skycoord)
            positions = np.c_[xx,yy]

            #fwhm = self.image_layers[0].header['L1FWHM']

            phot_table = run_aperture_photometry(self.image_data, self.image_errors, positions,5)
            exptime = self.image_layers[0].header['EXPTIME']

            phot_table['aperture_sum'] /= exptime
            phot_table['aperture_sum_err'] /= exptime

            self.aperture_photometry_table = phot_table

            self.log.info('Aperture Photometry successfully estimated')

        except Exception as error:

            self.log.info('Problems with the aperture photometry: aboard Aperture Photometry! Details below')
            self.log.error(f"Aperture Photometry Error: %s, %s" % (error, type(error)))

    def save_new_products_in_image(self):
        """
        Save the new product, corrected WCS and aperture phot table, on the image
        directly
        """

        #Save updated wcs in a new layer
        new_header = self.image_new_wcs.to_header()
        new_header['EXTNAME'] = 'LCO MICROLENSING PHOTOMETRY UPDATED WCS'
        new_wcs_hdu = fits.PrimaryHDU(header=new_header)
        self.image_layers.append(new_wcs_hdu)

        #Save Aperture Photometry  in a new layer
        aperture_hdu =  fits.BinTableHDU(data= self.aperture_photometry_table)
        aperture_hdu.header['EXTNAME'] = 'LCO MICROLENSING APERTURE PHOTOMETRY'
        self.image_layers.append(aperture_hdu)

        #Save updates
        self.image_layers.writeto(self.image_path, overwrite=True)

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

class AperturePhotometryDataset(object):
    """
    Class to store and manipulate the results of the AperturePhotometryAnalyst for a set of multiple images
    """

    def __init__(self, file_path=None):
        """
        Method to instantiate an AperturePhotometryDataset object and optionally load photometry data from
        an HSF5 file.

        Parameters
        ----------
        file_path   str     [optiona] Path to input HDF5 file

        Returns
        -------
        self
        """
        self.source_id = Table([Column(name='ID', data=np.array([]))])
        self.wcs_positions = Table(
            [
                Column(name='ra', data=np.array([]), unit=u.deg),
                Column(name='dec', data=np.array([]), unit=u.deg)
            ]
        )
        self.positions  = Table(
            [
                Column(name='x', data=np.array([]), unit='pixel'),
                Column(name='y', data=np.array([]), unit='pixel')
            ]
        )
        self.timestamps = Table([Column(name='MJD', data=np.array([]))])
        self.exptimes = Table([Column(name='exptime', unit=u.second, data=np.array([]))])
        self.flux = np.array([])
        self.err_flux = np.array([])
        self.pscale = np.array([])
        self.epscale = np.array([])

        if file_path:
            self.load_hdf5(file_path)

    def load_hdf5(self, file_path):
        """
        Method to load
        Parameters
        ----------
        file_path  str     Path to input HDF5 file

        Returns
        -------
        object with attributes populated with photometry from the file
        """

        if not os.path.isfile(file_path):
            raise IOError('Cannot find aperture photometry dataset file at ' + file_path)

        with h5py.File(file_path, 'r') as f:
            self.source_id = Table([Column(name='ID', data=np.array(f['source_id'][:]))])
            self.source_wcs = Table(
                [
                    Column(name='ra', data=np.array(f['source_wcs'][:])[:,0], unit=u.deg),
                    Column(name='dec', data=np.array(f['source_wcs'][:])[:,1], unit=u.deg),
                ]
            )
            self.positions = np.array(f['positions'][:])
            self.timestamps = Table([Column(name='HJD', data=np.array(f['HJD'][:]), unit=u.day)])
            self.flux = np.array(f['flux'])
            self.err_flux = np.array(f['err_flux'])
            self.pscale = np.array(f['pscale'])
            self.epscale = np.array(f['epscale'])

    def get_lightcurve(self, star_idx, filter):
        """

        Parameters
        ----------
        star_idx int    Index (not ID) of star in source catalog

        Returns
        -------
        lc  Table  Lightcurve data for the star with columns MJD, flux, err_flux, mag, err_mag
        """

        # Check for valid flux and err_flux measurements for this star's lightcurve
        flux = self.flux[star_idx, :]
        valid_flux = ~np.isnan(self.flux[star_idx, :])
        valid_err_flux = ~np.isnan(self.err_flux[star_idx, :])
        valid = np.logical_and(valid_flux, valid_err_flux)

        # Convert to magnitudes for convenience
        mag, err_mag, _, _ = conversions.flux_to_mag(
            self.flux[star_idx, valid],
            self.err_flux[star_idx, valid]
        )

        # Dat format lightcurve for interactive inspection
        lc = Table([
            Column(name='HJD', data=self.timestamps['HJD'][valid]),
            Column(name='flux', data=self.flux[star_idx, valid]),
            Column(name='err_flux', data=self.err_flux[star_idx, valid]),
            Column(name='mag', data=mag),
            Column(name='err_mag', data=err_mag),
        ])

        # TOM-compatible format lightcurve
        tom_lc = Table([
            Column(name='time', data=self.timestamps['HJD'][valid]),
            Column(name='filter', data=np.array([filter] * len(valid))),
            Column(name='magnitude', data=mag),
            Column(name='error', data=err_mag),
        ])

        return lc, tom_lc
