from photutils.aperture import aperture_photometry
from photutils.aperture import CircularAnnulus, CircularAperture
from photutils.aperture import ApertureStats
from photutils.detection import DAOStarFinder
from astropy.io import fits
from astropy.wcs import WCS
from astropy.stats import sigma_clipped_stats
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table, Column
import numpy as np
import sys
import os
import h5py
import time
import copy
from image_reduction.astrometry import wcs as lcowcs
from image_reduction.infrastructure import logs as lcologs
from image_reduction.IO import ds9_utils

class AperturePhotometryAnalyst(object):
    """
    Class of worker to perform WCS refinement and aperture photometry for one image

    Attributes
    ----------

    image_name : str, a name to the data
    image_path : str, a path+name to the data
    sources : astropy.Table, the star catalog of the field

    """

    def __init__(self, image_name, image_path, star_catalog, obs_set, config, log=None):

        lcologs.log(
            'Initializing Aperture Photometry Analyst on '+image_name+' at this location '+image_path,
            'info',
            log=log
        )

        self.image_name = image_name
        self.image_path = os.path.join(image_path, image_name)
        self.dir_path = image_path
        self.status = 'OK'
        self.image_layers = []

        try:
            with fits.open(self.image_path) as hdulist:
                self.image_header = hdulist[0].header
                self.image_extensions = [hdu.name for hdu in hdulist]
                self.image_layers = [hdu.data for hdu in hdulist]

            lcologs.log('Image found and open successfully!', 'info', log=log)

        except Exception as error:

            lcologs.log('Image not found: aboard Aperture Photometry! Details below', 'warning', log=log)
            lcologs.log(f"Aperture Photometry Error: %s, %s" % (error, type(error)), 'error', log=log)

            sys.exit()

        self.sources = copy.deepcopy(star_catalog.sources)
        self.catalog_complete = copy.deepcopy(star_catalog.complete)
        self.image_source_catalog = None
        self.ra_center = star_catalog.ra_center
        self.dec_center = star_catalog.dec_center
        self.image_new_wcs = None
        self.get_science_image()
        self.get_image_errors()
        self.image_original_wcs = WCS(self.image_header)
        self.phot_aperture = config['photometry']['aperture_arcsec'] / self.image_header['PIXSCALE']

        idx = obs_set.table['file'].tolist().index(image_name)
        if idx >= 0:
            self.pixscale = obs_set.table['pixscale'][idx]

    def get_science_image(self):
        """Method to identify and extract the science image, otherwise raise an error"""

        if 'SCI' in self.image_extensions:
            idx = self.image_extensions.index('SCI')
            self.image_data = self.image_layers[idx]
        else:
            raise IOError('Image ' + self.image_name + ' has no science image extension')

    def get_image_errors(self):
        """
        Method to identify the image uncertainties array, if present, otherwise store an array
        of the size of the image with zero pixel entries.
        """

        if 'ERR' in self.image_extensions:
            idx = self.image_extensions.index('ERR')
            self.image_errors = self.image_layers[idx]
        else:
            self.image_errors = np.zeros((self.image_header['NAXIS1'],self.image_header['NAXIS2']))

    def run_image_astrometry(self, star_catalog, log):
        """
        Process the image following the various steps

        :param star_catalog: StarCatalog
        """

        start = time.time()
        lcologs.log('Start image astrometry', 'info', log=log)

        lcologs.log('Original image WCS: ' + repr(self.image_original_wcs), 'info', log=log)

        # Detect objects within the working frame
        self.starfind(log)

        # Refine the image WCS
        lcologs.log(repr(time.time()-start), 'info', log=log)
        self.refine_wcs(log)
        lcologs.log(repr(time.time()-start), 'info', log=log)

        if self.status == 'OK':
            self.sources = copy.deepcopy(star_catalog.sources)

        return star_catalog

    def refine_wcs(self, log):
        """
        Starting from approximate WCS solution, this function refine the WCS solution with the Gaia catalog.
        """

        try:
            wcs2 = lcowcs.refine_image_wcs(self, star_limit=15000, log=log, debug=True)

            self.image_new_wcs = wcs2

            if wcs2:
                lcologs.log('WCS successfully updated', 'info', log=log)
            else:
                self.status = 'ERROR'
                lcologs.log('Problems with WCS update: image skipped', 'warning', log=log)
        except Exception as error:
            self.status = 'ERROR'
            lcologs.log('Problems with WCS update: abort Aperture Photometry! Details below', 'warning', log=log)
            lcologs.log(
                f"Aperture Photometry Error: %s, %s" % (error, type(error)),
                'error',
                log=log
            )

    def starfind(self, log, debug=False):
        """
        Method to perform an object detection on the image and ensure all detected objects
        are included in the star catalog
        """

        lcologs.log(
            'Running starfinder',
            'info',
            log=log
        )

        # Compute statistics of the image background to set detection thresholds
        mean, median, std = sigma_clipped_stats(self.image_data, sigma=3.0, maxiters=5)
        lcologs.log(
            'Image statistics, mean median, std: ' + str(mean) + ', ' + str(median) + ', ' + str(std),
            'info',
            log=log
        )

        # Run daofind algorithm to detect objects
        daofind = DAOStarFinder(fwhm=3.0, threshold=2. * std)
        sources = daofind(self.image_data - median)
        self.image_source_catalog = np.c_[sources['xcentroid'], sources['ycentroid'], sources['peak']]
        lcologs.log(
            'Detected ' + str(len(self.image_source_catalog)) + ' objects in the current frame',
            'info',
            log=log
        )

    def run_image_photometry(self, log):
        """
        Run aperture photometry on the image using the star catalog of Gaia for time been.
        """

        start = time.time()
        lcologs.log('Start image photometry', 'info', log=log)

        try:
            lcologs.log(
                'Performing photometry with aperture ' + str(self.phot_aperture) + ' pix',
                'info', log=log
            )

            # Photometer at the known positions of the combined catalog objects
            positions = np.column_stack([self.sources['x'], self.sources['y']])

            phot_table = run_aperture_photometry(self.image_data, self.image_errors, positions, self.phot_aperture)
            exptime = self.image_header['EXPTIME']

            self.sources['aperture_sum'] = phot_table['aperture_sum'] / exptime
            self.sources['aperture_sum_err'] = phot_table['aperture_sum_err'] / exptime

            lcologs.log('Aperture Photometry successfully completed', 'info', log=log)

        except Exception as error:

            lcologs.log(
                'Problems with the aperture photometry: aboard Aperture Photometry! Details below',
                'warning', log=log
            )
            lcologs.log(f"Aperture Photometry Error: %s, %s" % (error, type(error)), 'error', log=log)

        lcologs.log(repr(time.time() - start), 'info', log=log)

    def find_image_layer(self, hdulist, layer_name):
        """
        Method to find an existing FITS extension in a HDUList by name if available
        :return:
        layer_idx int  Index of the layer in the HDUList or -1 if not present
        """

        layer_idx = -1
        for i, im_layer in enumerate(hdulist):
            if im_layer.header['EXTNAME'] == layer_name:
                layer_idx = i

        return layer_idx

    def update_or_append_fits_layer(self, hdulist, layer_name, new_hdu):
        """
        Method to update an existing layer of a FITS file, if one is identified by its label,
        or otherwise append a new layer

        Params
        ------
        layer_name  str  Label of layer in FITS file
        new_hdu     HDU  New or updated layer content
        """

        layer_idx = self.find_image_layer(hdulist, layer_name)

        if layer_idx == -1:
            hdulist.append(new_hdu)
        else:
            hdulist[layer_idx] = new_hdu

        return hdulist

    def store_new_wcs_in_image(self, hdulist, log):
        """
        Method to store the revised WCS as a FITS table extention to the original image
        """
        if self.image_new_wcs:
            # Save updated wcs in a new layer or update an existing table extension if available
            layer_name = 'LCO MICROLENSING PHOTOMETRY UPDATED WCS'
            new_header = self.image_new_wcs.to_header()
            new_header['EXTNAME'] = layer_name
            new_wcs_hdu = fits.ImageHDU(header=new_header)
            hdulist = self.update_or_append_fits_layer(hdulist, layer_name, new_wcs_hdu)

            lcologs.log('Stored updated WCS in ' + self.image_path, 'info', log=log)
        else:
            lcologs.log('No new WCS to store for ' + self.image_path, 'info', log=log)

        return hdulist

    def store_photometry_in_image(self, hdulist, log):
        """
        Save the new photometry table, corrected WCS and aperture phot table, on the image
        directly
        """

        #Save Aperture Photometry  in a new layer or update an existing table extension if available
        layer_name = 'LCO MICROLENSING APERTURE PHOTOMETRY'
        aperture_hdu =  fits.BinTableHDU(data= self.sources)
        aperture_hdu.header['EXTNAME'] = layer_name
        aperture_hdu.header['APRAD'] = self.phot_aperture
        hdulist = self.update_or_append_fits_layer(hdulist, layer_name, aperture_hdu)

        lcologs.log('Stored photometry in ' + self.image_path, 'info', log=log)

        return hdulist

def run_aperture_photometry(image, error, positions, radius):
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

    def __init__(self):
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
        self.raw_flux = np.array([])
        self.raw_err_flux = np.array([])
        self.pscale = np.array([])
        self.epscale = np.array([])

    def load_hdf5(self, file_path):
        """
        Method to load the entire photometry store file
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

            sources = np.array(f['source_catalog'])

            self.sources = Table([
                Column(name='gaia_id', data=sources[:,0]),
                Column(name='ra', data=sources[:,1], unit=u.deg),
                Column(name='dec', data=sources[:,2], unit=u.deg),
                Column(name='x', data=sources[:,3]),
                Column(name='y', data=sources[:,4])
            ])

            self.file = f['file'].asstr()[:].tolist()

            self.timestamps = Table([Column(name='HJD', data=np.array(f['HJD'][:]), unit=u.day)])
            self.raw_flux = np.array(f['raw_flux'])

            if 'flux' in f.keys():
                self.flux = np.array(f['flux'])

            if 'pscales' in f.keys():
                self.pscales = np.array(f['pscales'])

    def load_raw_fluxes(self, file_path):
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
            self.files = f['file'].asstr()[:]
            self.raw_flux = np.array(f['raw_flux'])
