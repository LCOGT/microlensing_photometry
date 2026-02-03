import unittest
from astropy.table import Table, Column
from astropy.io import fits
from astropy.wcs import WCS
import numpy as np
from os import path, remove
from image_reduction.IO import fits_table_parser
from image_reduction.IO import hdf5

class FITSParser(unittest.TestCase):

    def test_fits_rec_to_table(self):

        # Create an Astropy table and convert it into a FITS
        # table for use as a test
        ndata = 100000
        column_list = [
                Column(name='id', data=np.arange(0,ndata,1)),
                Column(name='xcenter', data=np.random.rand(ndata)*100.0),
                Column(name='ycenter', data=np.random.rand(ndata)*100.0),
                Column(name='aperture_sum', data=np.random.rand(ndata)*1000.0),
                Column(name='aperture_sum_err', data=np.random.rand(ndata)*10.0)
            ]

        test_t = Table(column_list)
        test_fits = fits.table_to_hdu(test_t)

        new_t = fits_table_parser.fits_rec_to_table(test_fits)

        assert(type(new_t) == type(test_t))
        assert(new_t == test_t).all()

class HDF5(unittest.TestCase):

    def test_output_photometry(self):

        # Create test arrays for output
        nimages = 10
        nstars = 100
        catalog = Table([
            Column(name='source_id', data=np.array([5961294592641279104]*nstars), dtype='int64'),
            Column(name='ra', data=np.array([270.0]*nstars)),
            Column(name='dec', data=np.array([-22.0]*nstars))
        ])
        hdu = fits.PrimaryHDU()
        hdr = hdu.header
        hdr.set('NAXIS', 2)
        hdr.set('CTYPE1', 'RA---TAN', 'Type of WCS Projection')
        hdr.set('CTYPE2', 'DEC--TAN', 'Type of WCS Projection')
        hdr.set('CRPIX1', 2048.0000000, '[pixel] Coordinate of reference point (axis 1')
        hdr.set('CRPIX2', 2048.0000000, '[pixel] Coordinate of reference point (axis 2)')
        hdr.set('CRVAL1', 266.0433328, '[deg] RA at the reference pixel')
        hdr.set('CRVAL2', -39.1132223, '[deg] Dec at the reference pixel')
        hdr.set('CUNIT1', 'deg', 'Units of RA')
        hdr.set('CUNIT2', 'deg', 'Units of Dec')
        hdr.set('CD1_1', 0.0001081, 'WCS CD transformation matrix')
        hdr.set('CD1_2', -0.0000000, 'WCS CD transformation matrix')
        hdr.set('CD2_1', -0.0000000,  'WCS CD transformation matrix')
        hdr.set('CD2_2', -0.0001081, 'WCS CD transformation matrix')
        wcs_data = [WCS(hdr)]*nimages
        timestamps = np.linspace(52000, 53000, nimages)
        exptime = np.array([30.0]*nimages)
        flux = np.random.rand(nstars, nimages) * 10000.0
        err_flux = np.sqrt(flux)
        pscales = np.random.rand(nstars, nimages)
        epscales = np.random.rand(nstars, nimages)
        file_path = './aperture_phot_test.hd5'

        hdf5.output_photometry(catalog, timestamps, wcs_data, flux, err_flux, exptime, pscales, epscales, file_path)

        assert(path.isfile(file_path))
        remove(file_path)

if __name__ == '__main__':
    unittest.main()
