import unittest
from astropy.table import Table, Column
from astropy.io import fits
import numpy as np

class FITSParser(unittest.TestCase):

    def test_fits_rec_to_table(self):
        from microlensing_photometry.IO import fits_table_parser

        # Create an Astropy table and convert it into a FITS
        # table for use as a test
        ndata = 10
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

if __name__ == '__main__':
    unittest.main()
