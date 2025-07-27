import os
import unittest
class TestObservationSet(unittest.TestCase):

    def test_create(self):
        from src.infrastructure import data_classes

        obs_set = data_classes.ObservationSet()

        file_path = './test_data/lsc1m009-fa04-20250610-0414-e91.fits'
        header = {
            'SITEID': 'lsc',
            'ENCID': 'domb',
            'TELID': '1m0a',
            'INSTRUME': 'fa04',
            'FILTER': 'ip',
            'DATE-OBS': '2025-06-11T09:21:20.374',
            'EXPTIME': 20.0,
            'RA': '17:29:57.46820',
            'DEC': '-38:41:44.5230',
            'AIRMASS': 1.7141329,
            'L1FWHM': 2.247347124258783,
            'MOONFRAC': 0.9982681,
            'MOONDIST': 11.2136953,
            'L1MEAN': 2784.0373360045505,
            'CTYPE1': 'RA---TAN',
            'CTYPE2': 'DEC--TAN',
            'CRPIX1': 2048.5,
            'CRPIX2': 2048.5,
            'CRVAL1': 262.48945081,
            'CRVAL2': -38.6957009313,
            'CUNIT1': 'deg',
            'CUNIT2': 'deg',
            'CD1_1': 0.000108308535116,
            'CD1_2': -3.18163483965E-07,
            'CD2_1': -3.18163483965E-07,
            'CD2_2': -0.000108308535116,
            'WCSERR': 0,
            'NAXIS1': 4096,
            'NAXIS2': 4096,
            'WMSCLOUD': -18.6080000
        }

        obs_set.add_observation(file_path, header)

        assert(len(obs_set.table) == 1)

        test_output_path = 'test_input/data_summary.txt'
        obs_set.save(test_output_path)

        assert(os.path.isfile(test_output_path))

        #os.remove(test_output_path)

    def test_load(self):
        from src.infrastructure import data_classes

        test_path = 'test_input/data_summary.txt'

        obs_set = data_classes.ObservationSet(file_path=test_path)

        assert(len(obs_set.table) == 1)
        assert(len(obs_set.table.colnames) == 29)