import unittest
import numpy as np

from image_reduction.photometry import photometric_scale_factor, aperture_photometry


class PhotometricScaleFactor(unittest.TestCase):

    def test_calculate_pscale(self):

        # Set up test dataset
        # Columns: (mag, mag_err) for all stars in one image
        ref_image = 'lsc1m005-fa15-20260413-0222-e91.fits'
        nstars = 100
        nimages = 3
        dataset = aperture_photometry.AperturePhotometryDataset()
        dataset.file = [
            'lsc1m005-fa15-20260413-0222-e91.fits',
            'lsc1m005-fa15-20260413-0223-e91.fits',
            'lsc1m005-fa15-20260413-0224-e91.fits'
        ]
        dataset.raw_flux = np.zeros((nstars, nimages*2))
        mean_mags = np.random.uniform(15.0, 22.0, nstars)
        for i in range(0, nimages*2, 2):
            dataset.raw_flux[:,i] = mean_mags
            dataset.raw_flux[:,i+1] = np.random.normal(loc=0.1, scale=0.01, size=nstars)

        pscales, epscales, flux = photometric_scale_factor.calculate_pscale(ref_image, dataset, log=None)

        assert (pscales == np.ones((3,3))).all()
        assert (epscales == np.zeros(3)).all()
        assert (dataset.raw_flux.shape == flux.shape)
