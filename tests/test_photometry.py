import numpy as np

import microlensing_photometry.photometry.photometric_scale_factor as lcopscale
from microlensing_photometry.photometry import psf as lcopsf

def test_aperture_photometry():

    pass

    #This needs some discussion on how to test that

def test_photometric_scale_factor_from_lightcurves():

    lcs = np.array(([[3,2,1],[0.5,2.5,9]]))
    pscales =  lcopscale.photometric_scale_factor_from_lightcurves(lcs)

    assert np.allclose(pscales,np.array([[0.408, 1.   , 0.996],
                                         [0.85 , 1.   , 2.05 ],
                                         [1.292, 1.   , 3.104]]))


def test_Gaussian2d():

    X_star, Y_star = np.indices((5, 5))

    intensity, x_center, y_center, width_x, width_y = 1,2,2,2,2

    model = lcopsf.Gaussian2d(intensity, x_center, y_center, width_x, width_y, X_star, Y_star)

    assert np.allclose(model,np.array([[0.36787944, 0.53526143, 0.60653066, 0.53526143, 0.36787944],
                                    [0.53526143, 0.77880078, 0.8824969 , 0.77880078, 0.53526143],
                                    [0.60653066, 0.8824969 , 1.        , 0.8824969 , 0.60653066],
                                    [0.53526143, 0.77880078, 0.8824969 , 0.77880078, 0.53526143],
                                    [0.36787944, 0.53526143, 0.60653066, 0.53526143, 0.36787944]]))

