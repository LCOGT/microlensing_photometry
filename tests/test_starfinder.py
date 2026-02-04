import numpy as np
from scipy.stats import multivariate_normal

from image_reduction.starfinder import starfinder as lcostarfinder


def test_find_star_catalog():

    image = np.zeros((150,150))

    y,x = np.indices(image.shape)

    mu = np.array([75.0, 75.0])

    sigma = np.array([5, 5])
    covariance = np.diag(sigma ** 2)
    xy = np.c_[x.ravel(),y.ravel()]

    z = multivariate_normal.pdf(xy, mean=mu, cov=covariance)

    z = z.reshape(x.shape)
    cat = lcostarfinder.find_star_catalog(z)

    assert np.allclose([cat['x'][0],cat['y'][0],cat['flux'][0]],[75.,75.,0.14708267355499177])#,rtol=10**-5,atol=0)

