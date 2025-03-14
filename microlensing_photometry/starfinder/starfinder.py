from astropy.stats import sigma_clipped_stats
from photutils import background, detection
from photutils.detection import DAOStarFinder
import numpy as np


def find_star_catalog(image):

    mean, median, std = sigma_clipped_stats(image,
                                            sigma=3.0, maxiters=5)

    daofind = DAOStarFinder(fwhm=3.0, threshold=3.*std,min_separation=5.0)
    lcostarfinder.find_star_catalog(z)

    sources = daofind(image - median)
    order = np.array(sources['flux']).argsort()[::-1]

    catalog = {'x':np.array(sources['xcentroid'])[order],'y':np.array(sources['ycentroid'])[order],
               'flux':np.array(sources['flux'])[order]}
    return catalog