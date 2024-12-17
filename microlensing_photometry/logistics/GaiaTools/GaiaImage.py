from astroquery.gaia import Gaia
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np


def build_Gaia_image(gaia_catalog, image_wcs, image_shape=(4096,4096), star_limit = 1000):
    """
    Construct a model image of the Gaia catalog, useful for estimating initial X,Y shifts.

    Parameters
    ----------
    gaia_catalog : astropy.Table, an astropy table containing the Gaia catalog
    image_wcs :  astropy.wcs, an astropy wcs object to convert ra,dec to x,y
    image_shape : array, the shape of the image to build
    star_limit : int, limit to the X brightest star to simulate

    Returns
    -------
    gaia_image : array, a model image of the Gaia catalog
    """

    coords = SkyCoord(ra=gaia_catalog['ra'].data[:star_limit],
                      dec=gaia_catalog['dec'].data[:star_limit],
                      unit=(u.degree, u.degree), frame='icrs')

    fluxes = 10**((22-gaia_catalog['phot_g_mean_mag'].data)/2.5)

    star_pix = image_wcs.world_to_pixel(coords)
    XX, YY = np.indices((13, 13))
    model_gaussian = Gaussian2d(1, 6, 6, 2, 2, XX, YY)

    model_image = np.zeros(image_shape)

    for ind in range(len(star_pix[0][:star_limit])):

        try:

            model_image[star_pix[1].astype(int)[ind] - 6:star_pix[1].astype(int)[ind] + 7,
            star_pix[0].astype(int)[ind] - 6:star_pix[0].astype(int)[ind] + 7] += model_gaussian*fluxes[ind]
        except:

            #skip borders
            pass

    return model_image,coords,star_pix

def Gaussian2d(intensity,x_center,y_center,width_x,width_y,X_star,Y_star):
    """
    A 2d Gaussian model, to ingest stars in image model

    Parameters
    ----------
    intensity : float, the intensity of the gaussian
    x_center : float,  the gaussian center in x direction
    y_center : float, the gaussian center in y direction
    width_x : float, the gaussian sigma x
    width_y : float, the gaussian sigma y
    X_star : array, 2D  array containing the X value
    Y_star : array,  2D  array containing the Y value

    Returns
    -------
    model : array,  return the 2D gaussiam
    """

    model = intensity * np.exp(
            -(((X_star -x_center) / width_x) ** 2 + \
              ((Y_star - y_center) / width_y) ** 2) / 2)

    return model