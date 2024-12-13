from astroquery.gaia import Gaia
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np

def collect_Gaia_catalog(ra,dec,radius=15,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path='./'):
    """
    Collect the Gaia catalog for the field ra,dec,radius.

    Parameters
    ----------
    ra : right ascenscion in degree
    dec : right ascenscion in degree
    radius : radius in arcmin
    row_limit : the maximum number of stars (default:10000)
    catalog_name : the name of the catalog to save/load
    catalog_path : the path where to find the catalog

    Returns
    -------
    gaia_catalog : astropy.Table, sorted by magnitude (brighter to fainter)
    """

    try:

        from astropy.table import Table

        gaia_catalog = Table.read(catalog_name, format='ascii')

    except:

        Gaia.ROW_LIMIT = row_limit
        coord = SkyCoord(ra=ra, dec=dec, unit=(u.degree, u.degree), frame='icrs')

        radius = u.Quantity(radius /60., u.deg)

        gaia_catalog = Gaia.query_object_async(coordinate=coord,radius = radius)
        gaia_catalog =  gaia_catalog[ gaia_catalog['phot_g_mean_mag'].argsort(),]
        gaia_catalog.write(catalog_name, format='ascii')


    return  gaia_catalog

def build_Gaia_image(gaia_catalog,image_wcs, image_shape=(4096,4096), star_limit = 1000):
    """
    Construct a model image of the Gaia catalog, useful for estimating intial X,Y shifts.

    Parameters
    ----------
    gaia_catalog : an astropy table containing the Gaia catalog
    image_wcs :  an astropy wcs object to convert ra,dec to x,y
    image_shape : the shape of the image to build
    star_limit : limit to the X brightest star to simulate

    Returns
    -------
    gaia_image : a model image of the Gaia catalog
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
     intensity : the intensity of the gaussian
     x_center :  the gaussian center in x direction
     y_center :  the gaussian center in y direction
     width_x :  the gaussian sigma x
     width_y :  the gaussian sigma y
     X_star :  2D  array containing the X value
     Y_star :  2D  array containing the Y value

     Returns
     -------
     model : return the 2D gaussiam
     """
    model = intensity * np.exp(
            -(((X_star -x_center) / width_x) ** 2 + \
              ((Y_star - y_center) / width_y) ** 2) / 2)

    return model