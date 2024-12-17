from astroquery.gaia import Gaia
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np

def collect_Gaia_catalog(ra,dec,radius=15,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path='./'):
    """
    Collect the Gaia catalog for the field centered at ra,dec and radius.

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
