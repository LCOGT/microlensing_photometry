from astroquery.gaia import Gaia
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np
from os import path

from microlensing_photometry.logistics import vizier_tools

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
    catalog_path = path.join(catalog_path, catalog_name)

    try:

        from astropy.table import Table

        gaia_catalog = Table.read(catalog_path, format='ascii')

    except:

        gaia_catalog = vizier_tools.search_vizier_for_sources(ra, dec, radius, 'Gaia-DR3', row_limit=-1,
                                  coords='degree', log=None, debug=True)

        mask = np.isfinite(gaia_catalog['phot_g_mean_flux'])
        sub_gaia_catalog = gaia_catalog[mask]
        sub_gaia_catalog =  sub_gaia_catalog[sub_gaia_catalog['phot_g_mean_flux'].argsort()[::-1],]
        sub_gaia_catalog.write(catalog_path, format='ascii')

        gaia_catalog = sub_gaia_catalog

    return  gaia_catalog
