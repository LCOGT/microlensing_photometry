from astroquery.gaia import Gaia
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np
from os import path

from microlensing_photometry.logistics import vizier_tools

def collect_Gaia_catalog(ra,dec,radius=15,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path='./',timeout=60):
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

        gaia_catalog = vizier_tools.search_vizier_for_sources(ra, dec, radius, 'Gaia-EDR3', row_limit=row_limit,
                                  coords='degree', timeout=timeout,log=None, debug=True)

        mask = np.isfinite(gaia_catalog['phot_g_mean_flux'])
        sub_gaia_catalog = gaia_catalog[mask]
        sub_gaia_catalog =  sub_gaia_catalog[sub_gaia_catalog['phot_g_mean_flux'].argsort()[::-1],]
        sub_gaia_catalog.write(catalog_path, format='ascii')

        gaia_catalog = sub_gaia_catalog

    return  gaia_catalog

def find_nearest(catalog, ra, dec, radius=(2.0/3600.0)*u.deg):
    """
    Function to identify the nearest catalog entry to the given coordinates, within a cut-off radius

    Parameters
    ----------
    catalog catalog Table of objects within the image
    ra float    RA of location to search at [decimal deg]
    dec float   Dec of location to search at [decimal deg]
    radius float Search cut-off radius [decimal deg, default = 2 arcsec]

    Returns
    -------
    star_idx int    Index of star within the catalog
    closest match   catalog Table row for the closest match or None if no object is
                    within the search radius
    """

    sources = SkyCoord(catalog['ra'], catalog['dec'], frame='icrs', unit=(u.deg, u.deg))
    target = SkyCoord(ra, dec, frame='icrs', unit=(u.deg, u.deg))

    separations = target.separation(sources)

    idx = np.where(separations <= radius)[0]

    if len(idx) > 0:
        return idx[0], catalog[idx[0]]

    else:
        return None, None
