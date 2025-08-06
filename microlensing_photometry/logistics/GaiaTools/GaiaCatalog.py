from astroquery.gaia import Gaia
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table, Column
import numpy as np
from os import path

from microlensing_photometry.logistics import vizier_tools
from microlensing_photometry.infrastructure import logs as lcologs
from microlensing_photometry.photometry import conversions

def collect_Gaia_catalog(ra,dec,radius=15,row_limit = 10000, catalog_name='Gaia_catalog.dat',
                         catalog_path='./',timeout=60, log=None):
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

        gaia_catalog = Table.read(catalog_path, format='ascii')

        lcologs.log(
            'Loaded Gaia catalog from pre-existing file ' + catalog_path,
            'info',
            log=log
        )

    except:

        gaia_catalog = vizier_tools.search_vizier_for_sources(
            ra,
            dec,
            radius,
            'Gaia-EDR3',
            row_limit=row_limit,
            coords='degree',
            timeout=timeout,
            log=log,
            debug=True
        )

        if len(gaia_catalog):
            mask = np.isfinite(gaia_catalog['phot_g_mean_flux'])
            sub_gaia_catalog = gaia_catalog[mask]
            sub_gaia_catalog =  sub_gaia_catalog[sub_gaia_catalog['phot_g_mean_flux'].argsort()[::-1],]
            sub_gaia_catalog.write(catalog_path, format='ascii')

            gaia_catalog = sub_gaia_catalog
        else:
            gaia_catalog = None

    return  gaia_catalog

def gaia_flux_to_mag(Gflux, Gferr, passband="G"):
    """Function to convert Gaia flux measurements to photometric magnitudes
    on the VEGAMAG system, using the zeropoints produced from Gaia
    Data Release 2 published in Evans, D.W. et al., 2018, A&A, 616, A4.

    Passband options are:
        G       (default)   ZP=25.6884 +/- 0.0018
        G_BP                ZP=25.3514 +/- 0.0014
        G_RP                ZP=24.7619 +/- 0.0019

    Parameters:
        Gflux   float   Gaia flux measurements
        Gferr   float   Gaia flux uncertainty
        passband    str [optional] Gaia passband, one of G, G_BP, G_RP (default=G)

    Returns:
        Gmag    float   Gaia magnitude
        Gmerr   float   Gaia magnitude uncertainty
    """

    if passband == "G":
        ZP = 25.6884
        sigZP = 0.0018
    elif passband == 'G_BP':
        ZP = 25.3514
        sigZP = 0.0014
    elif passband == 'G_RP':
        ZP = 24.7619
        sigZP = 0.0019
    else:
        raise ValueError('No Gaia photometric transform available for passband ' + passband)

    if type(Gflux) == type(Column()):

        idx = np.where(Gflux > 0.0)
        Gmag = np.zeros(len(Gflux))
        Gmerr = np.zeros(len(Gflux))

        (Gmag[idx], Gmerr[idx]) = phot_conversion(Gflux[idx], Gferr[idx])

    else:

        if Gflux > 0.0:
            (Gmag, Gmerr) = phot_conversion(Gflux, Gferr)
        else:
            Gmag = 0.0
            Gmerr = 0.0

    return Gmag, Gmerr


def calc_gaia_magnitudes(gaia_catalog):
    """
    Function to convert the fluxes provided in a Gaia catalog to magnitudes

    Parameters:
        gaia_catalog  Table  Output of Gaia DR3 astroquery

    Returns:
        gaia_catalog  Table  Catalog table with columns added for magnitudes
    """

    # Dictionary of Gaia passbands converted to column names to be added to the catalog
    bandpasses = {
        'G': 'G',
        'G_BP': 'BP',
        'G_RP': 'RP'
    }

    # If the table does not already have columns for magnitude and uncertainty,
    # assume they need to be computed
    for f, key in bandpasses.items():
        if key + 'mag' not in gaia_catalog.colnames:
            mag, merr = gaia_flux_to_mag(
                gaia_catalog['phot_' + str(key).lower() + '_mean_flux'],
                gaia_catalog['phot_' + str(key).lower() + '_mean_flux_error'],
                passband=f
            )

            gaia_catalog.add_columns(
                [
                    Column(name=key + 'mag', data=mag),
                    Column(name=key + 'mag_err', data=merr),
                ]
            )

    return gaia_catalog


def calc_gaia_colours(gaia_catalog):
    """
    Function to calculate the Gaia BP-RP colours and uncertainties

    Parameters:
        gaia_catalog  Table  Output of Gaia DR3 astroquery

    Returns:
        gaia_catalog  Table  Catalog table with columns added for colors
    """

    if 'BP-RP' not in gaia_catalog.colnames:
        BP_RP = Column(name='BP-RP', data=(gaia_catalog['BPmag'] - gaia_catalog['RPmag']))

        BPRP_err = Column(
            name='BP-RP_err',
            data=(np.sqrt((gaia_catalog['BPmag_err'] * gaia_catalog['BPmag_err']) +
                          (gaia_catalog['RPmag_err'] * gaia_catalog['RPmag_err']))),
        )

        gaia_catalog.add_columns([BP_RP, BPRP_err])

    return gaia_catalog


def transform_gaia_phot_to_SDSS(gaia_catalog):
    """
    Function using the conversion transformations published in
    Evans, D.W. et al., 2018, A&A, 616, A4 to calculate the SDSS
    g, r and i magnitudes based on the Gaia photometry.

    Parameters:
        gaia_catalog  Table  Output of Gaia DR3 astroquery

    Returns:
        gaia_catalog  Table  Catalog table with columns added for SDSS photometry
    """

    g_coeff = [0.13518, -0.46245, -0.25171, 0.021349]
    Gg_err = 0.16497
    r_coeff = [-0.12879, 0.24662, -0.027464, -0.049465]
    Gr_err = 0.066739
    i_coeff = [-0.29676, 0.64728, -0.10141]
    Gi_err = 0.098957


    if 'gmag' not in gaia_catalog.colnames:
        G_g = gaia_catalog['Gmag'] - (
                    g_coeff[0] + g_coeff[1] * gaia_catalog['BP-RP'] + g_coeff[2] * gaia_catalog['BP-RP'] ** 2 + g_coeff[3] *
                    gaia_catalog['BP-RP'] ** 3)
        g_err = np.sqrt(
            (gaia_catalog['Gmag_err'] ** 2 + g_coeff[1] ** 2) * gaia_catalog['BP-RP_err'] ** 2
            + (4 * g_coeff[2] ** 2 * gaia_catalog['BP-RP'] ** 2 * gaia_catalog['BP-RP_err'] ** 2)
            + (9 * g_coeff[3] ** 2 * gaia_catalog['BP-RP'] ** 4 * gaia_catalog['BP-RP_err'] ** 2)
            + (Gg_err ** 2)
        )

        gaia_catalog.add_columns(
            [Column(name='gmag', data=G_g), Column(name='gmag_err', data=g_err)]
        )

    if 'rmag' not in gaia_catalog.colnames:
        G_r = gaia_catalog['Gmag'] - (
                    r_coeff[0] + r_coeff[1] * gaia_catalog['BP-RP'] + r_coeff[2] * gaia_catalog['BP-RP'] ** 2 + r_coeff[3] *
                    gaia_catalog['BP-RP'] ** 3)
        r_err = np.sqrt(
            (gaia_catalog['Gmag_err'] ** 2
             + r_coeff[1] ** 2) * gaia_catalog['BP-RP_err'] ** 2
            + (4 * r_coeff[2] ** 2 * gaia_catalog['BP-RP'] ** 2 * gaia_catalog['BP-RP_err'] ** 2)
            + (9 * r_coeff[3] ** 2 * gaia_catalog['BP-RP'] ** 4 * gaia_catalog['BP-RP_err'] ** 2)
            + (Gr_err ** 2)
        )

        gaia_catalog.add_columns(
            [Column(name='rmag', data=G_r), Column(name='rmag_err', data=r_err)]
        )

    if 'imag' not in gaia_catalog.colnames:
        G_i = gaia_catalog['Gmag'] - (
                    i_coeff[0] + i_coeff[1] * gaia_catalog['BP-RP'] + i_coeff[2] * gaia_catalog['BP-RP'] ** 2)
        i_err = np.sqrt(
            (gaia_catalog['Gmag_err'] ** 2
             + i_coeff[1] ** 2) * gaia_catalog['BP-RP_err'] ** 2
            + (4 * i_coeff[2] ** 2 * gaia_catalog['BP-RP'] ** 2 * gaia_catalog['BP-RP_err'] ** 2)
            + (Gi_err ** 2)
        )

        gaia_catalog.add_columns(
            [Column(name='imag', data=G_i), Column(name='imag_err', data=i_err)]
        )

    return gaia_catalog

def find_nearest(catalog, ra, dec, radius=(2.0/3600.0)*u.deg, log=None):
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
        lcologs.log(
            'Found nearest matching star ' + str(idx[0]) + ' ' + repr(catalog[idx[0]]),
            'info',
            log=log
        )
        return idx[0], catalog[idx[0]]

    else:
        lcologs.log(
            'No matching star found within search radius=' + str(radius) + ' deg',
            'warning',
            log=log
        )
        return None, None
