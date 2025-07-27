from sys import argv
from astroquery.vizier import Vizier
from astroquery.gaia import Gaia
from astropy import wcs, coordinates, units, visualization, table
import requests
from src.infrastructure import logs as lcologs

def search_vizier_for_sources(ra, dec, radius, catalog, row_limit=-1,
                              coords='sexigesimal', timeout=60, log=None, debug=False):
    """Function to perform online query of the catalog and return
    a catalogue of known objects within the field of view

    Parameters
    ----------
    ra : str or float, RA J2000 in sexigesimal or degree
    dec : str or float, DEC J2000 in sexigesimal or degree
    catalog : str, the catalog to query
    row_limit : int, the maximum number of row desired
    coords : str, the type of coordinates given, i.e. sexagesimal or degree
    log : logger, a logger object to write in (not in use it seems)
    debug: bool, debug mode or not

    Returns
    -------
    result : astropy.Table, an astropy.Table containing the catalog
    """

    supported_catalogs = { '2MASS': ['2MASS',
                                     {'_RAJ2000':'_RAJ2000', '_DEJ2000':'_DEJ2000', 'Jmag':'Jmag', 'e_Jmag':'e_Jmag', \
                                    'Hmag':'Hmag', 'e_Hmag':'e_Hmag','Kmag':'Kmag', 'e_Kmag':'e_Kmag'},
                                    {'Jmag':'<20'}],
                           'VPHAS+': ['II/341',
                                      {'sourceID':'sourceID', 'RAJ2000':'_RAJ2000', 'DEJ2000':'_DEJ2000',
                                      'gmag':'gmag', 'e_gmag':'e_gmag', 'rmag':'rmag', 'e_rmag':'e_rmag',
                                      'imag':'imag', 'e_imag':'e_imag', 'clean':'clean'},
                                    {}],
                            'Gaia-DR2': ['I/345/gaia2',
                                      {'RA_ICRS':'ra', 'DE_ICRS':'dec', 'Source':'source_id',
                                      'e_RA_ICRS':'ra_error', 'e_DE_ICRS':'dec_error',
                                      'FG':'phot_g_mean_flux', 'e_FG':'phot_g_mean_flux_error',
                                      'FBP':'phot_bp_mean_flux', 'e_FBP':'phot_bp_mean_flux_error',
                                      'FRP':'phot_rp_mean_flux', 'e_FRP':'phot_rp_mean_flux_error' },
                                    {}],
                            'Gaia-EDR3': ['I/350/gaiaedr3',
                                    {'RA_ICRS':'ra', 'DE_ICRS':'dec', 'Source':'source_id',
                                    'e_RA_ICRS':'ra_error', 'e_DE_ICRS':'dec_error',
                                    'FG':'phot_g_mean_flux', 'e_FG':'phot_g_mean_flux_error',
                                    'FBP':'phot_bp_mean_flux', 'e_FBP':'phot_bp_mean_flux_error',
                                    'FRP':'phot_rp_mean_flux', 'e_FRP':'phot_rp_mean_flux_error',
                                    'PM':'pm', 'pmRA':'pm_ra', 'e_pmRA':'pm_ra_error',
                                    'pmDE':'pm_dec', 'e_pmDE':'pm_dec_error',
                                    'Plx':'parallax', 'e_Plx': 'parallax_error'},
                                    #'parallax':'parallax', 'parallax_error': 'parallax_error'},
                                    {}],
                           'Gaia-DR3': ['I/355/gaiadr3',
                                         {'RA_ICRS': 'ra', 'DE_ICRS': 'dec', 'Source': 'source_id',
                                          'e_RA_ICRS': 'ra_error', 'e_DE_ICRS': 'dec_error',
                                          'FG': 'phot_g_mean_flux', 'e_FG': 'phot_g_mean_flux_error',
                                          'FBP': 'phot_bp_mean_flux', 'e_FBP': 'phot_bp_mean_flux_error',
                                          'FRP': 'phot_rp_mean_flux', 'e_FRP': 'phot_rp_mean_flux_error',
                                          'PM': 'pm', 'pmRA': 'pm_ra', 'e_pmRA': 'pm_ra_error',
                                          'pmDE': 'pm_dec', 'e_pmDE': 'pm_dec_error',
                                          'Plx': 'parallax', 'e_Plx': 'parallax_error'},
                                         {}],
                           }

    (cat_id,cat_col_dict,cat_filters) = supported_catalogs[catalog]

    v = Vizier(columns=list(cat_col_dict.keys())+["+_r"], \
               column_filters=cat_filters)

    v.ROW_LIMIT = row_limit

    if 'sexigesimal' in coords:
        c = coordinates.SkyCoord(ra+' '+dec, frame='icrs', unit=(units.hourangle, units.deg))
    else:
        c = coordinates.SkyCoord(ra, dec, frame='icrs', unit=(units.deg, units.deg))

    r = radius * units.arcminute

    # query_vizier_service function depreciated by latest astroquery changes
    catalog_list = Vizier.find_catalogs(cat_id)
    #(status, result) = query_vizier_servers(v, c, r, [cat_id], debug=debug,timeout=timeout)

    result = v.query_region(c, radius=r, catalog=cat_id)

    if result != None and len(result) > 0:

        col_list = []
        for col_id, col_name in cat_col_dict.items():
            col = table.Column(name=col_name, data=result[0][col_id].data)
            col_list.append(col)

        result = table.Table( col_list )

        lcologs.log(
            'Queried ' + cat_id + ' and found ' + str(len(result)) + ' stars within the field of view',
            'info',
            log=log
        )

    else:
        result = table.Table([])
        lcologs.error(
            'No results returned from catalog query of ' + cat_id,
            'info',
            log=log
        )

    return result

def query_vizier_servers(query_service, coord, search_radius, catalog_id, log=None,
                        debug=False,timeout=60):
    """Function to query different ViZier servers in order of preference, as
    a fail-safe against server outages.  Based on code from NEOExchange by
    T. Lister

    Parameters
    ----------
    query_service : Astroquery Vizier service object
    coord : astrop.Skycoord, a coordinate in sky units
    search_radius : float, the search radius in degree
    catalog_id : str, the name of the catalog in Vizier's notation
    coords : str, the type of coordinates given, i.e. sexagesimal or degree
    log : logger, a logger object to write in (not in use it seems)
    debug: bool, debug mode or not

    Returns
    -------
    status : bool, success or not of the query
    result : astropy.Table, an astropy.Table containing the catalog
    """

    vizier_servers_list = ['vizier.cds.unistra.fr', 'vizier.cfa.harvard.edu']

    query_service.VIZIER_SERVER = vizier_servers_list[0]

    query_service.TIMEOUT = timeout

    continue_query = True
    iserver = 0
    status = True

    while continue_query:
        query_service.VIZIER_SERVER = vizier_servers_list[iserver]
        query_service.TIMEOUT = timeout

        if debug:
            print('Searching catalog server '+repr(query_service.VIZIER_SERVER))

        if log != None:
            log.warning('Searching catalog server '+repr(query_service.VIZIER_SERVER))

        try:
            # Query_region now returns a TableList object, so we need to extract the result
            result = query_service.query_region(coord, radius=search_radius, catalog=catalog_id)

        # Handle long timeout requests:
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
            if debug:
                print('Catalog server '+repr(query_service.VIZIER_SERVER)+' timed out, trying longer timeout')
            if log!= None:
                log.warning('Catalog server '+repr(query_service.VIZIER_SERVER)+' timed out, trying longer timeout')

            query_service.TIMEOUT = timeout
            result = query_service.query_region(coord, radius=search_radius, catalog=catalog_id)

        # Handle preferred-server timeout by trying the alternative server:
        except requests.exceptions.ConnectTimeout:
            if debug:
                print('Catalog server '+repr(query_service.VIZIER_SERVER)+' timed out again')
            if log != None:
                log.warning('Catalog server '+repr(query_service.VIZIER_SERVER)+' timed out again')

            iserver += 1
            if iserver >= len(vizier_servers_list):
                continue_query = False
                result = []
                status = False

                return status, result

        if result == None or len(result) == 0:
            iserver += 1
            if iserver >= len(vizier_servers_list):
                continue_query = False
                result = []
                status = False
        elif len(result) > 0:
            continue_query = False
            result = result[0]

    return status, result

def search_vizier_for_gaia_sources(ra, dec, radius, log=None):
    """Function to perform online query of the Gaia catalog and return
    a catalogue of known objects within the field of view

    Parameters
    ----------
    ra : float, RA J2000 in degree
    dec : float, DEC J2000 in degree
    radius : float, the search radius in arcmin
    log : logger, a logger object to write in (not in use it seems)

    Returns
    -------
    catalog : astropy.Table, an astropy.Table containing the catalog
    """

    c = coordinates.SkyCoord(ra+' '+dec, frame='icrs', unit=(units.deg, units.deg))
    r = units.Quantity(radius/60.0, units.deg)

    query_service = Vizier(row_limit=1e6, column_filters={},
    columns=['RAJ2000', 'DEJ2000', 'e_RAJ2000', 'e_DEJ2000', 'Gmag', 'e_Gmag', 'Dup'])


    if log!=None:
        log.info('Searching for gaia sources within '+repr(r)+' of '+repr(c))

    try:
        qs = Gaia.cone_search_async(c, r)
    except AttributeError:
        if log!=None:
            log.info('No search results received from Vizier service')
        raise IOError('No search results received from Vizier service')
    except requests.exceptions.HTTPError:
        if log!=None:
            log.info('HTTP error while contacting ViZier')
        raise requests.exceptions.HTTPError()

    result = qs.get_results()

    catalog = result['ra','dec','source_id','ra_error','dec_error',
                     'phot_g_mean_flux','phot_g_mean_flux_error',
                     'phot_rp_mean_flux','phot_rp_mean_flux_error',
                     'phot_bp_mean_flux','phot_bp_mean_flux_error']

    return catalog
