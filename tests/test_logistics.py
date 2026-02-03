from os import getcwd, path, remove, makedirs
import shutil
import unittest

from image_reduction.logistics import (vizier_tools)
from image_reduction.logistics import GaiaCatalog as GC
from image_reduction.logistics import image_tools
from astropy.table import Table, Column
import numpy as np


CWD = getcwd()
TEST_DATA_DIR = path.join(CWD, 'tests/test_output/')
makedirs(TEST_DATA_DIR, exist_ok=True)

def test_collect_Gaia_catalog():


    ra,dec,radius = 270,-30, 0.1

    gaia_catalog = GC.collect_Gaia_catalog(ra,dec,radius=radius,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path=TEST_DATA_DIR)

    assert len(gaia_catalog) == 5
    assert np.allclose(
            gaia_catalog['phot_g_mean_flux'].value,
            np.array([3687.10009766,  822.27001953,  742.57000732,  251.30000305,
            201.86999512]),
            atol = 0.01)

    filepath = path.join(TEST_DATA_DIR, 'Gaia_catalog.dat')
    assert(path.isfile(filepath))

    #cleaning after tests
    remove(filepath)
    shutil.rmtree(path.join(CWD, 'tests'))

class CatalogTools(unittest.TestCase):

    def test_find_nearest(self):
        rng = np.random.default_rng()

        # Generate a catalog Table for testing
        nimages = 10
        nstars = 100
        ra = rng.standard_normal(nstars) + 270.0
        dec = rng.standard_normal(nstars) - 22.0
        catalog = Table([
            Column(name='source_id', data=np.array([5961294592641279104] * nstars), dtype='int64'),
            Column(name='ra', data=ra),
            Column(name='dec', data=dec)
        ])

        # Test search for a target known to be within the catalog
        star_idx, result = GC.find_nearest(catalog, catalog[0]['ra'], catalog[0]['dec'])
        assert(result['ra'] == catalog[0]['ra'])
        assert(result['dec'] == catalog[0]['dec'])

        # Test search for a target that is not within the catalog
        star_idx, result = GC.find_nearest(catalog, 10.0, 30.0)
        assert(result == None)

def test_build_image():

    size = 4096

    randomX = [15]
    randomY = [28]

    star_positions = np.c_[randomX,randomY]
    reference = image_tools.build_image(star_positions, [1]*len(randomX), (size,size), image_fraction = 1,
                                        star_limit = 1000)
    assert reference.shape == (size,size)
    assert reference.max() == 1
    assert reference.min() == 0


def test_vizier_tools():

    test_ra = 299.590
    test_dec = 35.201
    test_radius = 0.1

    source_catalog = vizier_tools.search_vizier_for_sources(
        test_ra,
        test_dec,
        test_radius,
        'Gaia-DR3',
        row_limit=-1,
        coords='degree',
        log=None,
        debug=True
    )

    assert(type(source_catalog) == type(Table([])))
    assert(len(source_catalog) == 1)
    assert(source_catalog['source_id'] == 2059383668236814720)
