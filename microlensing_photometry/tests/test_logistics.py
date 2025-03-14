from os import getcwd, path, remove
import numpy as np

from microlensing_photometry.logistics import vizier_tools
import microlensing_photometry.logistics.GaiaTools.GaiaCatalog as GC
from microlensing_photometry.astrometry import wcs as lcowcs
from microlensing_photometry.logistics import image_tools
from astropy.table import Table
import numpy as np

CWD = getcwd()
TEST_DATA_DIR = path.join(CWD, 'tests/test_output')
def test_collect_Gaia_catalog():

    ra,dec,radius = 270,-30, 0.1

    gaia_catalog = GC.collect_Gaia_catalog(ra,dec,radius=radius,row_limit = 10000,catalog_name='Gaia_catalog.dat',
                         catalog_path=TEST_DATA_DIR)

    assert len(gaia_catalog) == 5
    assert np.allclose(
        gaia_catalog['phot_g_mean_flux'].value,
        np.array([3687.07248 ,  830.04544,  747.28028,  252.18597 ,  201.87001]),
        atol = 0.01
    )

    filepath = path.join(TEST_DATA_DIR, 'Gaia_catalog.dat')
    assert(path.isfile(filepath))

    remove(filepath)


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
