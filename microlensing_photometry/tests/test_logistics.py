from os import getcwd, path, remove
import numpy as np

from microlensing_photometry.logistics import vizier_tools
import microlensing_photometry.logistics.GaiaTools.GaiaCatalog as GC
from microlensing_photometry.astrometry import wcs as lcowcs
from microlensing_photometry.logistics import image_tools

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



import numpy as np

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

    pass

    #Did not find test in the original pyDANDIA so I pass for time been
