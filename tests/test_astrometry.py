import numpy as np

from image_reduction.astrometry import wcs as lcowcs
from image_reduction.logistics import image_tools


def test_find_images_shifts():

    size = 4096

    randomX = np.random.uniform(0,size,1000)
    randomY = np.random.uniform(0, size, 1000)

    star_positions = np.c_[randomX,randomY]
    reference = image_tools.build_image(star_positions, [1]*len(randomX), (size,size), image_fraction = 0.25,
                                        star_limit = 1000)

    shiftx = 158.2
    shifty = 47.29

    star_positions2 = np.c_[randomX+shiftx, randomY+shifty]

    image = image_tools.build_image(star_positions2, [1]*len(randomX), (size,size), image_fraction = 0.25,
                                        star_limit = 1000)

    shiftX, shiftY = lcowcs.find_images_shifts(reference,image,image_fraction =0.25, upsample_factor=1)

    assert np.allclose((shiftx, shifty), -np.array([shiftX, shiftY]), atol=1)


def test_refine_image_wcs():

    pass

    #Need discussion on how to test this kind of function