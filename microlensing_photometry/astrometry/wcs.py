from skimage.registration import phase_cross_correlation
import numpy as np
from astropy.coordinates import SkyCoord
import scipy.spatial as sspa
from skimage.measure import ransac
from skimage import transform as tf
from astropy.wcs import WCS,utils

import microlensing_photometry.logistics.GaiaTools.GaiaImage as GI

def find_images_shifts(reference,image,image_fraction =0.25, upsample_factor=1):
    """
    Estimate the shifts (X,Y) between two images. Generally a good idea to do only a fraction of the field of view
    centred in the middle, where the effect of rotation is minimal.

    Parameters
    ----------
    reference : array,  an image acting as the reference
    image : array,  the image we want to align
    image_fraction : float, the fraction of image around the center we want to analyze
    upsample_factor : float, the degree of upsampling, if one wants subpixel accuracy


    Returns
    -------
    shiftx : float, the shift in pixels in the x direction
    shifty : float, the shift in pixels in the y direction
    """

    leny, lenx = (np.array(image.shape) * image_fraction).astype(int)
    starty,startx = (np.array(image.shape)*0.5-[leny/2,lenx/2]).astype(int)

    subref = reference.astype(float)[starty:starty+leny,startx:startx+lenx]
    subimage = image.astype(float)[starty:starty+leny,startx:startx+lenx]
    shifts, errors, phasediff = phase_cross_correlation(subref,subimage,
                                                        normalization=None,
                                                        upsample_factor=upsample_factor)

    shifty,shiftx = shifts

    return shiftx,shifty


def refine_image_wcs(image, stars_image, image_wcs, gaia_catalog, star_limit = 1000):
    """
    Refine the WCS of an image with Gaia catalog. First, find shifts in X,Y between the image stars catalog and
    a model image of the Gaia catalog. Then compute the full WCS solution using ransac and a affine transform.

    Parameters
    ----------
    image : array, the image to refine the WCS solution
    stars_image : array, the x,y positions of stars in the image
    image_wcs : astropy.wcs, the original astropy WCS solution
    gaia_catalog : astropy.Table, the entire gaia catalog


    Returns
    -------
    new_wcs : astropy.wcs, an updated astropy WCS object
    """
    mo_image, skycoords, star_pix = GI.build_Gaia_image(gaia_catalog, image_wcs, image_shape=image.shape)

    shiftx, shifty = find_images_shifts(mo_image, image, image_fraction=0.5, upsample_factor=1)

    dists = sspa.distance.cdist(stars_image[:star_limit],
                                np.c_[star_pix[0][:star_limit] - shiftx, star_pix[1][:star_limit] - shifty])

    mask = dists < 20
    lines, cols = np.where(mask)

    pts1 = np.c_[star_pix[0], star_pix[1]][:star_limit][cols]
    pts2 = np.c_[stars_image[:,0], stars_image[:,1]][:star_limit][lines]
    model_robust, inliers = ransac((pts2, pts1), tf.AffineTransform, min_samples=10, residual_threshold=3,
                                   max_trials=300)

    new_wcs = utils.fit_wcs_from_points(pts2[:star_limit][inliers].T, skycoords[cols][inliers])


    ### might be valuable some looping here

    # Refining???
    # dists2 = distance.cdist(np.c_[sources['xcentroid'],sources['ycentroid']][:500],projected2[:500])
    # mask2 = dists2<1
    # lines2,cols2 = np.where(mask2)
    # pts12 = np.c_[star_pix[0],star_pix[1]][:500][cols2]
    # pts22 = np.c_[sources['xcentroid'],sources['ycentroid']][:500][lines2]

    # model_robust2, inliers2 = ransac(( pts22,pts12), tf. AffineTransform,min_samples=10, residual_threshold=1, max_trials=300)
    # projected22 = model_robust2.inverse(pts12)
    # projected222 = model_robust2.inverse(np.c_[star_pix[0],star_pix[1]])

    # print(shifts)

    return new_wcs