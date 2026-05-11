import os
from prefect import task
from skimage.registration import phase_cross_correlation
import numpy as np
from astropy.coordinates import SkyCoord
import scipy.spatial as sspa
from skimage.measure import ransac
from skimage import transform as tf
from astropy.wcs import WCS, utils
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table, Column

from image_reduction.logistics import image_tools
from image_reduction.data_quality import astrometry_qc
from image_reduction.infrastructure import logs as lcologs
from image_reduction.IO import ds9_utils

@task
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

@task
def refine_image_wcs(analyst, star_limit=30000, log=None, debug=False):
    """
    Refine the WCS of an image with Gaia catalog. First, find shifts in X,Y between the image stars catalog and
    a model image of the Gaia catalog. Then compute the full WCS solution using ransac and a affine transform.

    Parameters
    ----------
    analyst: AperturePhotometryAnalyst
    star_limit : int, the limit number of stars to use
    log : object pipeline log

    Returns
    -------
    new_wcs : astropy.wcs, an updated astropy WCS object
    """

    skycoords = SkyCoord(ra=analyst.sources['ra'].data[:star_limit],
                      dec=analyst.sources['dec'].data[:star_limit],
                      unit=(u.degree, u.degree), frame='icrs')

    fluxes = [1]*len(analyst.sources['phot_g_mean_flux'].data)
    star_pix = analyst.image_original_wcs.world_to_pixel(skycoords)
    lcologs.log('Calculated image coordinates for ' + str(len(star_pix[0])) + ' catalog stars', 'info', log=log)

    stars_positions = np.array(star_pix).T

    wcs_check = astrometry_qc.check_stars_within_frame(analyst.image_data.shape, stars_positions, log=log)

    if wcs_check:
        model_gaia_image = image_tools.build_image(stars_positions, fluxes, analyst.image_data.shape,
                                                    image_fraction=1, star_limit = star_limit)
        if debug:
            file_path = os.path.join(analyst.dir_path, 'debug', analyst.image_name.replace('.fits', '_gaia.fits'))
            image_tools.output_image(model_gaia_image, file_path)
            file_path = os.path.join(analyst.dir_path, 'debug', analyst.image_name.replace('.fits', '_gaia.reg'))
            ds9_utils.output_ds9_overlay(stars_positions, file_path, format='array', colour='magenta', xcol=0,
                                         ycol=1)

        model_image = image_tools.build_image(analyst.image_source_catalog[:,:2],
                                              [1]*len(analyst.image_source_catalog),
                                              analyst.image_data.shape, image_fraction=1,
                                              star_limit = star_limit)
        if debug:
            file_path = os.path.join(analyst.dir_path, 'debug', analyst.image_name.replace('.fits', '_det.fits'))
            image_tools.output_image(model_image, file_path)
            file_path = os.path.join(analyst.dir_path, 'debug', analyst.image_name.replace('.fits', '_det.reg'))
            ds9_utils.output_ds9_overlay(analyst.image_source_catalog, file_path, format='array', colour='green',
                                         xcol=0, ycol=1)

        shiftx, shifty = find_images_shifts(model_gaia_image, model_image, image_fraction=0.25, upsample_factor=1)
        lcologs.log('Calculated image shifts in x,y = ' + str(shiftx) + ', ' + str(shifty), 'info', log=log)

        dists = sspa.distance.cdist(analyst.image_source_catalog[:star_limit,:2],
                                    np.c_[star_pix[0][:star_limit] - shiftx,
                                    star_pix[1][:star_limit] - shifty])
        mask = dists < 10
        lines, cols = np.where(mask)

        pts1 = np.c_[star_pix[0], star_pix[1]][:star_limit][cols]
        pts2 = np.c_[analyst.image_source_catalog[:,0], analyst.image_source_catalog[:,1]][:star_limit][lines]

        if len(pts1) > 5 and len(pts2) > 5:
            model_robust, inliers = ransac((pts2, pts1), tf.AffineTransform, min_samples=10, residual_threshold=5,
                                       max_trials=300)

            new_wcs = utils.fit_wcs_from_points(pts2[:star_limit][inliers].T, skycoords[cols][inliers])

            lcologs.log('New image WCS = ' + repr(new_wcs), 'info', log=log)

        else:
            new_wcs = None

    # Case where bad image_wcs causes misleading object positions
    else:
        new_wcs = None

    return new_wcs

def build_wcs_from_obs_set(obs_set):
    """
    Method to create an Astropy WCS object from a set of WCS keywords
    :return: im_wcs list of image WCS objects
    """
    wcs_params = [
        'CTYPE1',
        'CTYPE2',
        'CRPIX1',
        'CRPIX2',
        'CRVAL1',
        'CRVAL2',
        'CUNIT1',
        'CUNIT2',
        'CD1_1',
        'CD1_2',
        'CD2_1',
        'CD2_2',
        'NAXIS1',
        'NAXIS2'
    ]

    im_wcs = []
    for row in obs_set.table:
        params = {key: row[key] for key in wcs_params}
        im_wcs.append(WCS(header=params))

    return im_wcs
