import numpy as np
import image_reduction.infrastructure.observations as lcoobs
import image_reduction.infrastructure.logs as lcologs


def photometric_scale_factor_from_lightcurves(lcs, log=None, debug=False):
    """
    Estimate the 16,50,84 percentiles of the photometric scale factor, define as the median(flux)/flux for each
    epoch and stars.

    Parameters
    ----------
    lcs : array, an array containing all the lightcurves
    log: logger object

    Returns
    -------
    pscales : 2D array, the photometric scale factors for all stars in all images
    epscales: 2D array, uncertainties of the photometric scale factor
    """

    pscales = np.nanpercentile(lcs/np.nanmedian(lcs,axis=1)[:,None],[16,50,84], axis=0)
    epscales = (pscales[2] - pscales[0]) / 2

    if debug:
        lcologs.log('PSCALE values: ' + repr(pscales), 'info', log=log)
        lcologs.log('PSCALE uncertainties: ' + repr(epscales), 'info', log=log)

    return pscales, epscales

def calculate_pscale(ref_image, obs_set, phot_timeseries, log=None):
    """
    Function to compute the pscale factor for a set of aperture photometry catalogs

    Params
    ------
    ref_image string Name of reference image
    obs_set ObservationSet  Set of images in the dataset
    phot_timeseries arr timeseries photometry

    Return
    ------
    pscsales, epscales array    Photometric scale factors and errors for all images
    flux, err_flux      array   Fluxes for all stars in all images
    """

    lcologs.log('Computing photometric scale factors', 'info', log=log)

    # Image list used to synchronise the references to each image
    # The first non-None image photometry catalog in the dictionary is used as the
    # reference by default.
    image_set = [str(image) for image in obs_set.table['file']]
    ref_idx = image_set.index(ref_image)
    lcologs.log('Using image number ' + str(ref_idx) + ', (' + obs_set.table['file'][ref_idx] + ') as reference', 'info', log=log)

    # Collate the timeseries photometry for all stars into a single array.
    lcs = phot_timeseries[:, ::2]
    elcs = phot_timeseries[:, 1::2]

    # Select datapoints that have a reasonable SNR to avoid high uncertainty on the pscale factor,
    # using only stars with Gaia IDs because we can be sure that these stars are the same.
    SNR = 10
    valid = (np.abs(elcs / lcs) < 1 / SNR) & (lcs > 0)

    # Select the stars with the highest number of valid datapoints.
    # (This works because Python interprets Booleans and 1, 0)
    nvalid = valid.sum(axis=1)

    # Set the threshold number of valid points to require from a 75% percentile of the
    # maximum
    min_nvalid = np.nanpercentile(nvalid,[75])[0]

    # Create a mask for those lightcurves with at least this number of valid datapoints
    mask = nvalid >= min_nvalid
    lcologs.log(
        'Selected ' + str(mask.sum()) + ' lightcurves with at least '
        + str(min_nvalid) + ' valid datapoints',
        'info', log=log
    )

    # Compute the phot scale based on <950 stars
    pscales, epscales = photometric_scale_factor_from_lightcurves(lcs[mask], log=log)

    # Now apply the photometric scale factor to all star lightcurve
    flux = np.zeros(phot_timeseries.shape)
    flux[:, ::2] = lcs / pscales[1]
    flux[:, 1::2] = (elcs ** 2 / pscales[1] ** 2 + lcs ** 2 * epscales ** 2 / pscales[1] ** 4) ** 0.5

    return pscales, epscales, flux
