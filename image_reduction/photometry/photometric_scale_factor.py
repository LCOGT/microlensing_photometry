import numpy as np
import image_reduction.infrastructure.observations as lcoobs
import image_reduction.infrastructure.logs as lcologs


def photometric_scale_factor_from_lightcurves(lcs):
    """
    Estimate the 16,50,84 percentiles of the photometric scale factor, define as the median(flux)/flux for each
    epoch and stars.

    Parameters
    ----------
    lcs : array, an array containing all the lightcurves

    Returns
    -------
    pscales : array, the photometric scale factors
    """
    pscales = np.nanpercentile(lcs/np.nanmedian(lcs,axis=1)[:,None],[16,50,84],axis=0)

    return pscales

def calculate_pscale(obs_set, phot_catalogs, log=None):
    """
    Function to compute the pscale factor for a set of aperture photometry catalogs

    Params
    ------
    obs_set     object   Set of observations
    phot_catalogs   arr  Array of photometry catalogs

    Return
    ------
        pscsales, epscales array    Photometric scale factors and errors for all images
        flux, err_flux      array   Fluxes for all stars in all images
    """

    lcologs.log('Computing photometric scale factors', 'info', log=log)

    # Image list used to synchronise the references to each image
    # The first non-None image photometry catalog in the dictionary is used as the
    # reference by default.
    image_list = list(phot_catalogs.keys())
    ref_idx = None
    for k,im in enumerate(image_list):
        if phot_catalogs[im] and not ref_idx:
            ref_idx = k
    lcologs.log('Using image number ' + str(k) + ', (' + image_list[k] + ') as reference', 'info', log=log)

    # Default empty array is used to fill in the data cube for images where no photometry was possible
    nodata = np.empty(len(phot_catalogs[image_list[ref_idx]]))
    nodata.fill(np.nan)

    # Collate the timeseries photometry for all stars into a single array.
    lcs = np.array(
        [phot_catalogs[im]['aperture_sum'] if phot_catalogs[im] else nodata for im in obs_set.table['file']]
    ).T
    elcs = np.array(
        [phot_catalogs[im]['aperture_sum_err'] if phot_catalogs[im] else nodata for im in obs_set.table['file']]
    ).T

    #lcs = apsum.T
    #elcs = eapsum.T

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
    pscales = photometric_scale_factor_from_lightcurves(lcs[mask])
    epscales = (pscales[2] - pscales[0]) / 2
    lcologs.log('PSCALE values: ' + repr(pscales), 'info', log=log)
    lcologs.log('PSCALE uncertainties: ' + repr(epscales), 'info', log=log)

    # Now apply the photometric scale factor to all star lightcurve

    flux = lcs / pscales[1]
    err_flux = (elcs ** 2 / pscales[1] ** 2 + lcs ** 2 * epscales ** 2 / pscales[1] ** 4) ** 0.5

    return pscales, epscales, flux, err_flux, lcs, elcs
