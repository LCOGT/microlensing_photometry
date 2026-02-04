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

def calculate_pscale(obs_set, image_catalogs, log=None):
    """
    Function to compute the pscale factor for a set of aperture photometry catalogs
    :return:
        pscsales, epscales array    Photometric scale factors and errors for all images
        flux, err_flux      array   Fluxes for all stars in all images
    """

    lcologs.log('Computing photometric scale factors', 'info', log=log)

    # Create the aperture lightcurves.  Default empty array is used to fill in
    # the data cube for images where no photometry was possible
    image_list = list(image_catalogs.keys())
    nodata = np.empty(len(image_catalogs[image_list[0]]))
    nodata.fill(np.nan)

    apsum = np.array(
        [image_catalogs[im]['aperture_sum'] if image_catalogs[im] else nodata for im in obs_set.table['file']]
    )
    eapsum = np.array(
        [image_catalogs[im]['aperture_sum_err'] if image_catalogs[im] else nodata for im in obs_set.table['file']]
    )

    lcs = apsum.T
    elcs = eapsum.T

    # Select datapoints that have a reasonable SNR to avoid high uncertainty on the pscale factor
    SNR = 10
    #mask = np.all((np.abs(elcs / lcs) < 1 / SNR) & (lcs > 0), axis=1)
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

    flux = lcs / pscales[1]
    err_flux = (elcs ** 2 / pscales[1] ** 2 + lcs ** 2 * epscales ** 2 / pscales[1] ** 4) ** 0.5

    return pscales, epscales, flux, err_flux, lcs, elcs