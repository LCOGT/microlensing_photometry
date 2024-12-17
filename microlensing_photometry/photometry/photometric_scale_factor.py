import numpy as np


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
