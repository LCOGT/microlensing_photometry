import numpy as np


def photometric_scale_factor_from_lightcurves(lcs):
    """
    Estimate the 16,50,84 percentiles of the photometric scale factor

    Parameters
    ----------
    lcs : an array containing all the lightcurves

    Returns
    -------
    pscales : the photometric scale factors
    """
    pscales = np.nanpercentile(lcs/np.nanmedian(lcs,axis=1)[:,None],[16,50,84],axis=0)

    return pscales
