import numpy as np
import copy
import image_reduction.infrastructure.logs as lcologs
import matplotlib.pyplot as plt

def photometric_scale_factor_from_lightcurves(lcs, mask, dataset, log=None, debug=False):
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

    # Exclude NaN and negative flux entries
    valid = np.logical_and(~np.isnan(lcs), lcs > 10.0)
    lcs_cleaned = lcs.astype(float).copy()
    lcs_cleaned[~valid] = np.nan

    median_flux = np.nanmedian(lcs_cleaned, axis=1)[:, None]
    norm_lc = lcs/median_flux

    pscales = np.nanpercentile(norm_lc,[16,50,84], axis=0)
    ps = np.tile(pscales[1], (lcs.shape[0], 1))
    #epscales = (pscales[2] - pscales[0]) / 2
    epscales = np.nanmedian(abs(norm_lc - ps), axis=0)

    if debug:
        lcologs.log('PSCALE values: ' + repr(pscales), 'info', log=log)
        lcologs.log('PSCALE uncertainties: ' + repr(epscales), 'info', log=log)

        fig, ax = plt.subplots()
        im = 1
        ax.hist(norm_lc[:,im], bins=200, edgecolor='black')
        ylim = ax.get_ylim()
        ax.set_xlim([0,10])
        ax.plot([pscales[1,im]-epscales[im]]*2, ylim, 'c-')
        ax.plot([pscales[1,im]+epscales[im]]*2, ylim, 'c-', label='pscale error')
        ax.plot([pscales[1,im],pscales[1,im]], ylim, 'm-', label='pscale factor')
        ax.set_xlabel('Normalized flux radio for all stars')
        ax.set_ylabel('Frequency')
        ax.legend()
        plt.savefig('norm_lcs.png')
        plt.close(fig)

        fig, ax = plt.subplots()
        scatter = ax.scatter(
            x=dataset.sources['x'][mask], y=dataset.sources['y'][mask],
            c=norm_lc[:,im], cmap='PuBuGn', s=5,
            vmin=0.0, vmax=2.0
        )
        cb = plt.colorbar(scatter)
        cb.set_label('Norm flux residuals', fontsize=12)
        ax.set_xlabel('X pixel position')
        ax.set_ylabel('Y pixel position')

        plt.savefig('norm_lcs_spatial_variation.png')

    return pscales, epscales

def calculate_pscale(ref_image, obs_set, dataset, log=None, debug=True):
    """
    Function to compute the pscale factor for a set of aperture photometry catalogs

    Params
    ------
    ref_image string Name of reference image
    obs_set ObservationSet  Set of images in the dataset
    dataset AperturePhotometryDataset

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
    lcs = copy.deepcopy(dataset.raw_flux)
    elcs = copy.deepcopy(dataset.raw_err_flux)

    # Select datapoints that have a reasonable SNR to avoid high uncertainty on the pscale factor,
    # and those close to the center of the image, since the wings of the frame tend to have
    # larger residuals.
    SNR = 10
    valid = (np.abs(elcs / lcs) < 1 / SNR) & (lcs > 0)

    xcenter = dataset.sources['x'].max() / 2.0
    ycenter = dataset.sources['y'].max() / 2.0
    pix_radius = 1000
    separations = np.sqrt((dataset.sources['x'] - xcenter) ** 2 \
                          + (dataset.sources['y'] - ycenter) ** 2)
    central_mask = separations <= pix_radius
    central = np.repeat(central_mask[:, np.newaxis], valid.shape[1], axis=1)
    valid = np.logical_and(valid, central)

    # Select the stars with the highest number of valid datapoints.
    # (This works because Python interprets Booleans and 1, 0)
    # Count the number of valid datapoints per star
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
    pscales, epscales = photometric_scale_factor_from_lightcurves(lcs[mask], mask, dataset, log=log, debug=True)

    # Now apply the photometric scale factor to all star lightcurve
    flux = np.zeros(dataset.raw_flux.shape)
    dataset.flux = lcs / pscales[1]
    ps = np.tile(pscales[1], (lcs.shape[0], 1))
    eps = np.tile(epscales, (lcs.shape[0], 1))
    dataset.flux_err = (elcs ** 2 / pscales[1] ** 2 + lcs ** 2 * epscales ** 2 / pscales[1] ** 4) ** 0.5
    dataset.pscales = pscales
    dataset.epscales = epscales

    if debug:
        fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))
        plt.subplots_adjust(left=0.1, right=0.95, wspace=0.2)
        im = 0
        axs[0].hist((elcs / lcs)[:,im], bins=100, range=(-2,2), edgecolor='green', facecolor='green')
        med = np.nanmedian((elcs / lcs)[:,im])
        ylim = axs[0].get_ylim()
        axs[0].plot([(epscales[im]/pscales[1,im])]*2, ylim, 'k-', label='$\\sigma_{PS}/PS$')
        axs[0].plot([med]*2, ylim, 'm-.', label='Median $\\sigma_{f}/f$')
        axs[0].set_xlabel('$\\sigma_{f}/f$')
        axs[0].set_ylabel('Frequency')
        axs[0].set_title('Fractional flux errors for image ' + str(im))
        axs[0].legend()

        flux_frac = [np.nanmedian((elcs / lcs)[:,im]) for im in range(0,lcs.shape[1],1)]
        eps_frac = [np.nanmedian(epscales[im]/pscales[1,im]) for im in range(0,lcs.shape[1],1)]
        axs[1].plot(flux_frac, eps_frac, 'bd')
        xlim = axs[1].get_xlim()
        ylim = axs[1].get_ylim()
        axmax = max(xlim[1], ylim[1])
        axs[1].set_xlim((0.0, axmax))
        axs[1].set_ylim((0.0, axmax))
        axs[1].set_xlabel('Fractional error in flux')
        axs[1].set_ylabel('Fraction error in pscale')
        axs[1].set_title('Fractional errors for all images')

        plt.savefig('lightcurve_phot_uncertainties.png')

    return dataset
