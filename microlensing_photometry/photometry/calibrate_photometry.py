from microlensing_photometry.infrastructure import logs as lcologs
from microlensing_photometry.photometry import conversions
from scipy.odr import Model, Data, ODR
import numpy as np

class CalibratePhotometryAnalyst(object):
    """
    Analyst to calibrate photometry from one or more images,
    given catalog photometry to use as a reference.
    """

    def __init__(self, config, obs_set, gaia_catalog, pscales, epscales, flux, err_flux, log=None):
        self.config = config
        self.log = log
        self.obs_set = obs_set
        self.gaia_catalog = gaia_catalog
        self.pscales = pscales
        self.epscales = epscales
        self.flux = flux
        self.err_flux = err_flux
        self.ref_image = None

        # Initialize logging output
        self.log = log
        lcologs.log(
            'Initializing Calibrate Photometry Analyst'
            'info',
            log=self.log
        )

    def calibrate(self):

        # Select the image from the dataset to use as the reference.
        # This is controllable from the config file but if not
        # specified, the pipeline should select the best available option
        self.identify_ref_image()

        # Calculate instrumental magnitudes for measured fluxes from the reference image
        self.inst_mag, self.inst_err_mag, _, _ = conversions.flux_to_mag(self.flux[:, self.ref_image_index],
                                                                self.err_flux[:, self.ref_image_index])

        # Select stars with valid measurements for both catalog and pipeline photometry
        self.select_calib_stars()

        # Perform model fit to determine the transformation from instrumental to
        # calibrated photometry
        pinit = [1.0, 0.0]
        self.pfit, self.covar_fit = calc_transform(
            pinit,
            self.inst_mag[self.calib_stars],
            self.gaia_catalog['imag'][self.calib_stars]
        )
        lcologs.log(
            'Photometric calibration parameters: ' + repr(self.pfit) + ', ' + repr(self.covar_fit),
            'info',
            log=self.log
        )


    def identify_ref_image(self):
        """
        Method to identify the image from a single-filter dataset to be used as the
        reference.

        The selection can be controlled from the configuration file, with the options:
        photometry:
            reference_image: 'auto', '<image name>', 'none'
                none: Switch off photometric calibration and deliver only instrumental values
                auto: Select the best available reference image from the dataset
                <image name>: Name of a specific image in the observation set to use as reference

        :return:
            Sets ref_image attribute to the name of a specific image from the obs_set
            Sets ref_image_index attribute to the index of the referene image in the arrays
        """

        match self.config['photometry']['reference_image']:
            # Case 1: Configuration includes specific match subject:
            case 'none':
                self.ref_image = None
                lcologs.log(
                    'Photometric calibration switched OFF in the configuration',
                    'warning',
                    log=self.log
                )

            # Case 2: Select the best-available reference image from the obs_set
            # Currently not yet implemented
            case 'auto':
                self.ref_image = None
                lcologs.log(
                    'Auto selection of photometric reference image not yet implemented',
                    'warning',
                    log=self.log
                )

            # Case 3: No exact match to string, check for specified image name
            case _:
                if self.config['photometry']['reference_image'] in self.obs_set['file']:
                    self.ref_image = self.config['photometry']['reference_image']
                    lcologs.log(
                        'Photometric calibration will use pre-selected image ' + self.ref_image,
                        'info',
                        log=self.log
                    )

                else:
                    self.ref_image = None
                    lcologs.log(
                        'Cannot find pre-selected image ' + self.ref_image + ' to use as reference'
                        'error',
                        log=self.log
                    )

        # Identify the index of the reference image in the arrays
        if self.ref_image:
            self.ref_image_index = np.where(obs_set['file'] == self.ref_image)[0][0]
            lcologs.log(
                'Reference image index = ' + self.ref_image_index,
                log=self.log
            )

    def select_calib_stars(self):
        """
        Function to select a set of stars for the photometric calibration which have valid measurements
        in both the catalog and the chosen reference image.  Valid measurements are magnitudes that
        lie between upper and lower magnitude limits for both catalog and reference image.

        :return:
            Sets self.calib_stars   list  Boolean mask of star index in arrays
        """

        idx1 = self.gaia_catalog['imag'] > 10.0
        idx2 = self.gaia_catalog['imag'] <= 17.5
        idx3 = self.inst_mag > 12.0
        idx4 = self.inst_mag <= 20.0
        self.calib_stars = idx1 & idx2 & idx3 & idx4

    def calibrate_timeseries(self):
        """
        Method to apply the photometric calibration transform to timeseries arrays of the flux and
        flux uncertainties of all stars from a set of images.

        :return:
        :param cal_mag: Array Timeseries calibrated magnitudes
        :param err_cal_mag: Array Timeseries calibrated magnitude uncertainties
        """

        cal_mag = np.zeros(self.flux.shape)
        err_cal_mag = np.zeros(self.err_flux.shape)

        for i in range(0, self.flux.shape[1], 1):
            inst_mag, inst_err_mag, _, _ = conversions.flux_to_mag(self.flux[:, i], self.err_flux[:, i])
            cal_mag[:, i], err_cal_mag[:, i] = calc_calibrated_mags(
                self.pfit, self.covar_fit, inst_mag, inst_err_mag
            )

        return cal_mag, err_cal_mag

def phot_func(p,mags):
    """Photometric transform function"""
    # Expected function is of the form p[0]*mags + p[1]
    if len(p) == 2:
        return np.polyval(p,mags)
    else:
        raise IndexError('Photometric transform called with an unexpected number of terms')

def errfunc(p,x,y):
    """Function to calculate the residuals on the photometric transform"""

    return y - phot_func(p,x)

def calc_transform(pinit, x, y):
    """Function to calculate the photometric transformation between a set
    of catalogue magnitudes and the instrumental magnitudes for the same stars
    """

    #(pfit,iexec) = optimize.leastsq(errfunc,pinit,args=(x,y))
    #(pfit,covar_fit) = np.polyfit(x,y,1,cov=True)
    linear_model = Model(phot_func)
    dataset = Data(x, y)
    odr_obj = ODR(dataset, linear_model, beta0=pinit)
    results = odr_obj.run()

    pfit = [results.beta[0], results.beta[1]]
    covar_fit = results.cov_beta*results.res_var

    return pfit, covar_fit

def calc_transform_uncertainty(pfit, x, y):
    """Function to calculate the uncertainty on the calibrated magnitudes,
    based on the scatter of datapoints around the fitted model
    See: http://123.physics.ucdavis.edu/week_0_files/taylor_181-199.pdf
    """

    y2 = phot_func(pfit,x)
    delta = (y - y2)**2
    sigma_y2 = np.sqrt(delta.sum()/float(len(delta)-2))

    return sigma_y2


def calc_calibrated_mags(fit_params, covar_fit, mag, err_mag):
    ''' In this function, we propagate uncertainties of the mag calibration. The formula is:

        cal_mag = a*mag+b

        Therefore, the covariance matrix and Jacobian are:

        C = |sig_a**2 sig_ab 0|
            |sig_ab sig_b**2 0|
            |0       0 e_mag**2|

        J = [mag,1,a]

        and then

        e_cal_mag = J.T C J


        HOWEVER

        for computing reason, we implemented a modified version with identical result:

        C' = |sig_a**2 sig_ab 0|
             |sig_ab sig_b**2 0|
             |0       0 a**2|

        J' = [mag,1,e_mag]

        and

        e_cal_mag = J'.T C' J'
    '''

    ccalib = np.eye(3)
    ccalib[:2, :2] = covar_fit
    ccalib[2, 2] = fit_params[0] ** 2

    jac = np.c_[mag, [1] * len(mag), err_mag]
    cal_mag = phot_func(fit_params, mag)

    # OPTIMIZE THIS WITH MATRIX MATH
    errors = []
    for i in range(len(jac)):
        vect = []
        for j in range(len(ccalib)):
            vect.append(np.sum(ccalib[j] * jac[i]))
        errors.append(np.sum(vect * jac[i]) ** 0.5)

    err_cal_mag = np.array(errors)

    idx = np.where(mag < 7.0)
    cal_mag[idx] = 0.0
    err_cal_mag[idx] = 0.0

    return cal_mag, err_cal_mag
