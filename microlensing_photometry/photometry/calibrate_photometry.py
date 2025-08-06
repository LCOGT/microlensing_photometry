from microlensing_photometry.infrastructure import logs as lcologs

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

        # Extract photometry from reference image and catalog for the same
        # set of stars

        # Perform model fit to determine the transformation from instrumental to
        # calibrated photometry

        # Compose output arrays

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
                    self.red_image = None
                    lcologs.log(
                        'Cannot find pre-selected image ' + self.ref_image + ' to use as reference'
                        'error',
                        log=self.log
                    )