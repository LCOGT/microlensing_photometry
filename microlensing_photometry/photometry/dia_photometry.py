from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
from photutils.aperture import aperture_photometry
from photutils.aperture import CircularAnnulus, CircularAperture
from photutils.aperture import ApertureStats
import numpy as np
from skimage.measure import ransac
from skimage import transform as tf
from scipy import ndimage

from microlensing_photometry.infrastructure import logs as lcologs
from microlensing_photometry.photometry import aperture_photometry as lcoaphot

class DIAPhotometryAnalyst(object):
    """
    Class of worker to perform DIA photometry for one image

    Attributes
    ----------

    reference_name : str, a name to the reference
    reference_path : str, a path to the reference
    image_name : str, a name to the data
    image_path : str, a path+name to the data
    reference_catalog : astropy.Table, the star catalog of the reference
    image_catalog : astropy.Table, the star catalog of the image
    cutout_region : list, [RA,DEC,pixel_size] the size in pixels of the cutout image around RA,DEC
    kernel_size: int, the size of the numerical kernel use


    """

    def __init__(self, reference_name, reference_path,image_name, image_path, reference_catalog, image_catalog,
                 cutout_region, kernel_size,log_path='./logs'):

        self.log = lcologs.start_log(log_path, image_name)
        self.log.info('Initialize DIA Photometry Analyst')

        self.reference_path = reference_path+reference_name
        self.reference_layers = fits.open(self.reference_path)
        self.reference_image = self.reference_layers[0].data
        self.reference_wcs = WCS(self.reference_layers[-2].header)
        self.log.info('Reference found and open successfully!')

        self.image_path = image_path+image_name
        self.image_layers = fits.open(self.image_path)
        self.image = self.image_layers[0].data

        try:
            self.errors = self.image_layers[3].data
        except:
            self.errors = np.abs(self.image)**0.5
        try:
            self.fwhm = self.image_layers[0].header['L1FWHM']
        except:
            self.fwhm = 5

        try:
            self.reference_mask = self.reference_layers[2].data
        except:
            self.reference_mask = np.zeros(self.reference_image.shape)
        try:
            self.image_mask = self.image_layers[2].data
        except:
            self.image_mask = np.zeros(self.image.shape)


        self.image_wcs = WCS(self.image_layers[-2].header)
        self.log.info('Reference found and open successfully!')

        self.ref_catalog = reference_catalog
        self.image_catalog = image_catalog

        ra, dec, size = cutout_region
        self.ra = ra
        self.dec = dec
        self.size = size
        self.kernel_size = kernel_size

        self.process_image()

        #self.update_image_with_new_layers()
        lcologs.close_log(self.log)

    def process_image(self):
        """
        Process the image following the various steps
        """
        self.log.info('Start Image Processing')

        self.cut_image_and_ref()
        self.align_image_to_ref()
        self.run_dia_photometry()

    def cut_image_and_ref(self,):
        """
        Cut the image and ref around the ra,dec center
        """
        ra, dec, size, kernel_size = self.ra,self.dec,self.size,self.kernel_size
        kernel_size = int(kernel_size / 2)

        coo2 = SkyCoord(ra=ra,dec=dec,unit='deg')
        xx,yy = self.reference_wcs.world_to_pixel(coo2)

        self.origin_ref_x = xx - size / 2
        self.origin_ref_y = yy - size / 2

        self.cutout_reference = self.reference_image[int(int(yy) - size / 2 - kernel_size):int(int(yy) + size / 2 + 1 + kernel_size),
                     int(int(xx) - size / 2 - kernel_size):int(int(xx) + size / 2 + 1 + kernel_size)]

        self.cutout_reference_mask = self.reference_mask[
                                int(int(yy) - size / 2 - kernel_size):int(int(yy) + size / 2 + 1 + kernel_size),
                                int(int(xx) - size / 2 - kernel_size):int(int(xx) + size / 2 + 1 + kernel_size)]

        mask = (np.abs(self.ref_catalog['xcenter'] - xx) < size / 2 ) & (np.abs(self.ref_catalog['ycenter'] - yy) < size / 2 )
        self.ref_catalog_mask = mask

        xx, yy = self.image_wcs.world_to_pixel(coo2)

        self.origin_image_x = xx - size / 2
        self.origin_image_y = yy - size / 2

        self.cutout_image = self.image[
                            int(int(yy) - size / 2 - kernel_size):int(int(yy) + size / 2 + 1 + kernel_size),
                            int(int(xx) - size / 2 - kernel_size):int(int(xx) + size / 2 + 1 + kernel_size)]

        self.cutout_mask = self.image_mask[
                            int(int(yy) - size / 2 - kernel_size):int(int(yy) + size / 2 + 1 + kernel_size),
                            int(int(xx) - size / 2 - kernel_size):int(int(xx) + size / 2 + 1 + kernel_size)]


        self.cutout_errors = self.errors[
                        int(int(yy) - size / 2 - kernel_size):int(int(yy) + size / 2 + 1 + kernel_size),
                        int(int(xx) - size / 2 - kernel_size):int(int(xx) + size / 2 + 1 + kernel_size)]

        indi,indj = build_the_U_indexes(self.cutout_image, np.eye(self.kernel_size))

        self.indi = indi
        self.indj = indj

    def align_image_to_ref(self):
        """Perform image alignement to the reference"""
        matching = []

        for inde, i in enumerate(self.ref_catalog['id'][self.ref_catalog_mask]):

            if i in self.image_catalog['id']:

                index = np.where(self.image_catalog['id'] == i)[0][0]

                matching.append([inde, index])

            else:

                pass

        matching = np.array(matching)

        pts1 = np.c_[[self.ref_catalog['xcenter'][self.ref_catalog_mask] - self.origin_ref_x,
                      self.ref_catalog['ycenter'][self.ref_catalog_mask] - self.origin_ref_y]].T[matching[:, 0]]

        pts2 = np.c_[[self.image_catalog['xcenter'] - self.origin_image_x,
                      self.image_catalog['ycenter'] - self.origin_image_y]].T[matching[:, 1]]

        model_robust, inliers = ransac((pts2[:1000], pts1[:1000]), tf.AffineTransform,
                                       min_samples=int(0.5 * len(pts1[:1000])), residual_threshold=0.01, max_trials=300)

        aligned_image = tf.warp(self.cutout_image.astype(float),
                                model_robust.inverse,
                                output_shape=self.cutout_image.shape, order=3)
        aligned_mask = tf.warp(self.cutout_mask.astype(float), model_robust.inverse,
                               output_shape=self.cutout_image.shape, order=1)
        aligned_errors = tf.warp(self.cutout_errors.astype(float)**2,
                                 model_robust.inverse,
                                 output_shape=self.cutout_image.shape, order=3)

        self.cutout_aligned_image = aligned_image
        self.cutout_aligned_mask = aligned_mask
        self.cutout_aligned_errors = aligned_errors**0.5


    def run_dia_photometry(self):
        """
        Run DIA photometry on the image using the star catalog of Gaia for time been.
        """

        try:

            kernel_size = int(self.kernel_size / 2)
            mask = self.cutout_aligned_mask.astype(bool) | self.cutout_reference_mask.astype(bool)

            dia_image,image_model,dia_mask, kernel,bkg_coeffs,kernel_errors = (
                run_difference_image(self.cutout_reference, self.cutout_aligned_image,
                                     self.kernel_size,mask = mask,
                                     indi=self.indi, indj=self.indj))

            positions = np.c_[[self.ref_catalog['xcenter'][self.ref_catalog_mask] - self.origin_ref_x,
              self.ref_catalog['ycenter'][self.ref_catalog_mask] - self.origin_ref_y]].T

            #phot_table = lcoaphot.run_aperture_photometry(dia_image,
            #                                              np.abs(
            #                                              self.cutout_aligned_errors[kernel_size:-kernel_size, kernel_size:-kernel_size] ),
            #                                              positions, self.fwhm)

            phot_table = run_dia_photometry(dia_image,np.abs(self.cutout_aligned_errors[kernel_size:-kernel_size, kernel_size:-kernel_size] ),
                                            positions, self.fwhm)
            phot_table['id'] = self.ref_catalog['id'][self.ref_catalog_mask]

            self.dia_photometry = phot_table
            self.kernel = kernel
            self.bkg_coeffs = bkg_coeffs
            self.kernel_errors = kernel_errors
            #self.bkg_coeffs_errors =
            self.image_model = image_model
            self.dia_image = dia_image
            self.dia_mask = dia_mask

            self.log.info('DIA Photometry successfully estimated')

        except Exception as error:

            self.log.info('Problems with the DIA photometry: aboard DIA Photometry! Details below')
            self.log.error(f"DIA Photometry Error: %s, %s" % (error, type(error)))

    def update_image_with_new_layers(self):

            update_wcs_layer = fits.PrimaryHDU(header=self.image_header)
            update_wcs_layer.header['EXTNAME'] = 'LCO MICROLENSING UPDATED WCS'
            update_wcs_layer.header.update(self.image_new_wcs.to_header())

            try:

                self.image_layers[4] = update_wcs_layer

            except:

                self.image_layers.append(update_wcs_layer)

            aperture_photometry_table = fits.BinTableHDU(data=self.aperture_photometry_table)
            aperture_photometry_table.header['EXTNAME'] = 'LCO MICROLENSING APERTURE PHOTOMETRY'

            try:

                self.image_layers[5] = aperture_photometry_table

            except:

                self.image_layers.append(aperture_photometry_table)

            self.image_layers.writeto(self.image_path,overwrite=True)


def run_difference_image(reference_image, aligned_image, kernel_size, mask=None, error=None,indi=None, indj=None):
    """
        Difference image, given an aligned image to a reference.

        Parameters
        ----------
        reference_image : array, the reference data
        aligned_image: array, the image aligned to the data
        kernel_size: int, the size of the kernel in pixels
        mask : array, a boolean of data to ignore (i.e. 1 = ignored)
        error : array, the error data (2D)
        indi: array, the indexes in i for the U matrix construction
        indj: array, the indexes in j for the U matric construction

        Returns
        -------
        dia_image : array, the difference image
        image_model : array, the model of the image
        kernel: array, the estimated kernel
        bkg_coeffs: array, the estimated bkg coefficients (polynomial)
        kernel_errors: array, the errors on the kernel estimate
    """
    if mask is None:

        mask = np.zeros(reference_image.shape)

    if error:

        noise = error

    else:

        noise = np.ones(reference_image.shape)

    #Extend the mask a bit
    mask = ndimage.binary_dilation(mask, structure=np.ones((3, 3)).astype(bool),
                                   iterations=15).astype(bool)

    if (indi is not None) & (indj is not None):

        pass

    else:

        indi,indj = build_the_U_indexes(reference_image, np.eye(kernel_size))

    kernel_size = int(kernel_size / 2)

    Umatrix = ((reference_image-reference_image.mean()) / noise)[indi, indj]
    Umatrix2 = Umatrix.copy()
    Umatrix2[mask[indi, indj]] = 0

    tofit = ((aligned_image - aligned_image.mean()) / noise)
    tofit[mask] = 0

    ones = (np.ones(tofit.shape) / noise)
    ones[mask] = 0

    Y, X = np.indices(tofit.shape)
    xxx = (X / noise)
    xxx[mask] = 0

    yyy = (Y / noise)
    yyy[mask] = 0

    bkg_coeffs = np.c_[ones[kernel_size:-kernel_size,
                       kernel_size:-kernel_size].ravel(),
    xxx[kernel_size:-kernel_size, kernel_size:-kernel_size].ravel(),
    yyy[kernel_size:-kernel_size, kernel_size:-kernel_size].ravel()]
    #bkg_coeffs = ones[kernel_size:-kernel_size,kernel_size:-kernel_size].ravel()
    bigU = np.c_[Umatrix2, bkg_coeffs]
    solution = np.linalg.lstsq(bigU,
                               tofit[kernel_size:-kernel_size, kernel_size:-kernel_size].ravel())
    #breakpoint()
    if np.any(np.isnan(solution[0])):
        breakpoint()

    #model = ((((Umatrix @ solution[0][:-3]).reshape(
    #    reference_image[kernel_size:-kernel_size, kernel_size:-kernel_size].shape) +
    #           solution[0][-3] + X[kernel_size:-kernel_size,
    #    #           kernel_size:-kernel_size] * solution[0][-2]) +
    #          Y[kernel_size:-kernel_size, kernel_size:-kernel_size] * solution[0][
    #    #          -1]) +
    #         aligned_image.mean())

    model = (np.c_[Umatrix, bkg_coeffs] @ solution[0]).reshape(
        reference_image[kernel_size:-kernel_size,
        kernel_size:-kernel_size].shape) + aligned_image.mean()


    residus = aligned_image[kernel_size:-kernel_size, kernel_size:-kernel_size] - model
    #residus[mask[kernel_size:-kernel_size, kernel_size:-kernel_size]] = 0

    kernel = np.flip(solution[0][:-3].reshape((2*kernel_size+1, 2*kernel_size+1)))
    bkg_coeffs = solution[0][-3:]

    cov = np.linalg.pinv(Umatrix.T @ Umatrix)
    chisq = np.sum(residus ** 2)
    cov *= chisq / (len(tofit.ravel()) - len(kernel.ravel()))

    kernel_errors = np.flip((cov.diagonal() ** 0.5).reshape(kernel.shape))
    # bkg_coeffs_errors = TODO
    image_model = model
    dia_image = residus
    dia_mask = mask

    #import scipy.signal as ss

    #model2 = ss.fftconvolve(reference_image - reference_image.mean(),kernel,
    #                         mode='same' )
    #breakpoint()

    return dia_image,image_model,dia_mask,kernel,bkg_coeffs,kernel_errors

def run_dia_photometry(image, error, positions,radius):
    """
    DIA photometry on a image, using an error image, and fixed stars positions.

    Parameters
    ----------
    image : array, the image data
    error : array, the error data (2D)
    positions: array, [X,Y] positions of the stars to place aperture
    radius: float, the aperture radius use to extract flux

    Returns
    -------
    phot_table : astropy.Table, the photometric table
    """

    aperture = CircularAperture(positions, r=radius)
    #annulus_aperture = CircularAnnulus(positions, r_in=radius+3, r_out=radius+5)

    #aperstats = ApertureStats(image, annulus_aperture)

    #bkg_mean = aperstats.mean

    phot_table = aperture_photometry(image, aperture, error=error)
    #total_bkg = aperture.area * bkg_mean

    #phot_table['aperture_sum'] -= total_bkg
    #breakpoint()
    return phot_table




def build_the_U_matrix(X,K):
    """To save to make X,Y dependent kernel, might be of use later"""
    p, q = K.shape
    m, n = X.shape
    U = np.zeros(((n - q + 1) ** 2, q ** 2))
    #YY, XX = np.indices(X.shape)
    #UX = np.zeros(U.shape)
    #UY = np.zeros(U.shape)
    #UXX = np.zeros(U.shape)
    #UYY = np.zeros(U.shape)
    #UXY = np.zeros(U.shape)
    indi = np.zeros(U.shape)
    indj = np.zeros(U.shape)

    iii, jjj = np.indices(X.shape)

    line = 0
    for i in range(n - q + 1):
        for j in range(n - q + 1):
            U[line] = X[i:i + q, j:j + q].ravel()  # *MASK[i:i+q,j:j+q].ravel()
            #UX[line] = xx[i:i + q, j:j + q].ravel() * XX[i:i + q, j:j + q].ravel()  # *MASK[i:i+q,j:j+q].ravel()
            #UY[line] = xx[i:i + q, j:j + q].ravel() * YY[i:i + q, j:j + q].ravel()  # *MASK[i:i+q,j:j+q].ravel()
            #UXX[line] = xx[i:i + q, j:j + q].ravel() * XX[i:i + q, j:j + q].ravel() ** 2  # *MASK[i:i+q,j:j+q].ravel()
            #UYY[line] = xx[i:i + q, j:j + q].ravel() * YY[i:i + q, j:j + q].ravel() ** 2  # *MASK[i:i+q,j:j+q].ravel()
            #UXY[line] = xx[i:i + q, j:j + q].ravel() * XX[i:i + q, j:j + q].ravel() * YY[i:i + q,
            #                                                                          j:j + q].ravel()  # *MASK[i:i+q,j:j+q].ravel()

            indi[line] = iii[i:i + q, j:j + q].ravel()
            indj[line] = jjj[i:i + q, j:j + q].ravel()
            line += 1

    return U, indi.astype(int), indj.astype(int)


def build_the_U_indexes(reference_image, kernel):
    """
    Construct the indi,inj indexes to build quickly the umatrix for kernel solution
    Parameters
    ----------
    image : array, the image data
    error : array, the error data (2D)
    positions: array, [X,Y] positions of the stars to place aperture
    radius: float, the aperture radius use to extract flux

    Returns
    -------
    phot_table : astropy.Table, the photometric table
    """
    p, q = kernel.shape
    m, n = reference_image.shape
    iii, jjj = np.indices(reference_image.shape)

    indi = np.zeros(((n - q + 1) ** 2, q ** 2))
    indj = np.zeros(((n - q + 1) ** 2, q ** 2))

    line = 0
    for i in range(n - q + 1):
        for j in range(n - q + 1):

            indi[line] = iii[i:i + q, j:j + q].ravel()
            indj[line] = jjj[i:i + q, j:j + q].ravel()
            line += 1

    return indi.astype(int), indj.astype(int)

