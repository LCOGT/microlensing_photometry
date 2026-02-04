from prefect import task
import numpy as np

from image_reduction.photometry import psf

@task
def build_image(star_positions, fluxes, image_shape, image_fraction = 0.25, star_limit = 1000):
    """
    Construct an image with fake stars on top of a null background. Only the star inside the image fraction are computed

    Parameters
    ----------
    image : array, the image to refine the WCS solution
    stars_image : array, the x,y positions of stars in the image
    image_wcs : astropy.wcs, the original astropy WCS solution
    gaia_catalog : astropy.Table, the entire gaia catalog
    star_limit : int, the limit number of stars to use

    Returns
    -------
    model_image : array, a model image of the field
    """

    leny, lenx = (np.array(image_shape) * image_fraction).astype(int)
    mask = ((np.abs(star_positions[:,1] - image_shape[0] / 2) < leny)
            & (np.abs(star_positions[:,0] - image_shape[1] / 2) < lenx))

    sub_star_positions = np.array(star_positions)[mask][:star_limit]

    XX, YY = np.indices((21, 21))

    model_gaussian = psf.Gaussian2d(1, XX.max()/2, XX.max()/2, 3, 3, XX, YY)
    model_image = np.zeros(image_shape)

    for ind in range(len(sub_star_positions)):

        try:

            model_image[sub_star_positions[ind,1].astype(int) - int(XX.max()/2):sub_star_positions[ind,1].astype(int) + int(XX.max()/2+1),
            sub_star_positions[ind,0].astype(int) - int(XX.max()/2):sub_star_positions[ind,0].astype(int) + int(XX.max()/2+1)] += model_gaussian * fluxes[ind]

        except:

            # skip borders
            pass

    return model_image
