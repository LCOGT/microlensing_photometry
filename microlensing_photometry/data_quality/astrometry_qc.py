import numpy as np

def check_stars_within_frame(image_shape, star_positions):
    """
    Function to verify that the set of star pixel positions calculated from the image WCS
    actually lie within the frame boundaries.  This is a simple but effective test of
    whether the image_wcs parameters are reasonable.

    :param image_shape: Tuple of NAXIS1, NAXIS2 => max_y, max_x
    :param star_positions: Numpy array of star pixel x,y positions
    :return: Boolean
    """

    idx1 = np.where(star_positions[:,0] > 0)[0]
    idx2 = np.where(star_positions[:,0] < image_shape[1])[0]
    idx3 = np.where(star_positions[:,1] > 0)[0]
    idx4 = np.where(star_positions[:,1] < image_shape[0])[0]
    idx = set(idx1).intersection(set(idx2)).intersection(set(idx3)).intersection(set(idx4))

    if len(idx) > 0:
        return True
    else:
        return False