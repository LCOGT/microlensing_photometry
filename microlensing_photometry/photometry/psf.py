import numpy as np

def Gaussian2d(intensity,x_center,y_center,width_x,width_y,X_star,Y_star):
    """
    A 2d Gaussian model, to ingest stars in image model

    Parameters
    ----------
    intensity : float, the intensity of the gaussian
    x_center : float,  the gaussian center in x direction
    y_center : float, the gaussian center in y direction
    width_x : float, the gaussian sigma x
    width_y : float, the gaussian sigma y
    X_star : array, 2D  array containing the X value
    Y_star : array,  2D  array containing the Y value

    Returns
    -------
    model : array,  return the 2D gaussiam
    """

    model = intensity * np.exp(
            -(((X_star -x_center) / width_x) ** 2 + \
              ((Y_star - y_center) / width_y) ** 2) / 2)

    return model