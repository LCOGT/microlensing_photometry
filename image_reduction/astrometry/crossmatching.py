from scipy.spatial import cKDTree
import numpy as np
from astropy.coordinates import SkyCoord
from astropy import units as u

import image_reduction.infrastructure.logs as lcologs

def merge_positions(positions1, positions2, tolerance=1.0):
    """
    Merge two arrays of (x, y) coordinates, removing duplicates within
    a given spatial tolerance.

    The first set of positions passed should always be the reference catalog,
    as all objects within this catalog are retained at the top of the output array.
    Objects from positions2 that are not present in positions1 are appended
    at the end of the array.

    Parameters
    ----------
    positions1 : array-like, shape (N, 2)
    positions2 : array-like, shape (M, 2)
    tolerance  : float, max distance to consider two points the same target

    Returns
    -------
    merged : np.ndarray, shape (K, 2)  Combined catalog of positions
    unique_index: np.ndarray, shape (K, 1)  Indices of addition entries added from positions2
    """
    positions1 = np.array(positions1)
    positions2 = np.array(positions2)

    # Build a KD-tree from positions1
    tree = cKDTree(positions1)

    # For each point in positions2, find the nearest neighbour in positions1
    distances, _ = tree.query(positions2, k=1)

    # Keep only points in positions2 that have no close match in positions1
    unique_mask = distances > tolerance
    unique_to_p2 = positions2[unique_mask]
    unique_index = np.where(unique_mask)

    # Combine positions1 (kept wholesale) with the non-duplicate points from positions2
    merged = np.vstack([positions1, unique_to_p2])
    return merged, unique_index

def find_nearest(sources, ra, dec, radius=(2.0 / 3600.0) * u.deg, log=None):
    """
    Method to identify the nearest source catalog entry to the given coordinates,
    within a cut-off radius

    Parameters
    ----------
    sources Table  source catalog
    ra float    RA of location to search at [decimal deg]
    dec float   Dec of location to search at [decimal deg]
    radius float Search cut-off radius [decimal deg, default = 2 arcsec]

    Returns
    -------
    star_idx int    Index of star within the catalog
    closest match   catalog Table row for the closest match or None if no object is
                    within the search radius
    """

    sources = SkyCoord(sources['ra'], sources['dec'], frame='icrs', unit=(u.deg, u.deg))
    target = SkyCoord(ra, dec, frame='icrs', unit=(u.deg, u.deg))

    separations = target.separation(sources)

    idx = np.where(separations <= radius)[0]

    if len(idx) > 0:
        lcologs.log(
            'Found nearest matching star ' + str(idx[0]) + ' ' \
            + repr(sources[idx[0]]) + ', separation=' + str(separations[idx[0]]) + 'deg',
            'info',
            log=log
        )
        return idx[0], sources[idx[0]]

    else:
        lcologs.log(
            'No matching star found within search radius=' + str(radius) + ' deg',
            'warning',
            log=log
        )
        return None, None
