from scipy.spatial import cKDTree
import numpy as np


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
