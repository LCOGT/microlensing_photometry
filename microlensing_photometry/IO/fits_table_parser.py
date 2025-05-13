from astropy.table import Table, Column
import numpy as np

def fits_rec_to_table(fits_rec_array):
    """
    This module exists to handle the conversion from FITS binary table records
    to Astropy tables This is necessary because while the current Astropy has methods to convert
    existing Astropy tables to FITS record tables, it doesn't seem to have
    methods to reverse that conversion.  Providing the functionality here
    avoids having to re-load FITS tables from file

    Parameters
    ----------
    rec_array   FITS record array

    Returns
    -------
    data        Astropy Table
    """

    # This seems to be the fastest way to extract FITS_rec tuples
    # back to a 2D numpy array that can be manipulated by array slicing
    data = np.array(fits_rec_array.data.tolist())

    column_list = []
    for i,col in enumerate(fits_rec_array.columns):
        column_list.append(
            Column(name=col.name, data=data[:,i])
        )

    data = Table(column_list)

    return data

def find_phot_table(hdul, table_name):
    """
    Function to identify which HDUList extension holds the named table

    Parameters
    ----------
    hdul    Astropy HDUList (open)
    tabel_name str  Name of FITS extension to search for

    Returns
    -------
    table_index int     Index of the table in the HDUList or -1 if not present
    """

    table_index = -1

    i = 0
    while i < len(hdul) and table_index < 0:
        if hdul[i].name == table_name:
            table_index = i
        i += 1

    return table_index