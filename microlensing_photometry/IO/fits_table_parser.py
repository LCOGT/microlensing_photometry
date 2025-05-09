from astropy.io import fits
from astropy.table import Table, Column
from keyring.util.platform_ import data_root


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

    column_list = []
    for i,col in enumerate(fits_rec_array.columns):
        col_data = [row[i] for row in fits_rec_array.data]
        column_list.append(
            Column(name=col.name, data=col_data)
        )

    data = Table(column_list)

    return data
