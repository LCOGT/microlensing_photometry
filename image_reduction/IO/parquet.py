import os
import numpy as np
import pyarrow.parquet as pq
import pandas as pd
import image_reduction.infrastructure.logs as lcologs
from astropy.table import Table, Column

def make_output_directories(red_dir_path, log=None):
    """
    Function to create the subdirectories needed to store output in parquet format

    :param red_dir_path: str Path to reduction directory

    Returns:
        Created subdirectories if they don't already exist
    """

    sub_dirs = [
        os.path.join(red_dir_path, 'raw_flux'),
    ]

    for dir_path in sub_dirs:
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)

    lcologs.log('Verified output directories exist','info', log=log)

def load_raw_flux(red_dir_path):
    """
    Function to load the raw_flux parquet files for all images into numpy arrays

    :param red_dir_path: str Path to reduction directory

    Returns
    :param raw_flux: Table  Full table of source photometry output
    """

    # Read the raw flux data from separate parquet files for each image
    dir_path = os.path.join(red_dir_path, "raw_flux")
    flux_arrow = pq.read_table(dir_path)
    raw_flux = Table.from_pandas(flux_arrow.to_pandas())

    return raw_flux

def load_norm_flux(red_dir_path):
    """
    Function to load the normalized flux data

    :param red_dir_path: str  Path to reduction directory

    Returns
    :param flux:  arr  Table of flux data for all images
    :param flux_err: arr Table of uncertainties of flux data
    """
    file_path = os.path.join(red_dir_path, 'aperture_photometry.parquet')
    data = Table.read(file_path)

    # Flux measurements and uncertainties are horizontally concatenated in the table;
    # separate them into separate Tables
    flux_cols = [col for col in data.colnames if '_err_' not in col]
    flux_err_cols = [col for col in data.colnames if '_err_' in col]

    return data[flux_cols].to_pandas().to_numpy(), data[flux_err_cols].to_pandas().to_numpy()

def output_norm_flux(red_dir_path, dataset):
    """
    Function to output the normalized flux in parquet format

    :param red_dir_path: str Path to reduction directory
    :param dataset: object AperturePhotometryDataset
    """

    df = pd.DataFrame({
        'file': dataset.file,
        'timestamps': dataset.timestamps,
        'pscales': dataset.pscales[1],
        'epscales': dataset.epscales,
    })
    file_path = os.path.join(red_dir_path, 'image_data.parquet')
    df.to_parquet(file_path, engine='pyarrow')

    flux_table = Table(dataset.flux)
    colnames = ['flux_'+str(i) for i in range(dataset.flux.shape[1])]
    flux_table.rename_columns(flux_table.colnames, colnames)

    flux_err_table = Table(dataset.flux_err)
    colnames = ['flux_err_'+str(i) for i in range(dataset.flux.shape[1])]
    flux_err_table.rename_columns(flux_err_table.colnames, colnames)

    df = pd.concat([flux_table.to_pandas(), flux_err_table.to_pandas()], axis=1)

    file_path = os.path.join(red_dir_path, 'aperture_photometry.parquet')
    df.to_parquet(file_path, engine='pyarrow')
