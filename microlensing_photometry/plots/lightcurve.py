from os import path
from microlensing_photometry.photometry import aperture_photometry
import argparse

def aperture_timeseries(args):
    """
    Function to plot an aperture photometry timeseries from an HDF5 output file

    Parameters
    ----------
    args    object      Program arguments

    Returns
    -------

    """

    dataset = aperture_photometry.AperturePhotometryDataset(file_path=args.file_path)

    print(dataset.source_id)

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', help='Path to aperture photometry HDF5 file')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()
    aperture_timeseries(args)