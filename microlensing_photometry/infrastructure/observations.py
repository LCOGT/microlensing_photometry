import os
import copy
import argparse
from astropy.io import fits
from microlensing_photometry.infrastructure import data_classes

def get_observation_metadata(args):
    """
    Function to review all available observations in a single dataset and extract
    header information necessary for the reduction.

    :param args:
    :return: ObservationSet object
    """

    # Load an existing observation set summary table if there is one; otherwise return an empty table
    obs_set_file = os.path.join(args.directory, 'data_summary.txt')
    obs_set = data_classes.ObservationSet(file_path=obs_set_file)

    # List all observations in the reduction directory
    obs_list = [i for i in os.listdir(args.directory) if ('.fits' in i) & ('.fz' not in i)]

    # Review all observations in the list and extract header data where necessary
    for file_path in obs_list:
        if os.path.basename(file_path) not in obs_set.table['file']:

            # A deepcopy of the header object is taken so that the file itself can be properly
            # closed at the end of the function; this matters with large datasets.  Astropy
            # does not close the FITS HDU if pointers remain to stored header items.
            hdr = fits.getheader(os.path.join(args.directory, file_path))
            hdr0 = copy.deepcopy(hdr)

            obs_set.add_observation(file_path, hdr0)

    # Store summary of the dataset information
    obs_set.save(obs_set_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', help='Path to input data directory')
    args = parser.parse_args()

    get_observation_metadata(args)