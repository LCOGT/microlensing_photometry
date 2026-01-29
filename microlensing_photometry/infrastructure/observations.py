import os
import copy
import argparse
from astropy.io import fits
from microlensing_photometry.infrastructure import data_classes
from microlensing_photometry.infrastructure import logs as lcologs

def get_observation_metadata(args, log=None):
    """
    Function to review all available observations in a single dataset and extract
    header information necessary for the reduction.

    :param args:
    :return: ObservationSet object
    """

    # Load an existing observation set summary table if there is one; otherwise return an empty table
    obs_set_file = os.path.join(args.directory, 'data_summary.txt')
    obs_set = data_classes.ObservationSet(file_path=obs_set_file, log=log)

    # List all observations in the reduction directory
    obs_list = [i for i in os.listdir(args.directory) if ('.fits' in i) & ('.fz' not in i)]
    lcologs.log(
        'Identified ' + str(len(obs_list)) + ' observations in ' + args.directory,
        'info',
        log=log
    )

    # Review all observations in the list and extract header data where necessary
    for file_name in obs_list:
        if file_name not in obs_set.table['file']:
            file_path = os.path.join(args.directory, file_name)

            # A deepcopy of the header object is taken so that the file itself can be properly
            # closed at the end of the function; this matters with large datasets.  Astropy
            # does not close the FITS HDU if pointers remain to stored header items.
            with fits.open(file_path) as hdul:
                # Get the PrimaryDU header
                hdr0 = copy.deepcopy(hdul[0].header)

                # If this image has been photometered before there will also be a table extension
                # with an updated WCS.  If this is the case, load the WCS parameters from that table
                for hdu in hdul:
                    if hdu.header['EXTNAME'] == 'LCO MICROLENSING PHOTOMETRY UPDATED WCS':
                        hdr0 = update_wcs_parameters(hdu.header, hdr0)

                obs_set.add_observation(file_path, hdr0)

            # Ensure FITS files close properly
            hdul.close()
            del hdul

    # Store summary of the dataset information
    obs_set.save(obs_set_file, log=log)

    return obs_set

def update_wcs_parameters(src_hdr, dest_hdr):
    """
    Function transfers the WCS header keyworks from the src to the dest header objects

    :param src_hdr: Source FITS header object for the WCS parameters to be copied
    :param dest_hdr: Destination FITS header object to be updated
    :return: dest_hdr
    """

    dest_hdr['CTYPE1'] = copy.deepcopy(src_hdr['CTYPE1'])
    dest_hdr['CTYPE2'] = copy.deepcopy(src_hdr['CTYPE2'])
    dest_hdr['CRPIX1'] = copy.deepcopy(src_hdr['CRPIX1'])
    dest_hdr['CRPIX2'] = copy.deepcopy(src_hdr['CRPIX2'])
    dest_hdr['CRVAL1'] = copy.deepcopy(src_hdr['CRVAL1'])
    dest_hdr['CRVAL2'] = copy.deepcopy(src_hdr['CRVAL2'])
    dest_hdr['CUNIT1'] = copy.deepcopy(src_hdr['CUNIT1'])
    dest_hdr['CUNIT2'] = copy.deepcopy(src_hdr['CUNIT2'])
    dest_hdr['CD1_1'] = copy.deepcopy(src_hdr['PC1_1'])
    dest_hdr['CD1_2'] = copy.deepcopy(src_hdr['PC1_2'])
    dest_hdr['CD2_1'] = copy.deepcopy(src_hdr['PC2_1'])
    dest_hdr['CD2_2'] = copy.deepcopy(src_hdr['PC2_2'])
    dest_hdr['WCSERR'] = 0

    return dest_hdr

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', help='Path to input data directory')
    args = parser.parse_args()

    obs_set = get_observation_metadata(args)