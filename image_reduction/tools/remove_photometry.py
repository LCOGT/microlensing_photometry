import os
import sys
import copy
import argparse
from astropy.io import fits

def del_phot_extn(args):
    """
    Function to remove the WCS and photometry table extensions from a set of FITS images,
    deleting the products of an older reduction.

    :return: None
    """

    # List all uncompressed FITS files in the input directory
    obs_list = [i for i in os.listdir(args.directory) if ('.fits' in i) & ('.fz' not in i)]
    print('Removing old products from ' + str(len(obs_list)) + ' frames')

    # Load each FITS file in the directory, search for table extensions added by this pipeline
    # and remove them from the HDUList
    layer_names = ['LCO MICROLENSING PHOTOMETRY UPDATED WCS', 'LCO MICROLENSING APERTURE PHOTOMETRY']
    for frame in obs_list:
        file_path = os.path.join(args.directory, frame)
        with fits.open(file_path) as hdul:
            for i,hdu in enumerate(hdul):
                print(hdu.header['EXTNAME'])
                if hdu.header['EXTNAME'] in layer_names:
                    _ = hdul.pop(i)
                    hdul.flush()
                    print('Popped ', len(hdul))
            print(hdul, len(hdul))
            hdul.writeto(file_path, overwrite=True)
            print(' -> ' + frame)
def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('directory', help='Path to data directory')
    args = parser.parse_args()

    # Warn user this function is destructive
    opt = input('''
    WARNING!  This function will delete the products of previous data reductions.  
    Continue Y or any other key to abort
    ''')

    if opt != 'Y':
        sys.exit()

    return args


if __name__ == '__main__':
    args = get_args()
    del_phot_extn(args)