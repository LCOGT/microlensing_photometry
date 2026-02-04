import argparse
from os import path
from astropy.io import fits
import argparse
import glob

parser = argparse.ArgumentParser()
parser.add_argument('data_dir', help='Path to input data directory')
parser.add_argument('keyword', help='Photometry table keyword to add')
parser.add_argument('value', help='Value of keyword to add')
args = parser.parse_args()

frame_list = glob.glob(path.join(args.data_dir, '*fits'))

for frame in frame_list:
    with fits.open(frame) as hdul:

        table_index = -1

        i = 0
        while i < len(hdul) and table_index < 0:
            if hdul[i].name == 'LCO MICROLENSING APERTURE PHOTOMETRY':
                table_index = i
            i += 1

        if table_index > 0:
            print(hdul[table_index].header)
            breakpoint()