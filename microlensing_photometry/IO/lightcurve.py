from os import path
from microlensing_photometry.photometry import aperture_photometry
import microlensing_photometry.logistics.GaiaTools.GaiaCatalog as GC
import argparse
from astropy.coordinates import SkyCoord
from astropy import units as u

def aperture_timeseries(args):
    """
    Function to plot an aperture photometry timeseries from an HDF5 output file

    Parameters
    ----------
    args    object      Program arguments

    Outputs
    -------
    ASCII format lightcurve file
    """

    # Load the photometry dataset
    dataset = aperture_photometry.AperturePhotometryDataset(file_path=args.in_path)

    # Target coordinates can be in sexigesimal or decimal degree format, so handle both
    try:
        target_ra = float(args.target_ra)
        target_dec = float(args.target_dec)

        target = SkyCoord(target_ra, target_dec, frame='icrs', unit=(u.deg, u.deg))
    except ValueError:
        target = SkyCoord(args.target_ra, args.target_dec, frame='icrs', unit=(u.hourangle, u.deg))
        target_ra = target.ra
        target_dec = target.dec

    # Search the catalog for the nearest entry
    star_idx, entry = GC.find_nearest(dataset.source_wcs, target_ra, target_dec)

    # If a valid entry exists, extract the lightcurve and output
    if entry:
        lc = dataset.get_lightcurve(star_idx)

        lc.write(args.out_path, format='ascii', overwrite=True)

        print('Output lightcurve data to ' + args.out_path)

    else:
        print('No matching star found in source catalog')

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('in_path', help='Path to aperture photometry HDF5 file')
    parser.add_argument('target_ra', help='RA of target star in degrees')
    parser.add_argument('target_dec', help='Dec of target star in degrees')
    parser.add_argument('out_path', help='Path to output lightcurve file')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()
    aperture_timeseries(args)