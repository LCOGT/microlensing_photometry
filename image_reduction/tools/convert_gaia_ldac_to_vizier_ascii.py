from astropy.table import Table, Column
import astropy.units as u
import argparse
import numpy as np
from image_reduction.photometry.conversions import mag_to_flux

def convert(args):

    data = Table.read(args.file_path, hdu=2)

    flux, flux_error = mag_to_flux(data['MAG'], data['MAGERR'])

    if args.type == 'DAT':
        sources = Table(
            [
                Column(name='ra', data=data['XWIN_WORLD'], unit=u.deg),
                Column(name='dec', data=data['YWIN_WORLD'], unit=u.deg),
                Column(name='source_id', data=np.arange(1, len(data)+1, 1)),
                Column(name='ra_error', data=data['ERRAWIN_WORLD'], unit=u.deg),
                Column(name='dec_error', data=data['ERRBWIN_WORLD'], unit=u.deg),
                Column(name='phot_g_mean_flux', data=flux),
                Column(name='phot_g_mean_flux_error', data=flux_error),
                Column(name='phot_bp_mean_flux', data=flux),
                Column(name='phot_bp_mean_flux_error', data=flux_error),
                Column(name='phot_rp_mean_flux', data=flux),
                Column(name='phot_rp_mean_flux_error', data=flux_error),
                Column(name='pm', data=np.zeros(len(data))),
                Column(name='pm_ra', data=np.zeros(len(data))),
                Column(name='pm_ra_error', data=np.zeros(len(data))),
                Column(name='pm_dec', data=np.zeros(len(data))),
                Column(name='pm_dec_error', data=np.zeros(len(data))),
                Column(name='parallax', data=np.zeros(len(data))),
                Column(name='parallax_error', data=np.zeros(len(data))),
            ]
        )

        sources.write(args.output_path, format="ascii", overwrite=True)

    else:

        sources = Table(
            [
                Column(name='ra', data=data['XWIN_WORLD'], unit=u.deg),
                Column(name='dec', data=data['YWIN_WORLD'], unit=u.deg),
                Column(name='source_id', data=np.arange(1, len(data) + 1, 1)),
                Column(name='ra_error', data=data['ERRAWIN_WORLD'], unit=u.deg),
                Column(name='dec_error', data=data['ERRBWIN_WORLD'], unit=u.deg),
                Column(name='phot_g_mean_flux', data=flux),
                Column(name='phot_g_mean_flux_error', data=flux_error),
                Column(name='phot_bp_mean_flux', data=flux),
                Column(name='phot_bp_mean_flux_error', data=flux_error),
                Column(name='phot_rp_mean_flux', data=flux),
                Column(name='phot_rp_mean_flux_error', data=flux_error)
            ]
        )

        sources.write(args.output_path, format="fits", overwrite=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', help='Path to input LDAC catalog')
    parser.add_argument('output_path', help='Path to output ASCII catalog')
    parser.add_argument('type', help='Type of table to output (DAT or FITS)')
    args = parser.parse_args()

    convert(args)
