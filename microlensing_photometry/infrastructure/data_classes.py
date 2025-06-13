import os
from astropy.table import Table, Column
from astropy import units as u
from astropy.io import ascii
from astropy.coordinates import SkyCoord
import numpy as np

class ObservationSet(object):
    """
    Metadata describing a sequence of observations
    """

    def __init__(self, file_path=None):
        self.table = Table([
            Column(name='file', data=np.array([]), dtype='str'),
            Column(name='filter', data=np.array([]), dtype='str'),
            Column(name='dateobs', data=np.array([]), dtype='str'),
            Column(name='exptime', data=np.array([]), dtype='float64', unit=u.second),
            Column(name='RA', data=np.array([]), dtype='float64', unit=u.degree),
            Column(name='Dec', data=np.array([]), dtype='float64', unit=u.degree),
            Column(name='airmass', data=np.array([]), dtype='float64'),
            Column(name='fwhm', data=np.array([]), dtype='float64', unit=u.pixel),
            Column(name='moon_fraction', data=np.array([]), dtype='float64'),
            Column(name='moon_separation', data=np.array([]), dtype='float64', unit=u.degree),
            Column(name='sky_bkgd', data=np.array([]), dtype='float64', unit=u.adu),
            Column(name='HJD', data=np.array([]), dtype='float64', unit=u.day),
            Column(name='CTYPE1', data=np.array([]), dtype='str'),
            Column(name='CTYPE2', data=np.array([]), dtype='str'),
            Column(name='CRPIX1', data=np.array([]), dtype='float64'),
            Column(name='CRPIX2', data=np.array([]), dtype='float64'),
            Column(name='CRVAL1', data=np.array([]), dtype='float64'),
            Column(name='CRVAL2', data=np.array([]), dtype='float64'),
            Column(name='CUNIT1', data=np.array([]), dtype='str'),
            Column(name='CUNIT2', data=np.array([]), dtype='str'),
            Column(name='CD1_1', data=np.array([]), dtype='float64'),
            Column(name='CD1_2', data=np.array([]), dtype='float64'),
            Column(name='CD2_1', data=np.array([]), dtype='float64'),
            Column(name='CD2_2', data=np.array([]), dtype='float64'),
            Column(name='WCSERR', data=np.array([]), dtype='int'),
            Column(name='WMSCLOUD', data=np.array([]), dtype='float64', unit=u.deg_C),
            ])

        if file_path:
            self.load(file_path)

    def save(self, file_path):
        ascii.write(self.table, file_path, overwrite=True)

    def load(self, file_path, log=None):
        if os.path.isfile(file_path):
            self.table = ascii.read(file_path)
        else:
            if log:
                log.info('No data summary found at ' + file_path + '; empty table returned')

    def add_observation(self, file_path, header):
        """
        Method to extract observation from the FITS header of a single file

        :param header: Astropy FITS header object
        :return: Information added to self.table
        """

        # Ensure coordinates stored in decimal degrees
        if ':' in header['RA']:
            s = SkyCoord(header['RA'], header['DEC'], frame='icrs', unit=(u.hourangle, u.degree))
        else:
            s = SkyCoord(header['RA'], header['DEC'], frame='icrs', unit=(u.degree, u.degree))

        row = [
            os.path.basename(file_path),
            header['FILTER'],
            header['DATE-OBS'],
            header['EXPTIME'],
            s.ra.deg,
            s.dec.deg,
            header['AIRMASS'],
            header['L1FWHM'],
            header['MOONFRAC'],
            header['MOONDIST'],
            header['L1MEAN'],
            0.0,
            header['CTYPE1'],
            header['CTYPE2'],
            header['CRPIX1'],
            header['CRPIX2'],
            header['CRVAL1'],
            header['CRVAL2'],
            header['CUNIT1'],
            header['CUNIT2'],
            header['CD1_1'],
            header['CD1_2'],
            header['CD2_1'],
            header['CD2_2'],
            header['WCSERR'],
            header['WMSCLOUD']
        ]

        self.table.add_row(row)