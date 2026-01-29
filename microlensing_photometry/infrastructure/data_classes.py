import os
from astropy.table import Table, Column
from astropy import units as u
from astropy.io import ascii
from astropy.coordinates import SkyCoord
import numpy as np
from microlensing_photometry.infrastructure import time_utils as lcotime
from microlensing_photometry.infrastructure import logs as lcologs

class ObservationSet(object):
    """
    Metadata describing a sequence of observations
    """

    def __init__(self, file_path=None, log=None):
        self.table = Table([
            Column(name='file', data=np.array([]), dtype='str'),
            Column(name='facility_code', data=np.array([]), dtype='str'),
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
            Column(name='NAXIS1', data=np.array([]), dtype='int'),
            Column(name='NAXIS2', data=np.array([]), dtype='int'),
            Column(name='WMSCLOUD', data=np.array([]), dtype='float64', unit=u.deg_C),
            ])

        if file_path:
            self.load(file_path, log=log)

    def save(self, file_path, log=None):
        ascii.write(self.table, file_path, overwrite=True)
        lcologs.log(
            'Saved data summary of ' + str(len(self.table)) + ' to ' + file_path,
            'info',
            log=log
        )

    def load(self, file_path, log=None):
        if os.path.isfile(file_path):
            self.table = ascii.read(file_path)
            lcologs.log(
                'Loaded data summary of ' + str(len(self.table)) + ' from ' + file_path,
                'info',
                log=log
            )

        else:
            lcologs.log(
                'No data summary found at ' + file_path + '; empty table returned',
                'warning',
                log=log
            )

    def get_facility_code(self, header):
        """Function to return the reference code used within the phot_db to
        refer to a specific facility as site-enclosure-tel-instrument"""

        try:
            if 'fl' in header['INSTRUME']:
                header['INSTRUME'] = header['INSTRUME'].replace('fl', 'fa')
            facility_code = header['SITEID'] + '-' + \
                            header['ENCID'] + '-' + \
                            header['TELID'] + '-' + \
                            header['INSTRUME']
        except:
            facility_code = 'None'

        return facility_code

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

        # Identify the facility from its header information
        facility_code = self.get_facility_code(header)

        # Catch malformed header entries that won't parse into a table
        if 'UNKNOWN' in str(header['WMSCLOUD']):
            wmscloud = -9999.99
        else:
            wmscloud = header['WMSCLOUD']

        # Calculate HJD
        hjd, ltt_helio = lcotime.calc_hjd(
            header['DATE-OBS'],
            s.ra.deg,
            s.dec.deg,
            '-'.join(facility_code.split('-')[0:3]),
            header['EXPTIME']
        )

        row = [
            os.path.basename(file_path),
            facility_code,
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
            hjd,
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
            header['NAXIS1'],
            header['NAXIS2'],
            wmscloud
        ]

        self.table.add_row(row)

