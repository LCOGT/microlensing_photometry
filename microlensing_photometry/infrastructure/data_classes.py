import os
from astropy.table import Table, Column
from astropy import units as u
from astropy.io import ascii
from astropy.io.fits import getheader
from astropy.coordinates import SkyCoord
import numpy as np
import subprocess
from microlensing_photometry.infrastructure import time_utils as lcotime
from microlensing_photometry.infrastructure import logs as lcologs

class ObservationSet(object):
    """
    Metadata describing a sequence of observations and storing a table of their metadata
    """

    def __init__(self, file_path=None, log=None):
        self.table = Table([
            Column(name='file', data=np.array([]), dtype='str'),
            Column(name='facility_code', data=np.array([]), dtype='str'),
            Column(name='proposal_id', data=np.array([]), dtype='str'),
            Column(name='object', data=np.array([]), dtype='str'),
            Column(name='reqnum', data=np.array([]), dtype='int'),
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
            Column(name='CTYPE1', data=np.array([]), dtype=str),
            Column(name='CTYPE2', data=np.array([]), dtype=str),
            Column(name='CRPIX1', data=np.array([]), dtype='float64'),
            Column(name='CRPIX2', data=np.array([]), dtype='float64'),
            Column(name='CRVAL1', data=np.array([]), dtype='float64'),
            Column(name='CRVAL2', data=np.array([]), dtype='float64'),
            Column(name='CUNIT1', data=np.array([]), dtype=str),
            Column(name='CUNIT2', data=np.array([]), dtype=str),
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
            data = ascii.read(file_path)
            if len(data) > 0:
                self.table = data
                lcologs.log(
                    'Loaded data summary of ' + str(len(self.table)) + ' from ' + file_path,
                    'info',
                    log=log
                )
            else:
                lcologs.log(
                    'Data summary ' + file_path + ' empty, returning empty table',
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

    def add_observation(self, file_path, header=None):
        """
        Method to extract observation from the FITS header of a single file

        :param header: Astropy FITS header object
        :return: Information added to self.table
        """

        # Read header info if not parsed to method
        if not header:
            header = getheader(file_path)

        # Ensure coordinates stored in decimal degrees
        if ':' in header['RA']:
            s = SkyCoord(header['RA'], header['DEC'], frame='icrs', unit=(u.hourangle, u.degree))
        else:
            s = SkyCoord(header['RA'], header['DEC'], frame='icrs', unit=(u.degree, u.degree))

        # Identify the facility from its header information
        facility_code = get_facility_code(header)

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
            header['PROPID'],
            header['OBJECT'],
            header['REQNUM'],
            header['FILTER'],
            header['DATE-OBS'],
            header['EXPTIME'],
            float(s.ra.deg),
            float(s.dec.deg),
            header['AIRMASS'],
            header['L1FWHM'],
            header['MOONFRAC'],
            header['MOONDIST'],
            header['L1MEAN'],
            float(hjd),
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

    def check_file_in_set(self, filename):
        """
        Method to verify whether or not the given file is present in the ObservationSet

        :param obs:  Observation object
        :return: boolean
        """

        if filename in self.table['file']:
            return True
        else:
            return False

def get_facility_code(header):
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

def get_reduction_parameters(file_path):
    header = getheader(file_path)
    red_params = {
        'name': header['OBJECT'],
        'RA': header['RA'],
        'Dec': header['DEC']
    }
    return red_params


class LCOArchiveEntry(object):
    """
    Class describing the parameters held by the LCO archive for a single astronomical datafile in FITS format.
    """

    def __init__(self, params={}):
        self.id = None               # Archive Integer ID
        self.basename = None         # Raw file basename
        self.observation_date = None    # Observation date (NOT DATE-OBS) in %Y-%m-%dT%H:%M:%S.fZ format
        self.observation_day =  None    # Day of observation in %Y-%m-%d format
        self.proposal_id = None         # string, proposal ID
        self.instrument_id = None       # string, intrument identifier
        self.target_name = None         # string, target name
        self.reduction_level = None     # Integer, LCO pipeline reduction level
        self.site_id = None             # string, site code
        self.telescope_id = None        # string, telescope id code
        self.exposure_time = None       # float, exposure time in seconds
        self.primary_optical_element = None     # string, first filter or grating identifier
        self.public_date = None         # datetime of end of priprietry period in %Y-%m-%dT%H:%M:%S.fZ format
        self.configuration_type = None    # Type of observation, e.g. EXPOSE, GUIDE, etc
        self.observation_id = None      # Integer, observation ID
        self.request_id = None          # Integer, request ID
        self.version_set = []           # List of dictionaries containing information about the version of this file in the archive
        self.url = None                 # AWS URL for the file
        self.filename = None            # Filename of the data file
        # The following are values extrated from the file header
        self.DATE_OBS = None            # DATE-OBS in %Y-%m-%dT%H:%M:%S.fZ format
        self.DAY_OBS = None             # Day of observation in %Y-%m-%d format
        self.PROPID = None              # string, proposal ID
        self.INSTRUME = None            # string, instrument identifier
        self.OBJECT = None              # string, target name
        self.RLEVEL = None              # Integer, LCO pipeline reduction level
        self.SITEID = None              # string, site code
        self.TELID = None               # string, telescope id code
        self.EXPTIME = None             # float, exposure time in seconds
        self.FILTER = None              # string, first filter
        self.L1PUBDAT = None            # datetime of end of pripriatry period in %Y-%m-%dT%H:%M:%S.fZ format
        self.OBSTYPE = None             # Type of observation, e.g. EXPOSE, GUIDE, etc
        self.BLKUID = None              # Integer, observation ID
        self.REQNUM = None              # Integer, request ID
        self.area = None                # Dictionary of verticies coordinates of the sky region
        self.red_dir = None             # Reduction directory path
        self.instrument_class = None    # Class of instrument
        self.instrument_type = None      # Type of instrument, either imager or spectrograph

        if params:
            for key, value in params.items():
                setattr(self, key, value)

            self.set_instrument_class()

    def summary(self):
        return 'LCO archive entry: ' + self.filename + ' ' + self.DATE_OBS \
                  + ' ' + self.SITEID + ' ' + self.INSTRUME + ' ' + self.FILTER

    def set_instrument_class(self):
        if 'fl' in self.INSTRUME or 'fa' in self.INSTRUME:
            self.instrument_class = 'sinistro'
            self.instrument_type = 'imager'
        elif 'ep' in self.INSTRUME:
            self.instrument_class = 'muscat'
            self.instrument_type = 'imager'
        elif 'sq' in self.INSTRUME:
            self.instrument_class = 'qhy'
            self.instrument_type = 'imager'
        elif 'en' in self.INSTRUME:
            self.instrument_class = 'FLOYDS'
            self.instrument_type = 'spectrograph'
        elif 'GHTS_RED' in self.INSTRUME or 'GHTS_BLUE' in self.INSTRUME:
            self.instrument_class = 'goodman'
            self.instrument_type = 'spectrograph'

    def set_reduction_directory(self, config):
        """
        Method to define the default directory structure for reduced data.

        Since a given target may be observed with multiple instruments, including both
        imaging and spectroscopy, the data structure needs to accommodate the different expectations
        of the various reduction pipelines.

        The structure used is as follows:

        /data_reduction_dir/
                    |- target_name/
                            |- imager_class
                                    |- filter
                            |- spectrograph_class
                                    |- date
        E.g.
          /data_reduction_dir/
                    |- OGLE-2026-BLG-0001/
                            |- sinistro
                                    |- SDSSi/
                                    |- SDSSg/
                            |- floyds
                                    |- 20260101/
                            |- goodman
                                    |- 20260101/

        Parameters:
            config : Argparse object    Configuration parameters

        Returns:
            Sets self.red_dir : string            Path to reduction directory

        """

        red_dir = os.path.join(config['data_reduction_dir'], self.target_name)

        # Imager classes
        if self.instrument_class in ['sinistro', 'qhy', 'muscat']:
            self.red_dir = os.path.join(red_dir, self.instrument_class, self.FILTER)

        # Spectrograph classes
        elif self.instrument_class in ['floyds', 'goodman']:
            self.red_dir = os.path.join(
                red_dir,
                self.instrument_class,
                str(self.DAY_OBS).replace('-','')
            )

        # Unrecognized instrument
        else:
            raise IOError('Unrecognized instrument ' + self.INSTRUME + '; cannot created reduction directory')

    def set_uncompressed_filename(self, uncompressed_path):
        self.filename = os.path.basename(uncompressed_path)

    def get_reduction_params(self):
        """
        Method to return a dictionary of all parameters necessary for reduction of imaging data
        """
        red_params = {
            'name': self.OBJECT,
            'RA': self.RA,
            'Dec': self.Dec
        }
        return red_params