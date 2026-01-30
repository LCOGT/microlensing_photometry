import os
import argparse
import requests
import yaml
import json
from shutil import move
from datetime import datetime, timedelta, timezone
from microlensing_photometry.infrastructure import logs as lcologs
from microlensing_photometry.infrastructure import data_classes
from microlensing_photometry.IO import compression_utils

def run(args):
    """
    Script to manage automated downlaods of observations from AWS-hosted data archives.

    Parameters
    ----------

    args : Argparser object
    """

    # Load reduction configuration
    config = yaml.safe_load(open(args.config_file))

    # Start logging
    log = lcologs.start_log(config['log_dir'], 'data_download')

    # Fetch list of frames already downloaded
    data_summary_path = os.path.join(config['log_dir'], 'main_data_list.txt')
    if os.path.isfile(data_summary_path):
        obs_set = data_classes.ObservationSet(file_path=data_summary_path, log=log)
    else:
        obs_set = data_classes.ObservationSet()
        lcologs.log('Instantiating new observations set list', 'info', log=log)

    # Configure the data range used to query the archive within a limited range
    (start_time, end_time) = set_date_range(config, log)

    # For each configured archive, fetch a list of all available data from each configured proposal
    for archive_id, archive_config in config['archives'].items():
        new_data = fetch_new_datalist(archive_config, obs_set, start_time, end_time, log)

        # Retrieve and unpack all new observations
        for entry in new_data:

            # Download the file from the archive
            download_new_frame(config, entry, log)

            # Uncompress the observation if necessary
            stat, uncompressed_path = compression_utils.funpack_frame(config, entry.filename, log)

            # Transfer the frame to the appropriate data reduction directory if
            # that directory is unlocked; otherwise hold the data in situ for the next run
            if stat:
                entry.set_uncompressed_filename(uncompressed_path)

                entry.set_reduction_directory(config)

                dir_stat = check_red_dir_unlocked(entry.red_dir, log=log)

                if dir_stat:
                    move_to_red_dir(config, entry, log=log)
                    obs_set.add_observation(os.path.join(entry.red_dir, entry.filename))

    # Update list of data already downloaded
    obs_set.save(data_summary_path)

    lcologs.close_log(log)

def set_date_range(config, log):
    """Function to set the search date range from the configuration or to
    the last 24hrs

    Expected date-time format is %Y-%m-%d %H:%M in UTC"""

    tz = timezone(timedelta(hours=0, minutes=0))  # Use UTC

    if 'none' not in str(config['start_datetime']).lower() and \
        'none' not in str(config['end_datetime']).lower():
        start_time = datetime.strptime(config['start_datetime'],'%Y-%m-%d %H:%M')
        end_time = datetime.strptime(config['end_datetime'],'%Y-%m-%d %H:%M')
    else:
        end_time = datetime.now(tz)
        start_time = end_time - timedelta(days=1)

    # Optional time offset for debugging
    #dt = timedelta(hours=24.0)
    #start_time -= dt

    lcologs.log('Searching for data taken between '+start_time.strftime("%Y-%m-%d %H:%M")+\
                ' and '+end_time.strftime("%Y-%m-%d %H:%M"), 'info', log=log)

    return start_time, end_time

def fetch_new_datalist(archive_config, obs_set, start_time, end_time, log):
    """Function to query the archive and retrieve a list of frames,
    described as dictionaries of frame parameters."""

    if archive_config['active']:
        lcologs.log('Retrieving new data from archive ' + archive_config['name'], 'info', log=log)

        for proposal in archive_config['proposals']:

            ur = { 'PROPID': proposal, 'start': start_time.strftime("%Y-%m-%d %H:%M"),
                                        'end': end_time.strftime("%Y-%m-%d %H:%M") }

            results = talk_to_lco_archive(archive_config, ur, 'frames', 'GET')

            # Build a list of new frames from query results, excluding calibration data
            new_data = build_data_list(archive_config, obs_set, results, proposal, log)

    else:
        lcologs.log(
            'WARNING: Download of data from ' + archive_config['name'] + ' switched off in the configuration',
            'info',
            log=log
        )

    return new_data

def build_data_list(archive_config, obs_set, query_results, proposal, log):
    """
    Function to add new frames to a list of frames to be downloaded,
    excluding certain data types such as calibration frames.

    This function is selective, downloading instrument-corrected data for those instruments
    where these products are provided, but raw data for those where they are not.
    """

    lcologs.log('Looking for new frames in archive records from ' + archive_config['name'] + '...',
                'info',
                log=log)

    # Initialise a list of new data entries for this archive
    new_data = []

    for entry in query_results['results']:
        use_file = False

        # Archive queries return lots of data entries, so
        # verify that the current query result is a type of frame that
        # the config says we should download.
        if entry['OBSTYPE'] == 'EXPOSE':
            if entry['RLEVEL'] == archive_config['reduction_level']['image']:
                use_file = True
        else:
            if archive_config['name'] == 'LCO':
                if entry['OBSTYPE'] == 'SPECTRUM':
                    if entry['RLEVEL'] == archive_config['reduction_level']['spectrum']:
                        use_file = True
            else:
                if entry['OBSTYPE'] == 'SPECTRUM':
                    if 'wecfzst' in entry['filename']:
                        use_file = True

        # Check whether the frame has already been downloaded;
        # if not, record the archive entry in the list of files to download
        # from this archive
        if use_file and not obs_set.check_file_in_set(entry['filename']):
            archive_entry = data_classes.LCOArchiveEntry(params=entry)
            new_data.append(archive_entry)
            lcologs.log(archive_entry.summary(),'info',
        log=log)


    lcologs.log(
        'Found '+str(len(new_data)) \
        +' frame(s) available for download from proposal '+proposal,
        'info',
        log=log
    )

    return new_data

def check_red_dir_unlocked(dataset_path, log=None):
    """Function to check for a lockfile in a given dataset before starting
    a reduction.  Returns True if unlocked."""

    lockfile = os.path.join(dataset_path, 'dataset.lock')
    lcologs.log('Searching for lockfile at ' + lockfile, 'info', log=log)

    if os.path.isfile(lockfile):
       lcologs.log(' -> ' + os.path.basename(dataset_path) + ' is locked', 'info', log=log)
       status = False
    else:
        lcologs.log(' -> ' + os.path.basename(dataset_path) + ' is not locked', 'info', log=log)
        status = True

    return status

def _search_key_in_filename(filename, search_keys):

    status = False
    for key in search_keys:
        if key in filename:
            status = True
            continue

    return status

def is_frame_calibration_data(filename):
    """Function to determine whether or not a given frame is a calibration or
    science frame, based on its filename"""

    search_keys = [ 'bias', 'dark', 'flat', 'skyflat' ]

    status = _search_key_in_filename(filename, search_keys)

    return status

def talk_to_lco_archive(config, ur, end_point, method):
    """Function to communicate with various APIs of the LCO network.
    ur should be a user request while end_point is the URL string which
    should be concatenated to the observe portal path to complete the URL.
    Accepted end_points are:
        "userrequests"
        "userrequests/cadence"
        "userrequests/<id>/cancel"
    Accepted methods are:
        POST GET
    """

    jur = json.dumps(ur)

    headers = {'Authorization': 'Token ' + os.environ[config['token_envvar']]}

    if end_point[0:1] == '/':
        end_point = end_point[1:]
    if end_point[-1:] != '/':
        end_point = end_point+'/'
    url = os.path.join(config['url'], end_point)

    if method == 'POST':
        if ur != None:
            response = requests.post(url, headers=headers, json=ur).json()
        else:
            response = requests.post(url, headers=headers).json()
    elif method == 'GET':
        response = requests.get(url, headers=headers, params=ur).json()

    return response

def download_new_frame(config, entry, log):
    """Function to download list of frames"""

    # Check whether a file of this name has been downloaded on a previous run
    # and is already waiting in the incoming/ data directory.  This can occur
    # if data are downloaded while a reduction is running as this locks the
    # reduction directory for that object
    if os.path.isfile(os.path.join(config['data_download_dir'], entry.filename)) \
        or os.path.isfile(
            os.path.join(config['data_download_dir'], str(entry.filename).replace('.fz',''))
            ):
            lcologs.log('File ' + entry.filename + ' already downloaded', 'info', log=log)

    # If not, then download the file:
    else:
        try:
            response = requests.get(entry.url)

            if response.status_code == 200:
                with open(os.path.join(config['data_download_dir'], entry.filename), 'wb') as dframe:
                    dframe.write(response.content)
                    dframe.close()
                lcologs.log('-> Downloaded ' + entry.filename, 'info', log=log)
            else:
                lcologs.log(
                    '->>>> Error downloading '+entry.filename+\
                    ' status code='+str(response.status_code),
                    'info', log=log
                )

        except requests.exceptions.ConnectionError:
            lcologs.log('->>>> ConnectionError downloading ' + entry.filename, 'warning', log=log)

def move_to_red_dir(config, entry, log=None):

    if not os.path.isdir(entry.red_dir):
        os.makedirs(entry.red_dir)

    src = os.path.join(config['data_download_dir'], entry.filename)
    dest = os.path.join(entry.red_dir, os.path.basename(entry.filename))

    move(src, dest)

    lcologs.log(os.path.basename(entry.filename) + ' --> ' + os.path.basename(dest), 'info', log=log)

def start_day_log(config):
    """Function to set up a logger object"""

    if path.isdir(config['log_dir']) == False:
        makedirs(config['log_dir'])

    log_date = datetime.utcnow().strftime("%Y-%m-%d")
    log_file = path.join(config['log_dir'], 'data_download_'+log_date+'.log')

    log = logging.getLogger( 'data_download' )

    if len(log.handlers) == 0:
        log.setLevel( logging.INFO )
        file_handler = logging.FileHandler( log_file )
        file_handler.setLevel( logging.INFO )
        formatter = logging.Formatter( fmt='%(asctime)s %(message)s', \
                                    datefmt='%Y-%m-%dT%H:%M:%S' )
        file_handler.setFormatter( formatter )
        log.addHandler( file_handler )

    log.info('------------------------------------------------------------\n')
    log.info('Started data download process\n')

    return log

def close_log(log):
    log.info( 'Processing complete\n' )
    logging.shutdown()

def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='Path to the configuration file')
    args = parser.parse_args()

    return args

if __name__ == '__main__':

    args = get_args()
    run(args)
