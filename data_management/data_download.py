from prefect import flow, task
import os
import argparse
import requests
import yaml
import json
from shutil import move
from datetime import datetime, timedelta, timezone
from image_reduction.infrastructure import logs as lcologs
from image_reduction.infrastructure import data_classes
from image_reduction.IO import compression_utils
from image_reduction.configuration import configure_aperture_pipeline

@flow
def check_for_new_data():
    """
    Script to manage automated downloads of observations from AWS-hosted data archives.
    """

    # Get arguments
    args = get_args()

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
            stat, uncompressed_path = compression_utils.funpack_frame(config, entry.filename, log=log)

            # Transfer the frame to the appropriate data reduction directory if
            # that directory is unlocked; otherwise hold the data in situ for the next run
            if stat:
                entry.set_uncompressed_filename(uncompressed_path)

                entry.set_reduction_directory(config)

                # If this is a new reduction, create the directory and
                # set up the reduction configuration if needed
                if not os.path.isdir(entry.red_dir):
                    os.makedirs(entry.red_dir)
                    lcologs.log('Created new reduction directory ' + entry.red_dir, 'info', log=log)

                    if entry.instrument_type == 'imager':
                        red_params = data_classes.get_reduction_parameters(
                            os.path.join(config['data_download_dir'], entry.filename)
                        )
                        configure_aperture_pipeline.create_red_config(
                            config, red_params, entry.red_dir, log=log
                        )

                dir_stat = check_red_dir_unlocked(entry.red_dir, log=log)

                if dir_stat:
                    move_to_red_dir(config, entry, log=log)
                    obs_set.add_observation(os.path.join(entry.red_dir, entry.filename))

    # Update list of data already downloaded
    obs_set.save(data_summary_path)

    lcologs.close_log(log)

@task
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

@task
def fetch_new_datalist(archive_config, obs_set, start_time, end_time, log):
    """Function to query the archive and retrieve a list of frames,
    described as dictionaries of frame parameters."""

    if archive_config['active']:
        lcologs.log('Retrieving new data from archive ' + archive_config['name'], 'info', log=log)

        for proposal in archive_config['proposals']:

            ur = {
                'start': start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                'end': end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                'proposal_id': proposal,
                'reduction_level': str(archive_config['reduction_level']['image']),
                'public': 'false'
            }

            results = retrieve_paginated_results(archive_config, ur, log=log)

        lcologs.log('Queried data archive with ' + str(len(results)) + ' results', 'info', log=log)

        # Build a list of new frames from query results, excluding calibration data
        new_data = build_data_list(archive_config, obs_set, results, proposal, log)

    else:
        lcologs.log(
            'WARNING: Download of data from ' + archive_config['name'] + ' switched off in the configuration',
            'info',
            log=log
        )

    return new_data

def retrieve_paginated_results(archive_config, ur, log=None):
    """
    Function to retrieve a single page of paginated results from an archive

    Parameters
    ----------

    archive_config dict  Parameters describing the data archive
    ur      dict    Query parameters
    log     object  [optional] Logger instance

    Returns
    -------
    results list    Results information extracted from the website
    """

    results = []
    next_page = True
    page = 0
    max_page = 10

    # Set the limit per page explicitly to ensure we offset properly per page
    limit = 100
    ur['limit'] = str(limit)

    while next_page:
        ur['offset'] = str(page*limit)
        response = talk_to_lco_archive(archive_config, ur, 'frames', 'GET', log=log)
        lcologs.log(' -> Retrieved page ' + str(page) + ' of archive ' + str(response['count']) + ' results',
                    'info', log=log)
        results += response['results']
        page += 1

        if not response['next'] or page >= max_page:
            next_page = False

    lcologs.log(' -> Retrieved a total of ' + str(len(results)) + ' of archive results', 'info', log=log)

    return results

@task
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

    for entry in query_results:
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
        else:
            lcologs.log('Not downloading file ' + entry['filename'] + ' because '
                        + repr(use_file) + ' '
                        + repr(obs_set.check_file_in_set(entry['filename'])),
                               'info',log=log)

    lcologs.log(
        'Found '+str(len(new_data)) \
        +' frame(s) available for download from proposal '+proposal,
        'info',
        log=log
    )

    return new_data

@task
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

@task
def _search_key_in_filename(filename, search_keys):

    status = False
    for key in search_keys:
        if key in filename:
            status = True
            continue

    return status

@task
def is_frame_calibration_data(filename):
    """Function to determine whether or not a given frame is a calibration or
    science frame, based on its filename"""

    search_keys = [ 'bias', 'dark', 'flat', 'skyflat' ]

    status = _search_key_in_filename(filename, search_keys)

    return status

@task
def talk_to_lco_archive(config, ur, end_point, method, log=None):
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

    headers = {'Authorization': 'Token ' + os.environ[config['token_envvar']]}

    if end_point[0:1] == '/':
        end_point = end_point[1:]
    if end_point[-1:] != '/':
        end_point = end_point+'/'
    url = os.path.join(config['url'], end_point)
    lcologs.log('Querying data archive at URL ' + url,
                'info', log=log)

    if method == 'POST':
        if ur != None:
            response = requests.post(url, headers=headers, json=ur)
        else:
            response = requests.post(url, headers=headers)

    # Iterate over paginated results
    elif method == 'GET':
        response = requests.get(url, headers=headers, params=ur)

    lcologs.log('Queried data archive with status=' + str(response.status_code),
                'info', log=log)

    return response.json()

@task
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

@task
def move_to_red_dir(config, entry, log=None):

    src = os.path.join(config['data_download_dir'], entry.filename)
    dest = os.path.join(entry.red_dir, os.path.basename(entry.filename))

    move(src, dest)

    lcologs.log(os.path.basename(entry.filename) + ' --> ' + os.path.dirname(dest), 'info', log=log)

@task
def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='Path to the configuration file')
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    check_for_new_data()
