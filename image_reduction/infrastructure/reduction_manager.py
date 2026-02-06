from prefect import flow, task
from prefect.deployments import run_deployment
import os
import sys
import psutil
import argparse
import yaml
import subprocess
from datetime import datetime, UTC, timedelta
from astropy.io.fits import getheader
from image_reduction.infrastructure import logs as lcologs

@flow
def archon():
    """
    Data pipeline tasks manager to manage the selection and reduction for multiple datasets
    """

    # Get arguments
    args = get_args()

    # Load reduction configuration
    config = yaml.safe_load(open(args.config_file))

    # Start logging
    log = lcologs.start_log(config['log_dir'], 'archon')

    # Count the number of currently running reductions and avoid triggering more
    # than the allowed maximum
    nreductions = count_running_processes('aperture_pipeline.py', log=log)
    if nreductions >= config['max_parallel']:
        lcologs.log('Maximum number of parallel reductions already reached', 'warning', log=log)

    # Identify datasets to be reduced, avoiding any that are locked due to ongoing reductions.
    datasets = find_imaging_data_for_aperture_photometry(config, log)

    # Trigger parallelized reduction processes
    process_datasets(config, datasets, nreductions, log)

    lcologs.close_log(log)

@task
def count_running_processes(command, log=None):
    """
    Function to count the number of active instances of the image reduction pipeline

    Parameters
    ----------
    command     string  Name of the process in the system logs to check for

    Returned
    --------
    nprocess    int     Number of running reduction processes
    """

    instances = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        if 'Python' in proc.info['name']:
            if command in proc.info['cmdline'][1]:
                instances.append(proc.info['pid'])

    lcologs.log('Found ' + str(len(instances)) + ' reductions currently running', 'info', log=log)

    return len(instances)

@task
def process_datasets(config, datasets, nreductions, log):
    """
    Function to initiate parallelized reductions of multiple separate datasets.

    Parameters
    ----------
    config    dict  Script configuration
    datasets  list  Set of directory paths to unlocked datasets to be reduced
    nreductions int Existing number of ongoing reductions
    log        object   Logger instance
    """

    command = os.path.join(config['software_dir'], 'infrastructure', 'aperture_pipeline.py')
    i = 0
    while nreductions < config['max_parallel'] and i < len(datasets):
        arguments = [datasets[i]]
        lcologs.log('Started reduction for ' + datasets[i], 'info', log=log)
        pid = trigger_process(command, arguments, log, wait=False)
        nreductions += 1
        i += 1
        if nreductions >= config['max_parallel']:
            lcologs.log(
                'Reached configured maximum number of parallel processes',
                'warning', log=log
            )

@task
def trigger_process(command, arguments, log, wait=True):
    """
    Function to initiate a subprocess to reduce an individual dataset

    Parameters
    ----------
    command     string  Full path to Python software to be run
    arguments   list    Arguments needed for the command to run
    wait        Boolean Switch to wait for output from the process or not
    log         object  Logger instance

    Returns
    ----------
    pid         int     Process ID code
    """

    args = [sys.executable, command] + arguments

    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if wait:
        stdout, stderr = proc.communicate()

    lcologs.log('Started process PID=' + str(proc.pid), 'info', log=log)

    if wait:
        lcologs.log(stderr.decode(), 'error', log=log)

    return proc.pid

@task
def find_imaging_data_for_aperture_photometry(config, log):
    """
    Function to determine which imaging datasets should be processed by the aperture photometry
    pipeline, avoiding any that are already locked due to an ongoing reduction

    Datasets can be selected by one of four mechanisms, controlled by the dataset_selection
    parameters in the reduction manager configuration
    1) group = 'all': Reduce all available unlocked image datasets
    2) group = 'date': Reduce all available unlocked image datasets with data taken between
                    start_date and end_date
    3) group = 'recent': Reduce all available unlocked image datasets with data taken since
                    now - ndays
    4) group = 'file': Reduction all unlocked image datasets from the reduction directories
                    listed in the given file

    Note that the group parameter takes precedence over the other settings.

    Parameters
    ----------

    config   dict   Archon process configuration
    log      log instance

    Returns
    ----------
    datasets    list    List of data reduction directories to be processed
    """
    group = str(config['dataset_selection']['group']).lower()
    lcologs.log('Data group selection: ' + group, 'info', log=log)

    match group:

        # 1) group = 'all': Reduce all available unlocked image datasets
        case 'all':
            datasets = find_unlocked_red_dirs.fn(config, log)

        # 2) group = 'date': Reduce all available unlocked image datasets with data taken between
        #                     start_date and end_date
        case 'date':
            # Fetch selected date range
            start_date = datetime.strptime(
                config['dataset_selection']['start_date'],
                '%Y-%m-%d'
            ).replace(tzinfo=UTC)
            end_date = datetime.strptime(
                config['dataset_selection']['end_date'],
                '%Y-%m-%d'
            ).replace(tzinfo=UTC)

            # Make a list of all unlocked directories to be reviewed
            unlocked_dirs = find_unlocked_red_dirs.fn(config, log)

            # Review the data in all unlocked directories
            datasets = find_data_within_daterange(config, unlocked_dirs, start_date, end_date)

        # 3) group = 'recent': Reduce all available unlocked image datasets with data taken since
        #                     ndays
        case 'recent':
            end_date = datetime.now(UTC)
            start_date = end_date - timedelta(days = float(config['dataset_selection']['ndays']))

            # Make a list of all unlocked directories to be reviewed
            unlocked_dirs = find_unlocked_red_dirs.fn(config, log)

            # Review the data in all unlocked directories
            datasets = find_data_within_daterange(config, unlocked_dirs, start_date, end_date)

        # 4) group = 'file': Reduction all unlocked image datasets from the reduction directories
        #                     listed in the file given by 'file'
        case 'file':
            if os.path.isfile(config['dataset_selection']['file']):
                with open(config['dataset_selection']['file'], 'r') as f:
                    dir_list = f.readlines()
                    f.close()
                    dir_list = [
                        os.path.join(config['data_reduction_dir'], dpath.replace('\n',''))
                        for dpath in dir_list
                        if len(dpath.replace('\n','')) > 0
                    ]

                    # Check datasets unlocked
                    datasets = [ dpath for dpath in dir_list if not check_dataset_lock.fn(dpath, log) ]

            else:
                lcologs.log(
                    'Cannot find input file for dataset selection at ' + config['dataset_selection']['file'],
                'info', log=log
                )
                raise IOError('Cannot find input file for dataset selection at ' + config['dataset_selection']['file'])

        case _:
            lcologs.log('Invalid data selection group in reduction manager configuration', info, log=log)
            raise IOError('Invalid data selection group in reduction manager configuration')

    lcologs.log('Found ' + str(len(datasets)) + ' unlocked datasets to process', 'info', log=log)

    return datasets

@task
def find_unlocked_red_dirs(config, log):
    """
    Function to create a list of all reduction directories

    Parameters
    ----------
    config  dict   Script configuration

    Returns
    datasets list  List of reduction directory paths
    """
    datasets = []
    target_dirs = [f.path for f in os.scandir(config['data_reduction_dir']) if f.is_dir()]
    for dpath in target_dirs:
        for instrument in config['instrument_list']:
            if os.path.isdir(os.path.join(dpath, instrument)):
                datasets += [
                    f.path for f in os.scandir(os.path.join(dpath, instrument))
                    if f.is_dir() and not check_dataset_lock.fn(f, log)
                ]

    return datasets

def find_data_within_daterange(config, dir_list, start_date, end_date):
    """
    Function to review the FITS data within a set of directories and return a list of
    those directories that contain any data obtained within the given date range

    Parameters
    ----------
    dir_list  list   List of full directory paths
    start_date datetime  Minimum of the allowed date range
    end_date   datetime  Maximum of the allowed date range

    Returns
    -------
    selected_dir_list  list   List of the full paths of directories passing the selection
    """
    selected_dir_list = []

    # Review the data in all unlocked directories
    for red_dir in dir_list:
        images = [os.path.join(red_dir, f.name) for f in os.scandir(red_dir)
                  if f.is_file() and ('.fits' in f.name or '.fts' in f.name)]

        use_dir = False
        i = 0
        while i < len(images) and not use_dir:
            header = getheader(images[i])
            dateobs = datetime.strptime(
                header['DATE-OBS'],
                '%Y-%m-%dT%H:%M:%S.%f'
            ).replace(tzinfo=UTC)
            if dateobs >= start_date and dateobs <= end_date:
                use_dir = True
            i += 1

        if use_dir:
            selected_dir_list.append(red_dir)

    return selected_dir_list

@task
def check_dataset_lock(red_dir, log):
    """
    Function to check for a lockfile in a given dataset before starting
    a reduction

    Parameters
    ----------
    red_dir   string    Path to reduction directory
    log      logger     Pipeline logging instance

    Returns
    -------
    status  Boolean     True if locked, otherwise False
    """

    lockfile = os.path.join(red_dir,'dataset.lock')

    if os.path.isfile(lockfile):
        lcologs.log(os.path.basename(red_dir) + ' is locked', 'info', log=log)
        status = True
    else:
        lcologs.log(os.path.basename(red_dir) + ' is not locked', 'info', log=log)
        status = False

    return status

@task
def lock_dataset(red_dir, log):
    """
    Function to create a lockfile in a dataset's reduction directory to indicate on ongoing reduction

    Parameters
    ----------
    red_dir   string    Path to reduction directory
    log      logger     Pipeline logging instance
    """

    lockfile = os.path.join(red_dir, 'dataset.lock')

    ts = datetime.now(UTC)

    with open(lockfile,'w') as f:
        f.write(ts.strftime('%Y-%m-%dT%H:%M:%S'))
        f.close()

    lcologs.log('-> Locked dataset ' + os.path.basename(red_dir), 'info', log=log)

@task
def unlock_dataset(red_dir, log):
    """
    Function to remove a lock on a dataset once reductions have completed

    Parameters
    ----------
    red_dir   string    Path to reduction directory
    log      logger     Pipeline logging instance
    """

    lockfile = os.path.join(red_dir, 'dataset.lock')

    if os.path.isfile(lockfile):
        os.remove(lockfile)
        lcologs.log('-> Unlocked dataset ' + os.path.basename(red_dir), 'info', log=log)

    else:
        lcologs.log(
            '-> Dataset ' + os.path.basename(red_dir) + ' found unlocked when lock expected',
            'warning', log=log
        )

@task
def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='Path to the configuration file')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    archon()
