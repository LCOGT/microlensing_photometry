from datetime import datetime, timedelta, UTC
import pytest
import os
import sys
import shutil
import psutil
import time
from pathlib import Path
from astropy.io import fits
import numpy as np
import subprocess
from image_reduction.infrastructure import reduction_manager

def generate_test_red_dirs():
    """
    Function to generate a selection of test data
    """
    data_dir = os.path.join(os.getcwd(), 'tests', 'test_input')
    event_list = ['OGLE-2026-BLG-0001', 'OGLE-2026-BLG-0002']

    tnow = datetime.now(UTC)
    datasets = []

    for event in event_list:
        for optic in ['ip', 'gp']:
            red_dir = os.path.join(data_dir, event, 'sinistro', optic)
            if not os.path.isdir(red_dir):
                os.makedirs(red_dir)
            datasets.append(red_dir)

            for i in range(0, 10, 1):
                data = np.random.random((100, 100))
                hdu = fits.PrimaryHDU(data)
                hdu.header['DATE-OBS'] = datetime.strftime(
                    tnow - timedelta(days=i),
                    "%Y-%m-%dT%H:%M:%S.%f"
                )
                hdu.writeto(os.path.join(red_dir, event + '_' + str(i) + '.fits'), overwrite=True)

    # Third event has older data
    for optic in ['ip', 'gp']:
        red_dir = os.path.join(data_dir, 'OGLE-2026-BLG-0003', 'sinistro', optic)
        if not os.path.isdir(red_dir):
            os.makedirs(red_dir)
        datasets.append(red_dir)

        for i in range(11, 20, 1):
            data = np.random.random((100, 100))
            hdu = fits.PrimaryHDU(data)
            hdu.header['DATE-OBS'] = datetime.strftime(
                tnow - timedelta(days=i),
                "%Y-%m-%dT%H:%M:%S.%f"
            )
            hdu.writeto(os.path.join(red_dir, 'OGLE-2026-BLG-0003' + '_' + str(i) + '.fits'), overwrite=True)

    # Sort to ensure the same order
    datasets.sort()

    return datasets

def remove_test_data(datasets):
    """
    Function to remove the test datasets generated
    """

    for red_dir in datasets:
        dir_path = Path(red_dir)
        if dir_path.exists() and dir_path.is_dir():
            shutil.rmtree(dir_path)

def check_for_process(test_pid):
    """
    Function to check for a running process
    """

    got_proc = False
    for proc in psutil.process_iter(['pid']):
        if test_pid == proc.info['pid']:
            got_proc = True

    return got_proc

def wait_for_process(test_pid):
    """
    Function to wait until a given process is complete
    """

    got_proc = check_for_process(test_pid)

    # Wait until all test processes are finished, to avoid residual processes
    # causing false results for subsequent tests
    max_iter = 5
    i = 0
    while got_proc and i <= max_iter:
        time.sleep(0.5)  # Seconds
        got_proc = check_for_process(test_pid)
        i += 1

class TestReductionManager:

    @pytest.mark.parametrize("test_config,", [
        ({'data_reduction_dir': os.path.join(os.getcwd(), 'tests', 'test_input'),
          'instrument_list': ['sinistro', 'qhy'],
            'dataset_selection':
              {'group': 'all',
               'file': 'None',
               'start_date': 'None',
               'end_date': 'None',
               'ndays': 2}
          }),
        ({'data_reduction_dir': os.path.join(os.getcwd(), 'tests', 'test_input'),
          'instrument_list': ['sinistro', 'qhy'],
          'dataset_selection':
              {'group': 'date',
               'file': 'None',
               'start_date': datetime.strftime(
                   datetime.now(UTC) - timedelta(days=10),
                   "%Y-%m-%d"
               ),
               'end_date': datetime.strftime(datetime.now(UTC), "%Y-%m-%d"),
               'ndays': 2}
          }),
        ({'data_reduction_dir': os.path.join(os.getcwd(), 'tests', 'test_input'),
          'instrument_list': ['sinistro', 'qhy'],
          'dataset_selection':
              {'group': 'recent',
               'file': 'None',
               'start_date': 'None',
               'end_date': 'None',
               'ndays': 5}
          }),
        ({'data_reduction_dir': os.path.join(os.getcwd(), 'tests', 'test_input'),
          'instrument_list': ['sinistro', 'qhy'],
          'dataset_selection':
              {'group': 'file',
               'file': os.path.join(os.getcwd(), 'tests', 'test_input', 'test_reduce_datasets.txt'),
               'start_date': 'None',
               'end_date': 'None',
               'ndays': 0}
          }),
    ])
    def test_find_imaging_data_for_aperture_photometry(self, test_config):
        """
        Test the function to identify datasets for reduction based on the configured
        selection criteria
        """

        # Generate test data in the expected data structure:
        test_datasets = generate_test_red_dirs()

        # Test option 'all':
        if test_config['dataset_selection']['group'] == 'all':
            datasets = reduction_manager.find_imaging_data_for_aperture_photometry.fn(test_config, None)
            datasets.sort()
            assert datasets == test_datasets

        # Test option 'date' or 'recent':
        elif test_config['dataset_selection']['group'] == 'date' or \
            test_config['dataset_selection']['group'] == 'recent':
            test_datasets2 = [dpath for dpath in test_datasets if 'OGLE-2026-BLG-0003' not in dpath]

            datasets = reduction_manager.find_imaging_data_for_aperture_photometry.fn(test_config, None)
            datasets.sort()

            assert datasets == test_datasets2

        # Test option 'file':
        elif test_config['dataset_selection']['group'] == 'file':
            test_datasets3 = [dpath for dpath in test_datasets if 'OGLE-2026-BLG-0003' in dpath]

            datasets = reduction_manager.find_imaging_data_for_aperture_photometry.fn(test_config, None)
            datasets.sort()

            assert datasets == test_datasets3

        # Remove test data
        remove_test_data(test_datasets)

    def test_trigger_process(self):
        command = os.path.join(os.getcwd(), 'tests', 'task_process.py')
        arguments = [os.path.join(os.getcwd(), 'tests', 'test_output', 'count_file.txt')]

        log = None

        test_pid = reduction_manager.trigger_process.fn(command, arguments, log, wait=False)

        got_proc = check_for_process(test_pid)
        assert got_proc

        # Wait until all test processes are finished, to avoid residual processes
        # causing false results for subsequent tests
        wait_for_process(test_pid)

    def test_count_running_processes(self):

        # Establish the test process to check for
        command_name = 'task_process.py'
        command = os.path.join(os.getcwd(), 'tests', command_name)
        arguments = [os.path.join(os.getcwd(), 'tests', 'test_output', 'count_file.txt')]

        log = None

        test_pid = reduction_manager.trigger_process.fn(command, arguments, log, wait=False)

        # Count the running processes by name of instance
        nproc = reduction_manager.count_running_processes.fn(command_name, log=None)

        assert nproc == 1

        # Wait until all test processes are finished, to avoid residual processes
        # causing false results for subsequent tests
        wait_for_process(test_pid)
