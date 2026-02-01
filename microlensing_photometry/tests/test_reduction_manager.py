from datetime import datetime, timedelta, UTC
import pytest
import os
import shutil
from pathlib import Path
from astropy.io import fits
import numpy as np
from microlensing_photometry.microlensing_photometry.infrastructure import reduction_manager

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

        # Test option 'date':
        elif test_config['dataset_selection']['group'] == 'date':
            test_datasets2 = [dpath for dpath in test_datasets if 'OGLE-2026-BLG-0003' not in dpath]

            datasets = reduction_manager.find_imaging_data_for_aperture_photometry.fn(test_config, None)
            datasets.sort()

            assert datasets == test_datasets2

        # Test option 'recent':

        # Test option 'file':

        # Remove test data
        remove_test_data(test_datasets)