from os import path, makedirs
import shutil
import glob
import argparse
from image_reduction.IO import tom_utils
from image_reduction.infrastructure import logs as lcologs

def upload_lcs(args):

    log = lcologs.start_log(args.data_dir, 'tom_upload')

    # Make a list of .csv format files to upload in the data_dir
    file_list = glob.glob(path.join(args.data_dir, '*.csv'))

    # Create a holding subdirectory for files once uploaded
    uploaded_dir = path.join(args.data_dir, 'uploaded')
    if not path.isdir(uploaded_dir):
        makedirs(uploaded_dir, exist_ok=True)

    # Upload each file, extracting the necessary parameters to
    # associate the data with the right target in the TOM
    for lc_path in file_list:
        file_name = path.basename(lc_path)
        target_name = file_name.split('_')[0]
        data_label = file_name.split('_')[1]

        params = {
            'file_path': lc_path,
            'red_dir': args.data_dir,
            'data_label': data_label,
            'target_name': target_name,
            'tom_config_file': args.tom_config_file
        }

        print(params)
        tom_utils.upload_lightcurve(params, log=log)

        # Move uploaded files to subdirectory
        shutil.move(lc_path, path.join(uploaded_dir, file_name))

        lcologs.close_log(log)


def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir', help='Path to the reduction directory')
    parser.add_argument('tom_config_file', help='Path to TOM configuration file')
    args = parser.parse_args()

    return args


if __name__ == '__main__':

    args = get_args()
    upload_lcs(args)
