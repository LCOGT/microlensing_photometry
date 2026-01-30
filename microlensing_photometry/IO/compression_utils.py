import os
import subprocess
from shutil import move
from microlensing_photometry.infrastructure import logs as lcologs

def funpack_frame(config, compressed_filename, log=None):
    """
    Utility function to uncompress a file that was compressed using fpack.

    Parameters
    ----------
    config :  Argparse object   Configuration parameters
    compressed_filename :  string         Name of the compressed file
    log : logger object       [optional] logging instance

    Returns
    -------
    status : Boolean            True if a valid uncompressed file is produced,
                                False if an error was encountered
    """

    unused_dir = os.path.join(config['data_download_dir'], 'unused')
    compressed_frame_path = os.path.join(config['data_download_dir'], compressed_filename)
    uncompressed_frame_path = compressed_frame_path.replace('.fz','')

    if os.path.isdir(unused_dir) == False:
        os.makedirs(unused_dir)

    if os.path.isfile(uncompressed_frame_path) or compressed_frame_path.split('.')[-1] in ['fits', 'fts']:
        lcologs.log('File ' + uncompressed_frame_path + ' already uncompressed', 'info', log=log)
        status = True

    elif compressed_frame_path.split('.')[-1] == 'fz':
        args = ['funpack', compressed_frame_path]
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        p.wait()

        new_frame = compressed_frame_path.replace('.fz','')

        if os.path.isfile(new_frame):
            if os.path.isfile(compressed_frame_path):
                os.remove(compressed_frame_path)
            lcologs.log('Decompressed '+os.path.basename(compressed_frame_path), 'info', log=log)
            status = True

        else:
            lcologs.log('ERROR decompressing '+os.path.basename(compressed_frame_path), 'info', log=log)

            if os.path.isfile(compressed_frame_path):
                move(compressed_frame_path, os.path.join(unused_dir, os.path.basename(compressed_frame_path)))

            status = False

    else:
        lcologs.log(
            'Error: Found unrecognized image compression extension: '+os.path.basename(compressed_frame_path),
            'info', log=log
        )

        move(compressed_frame_path, os.path.join(unused_dir, os.path.basename(compressed_frame_path)))
        status = False

    return status, uncompressed_frame_path
