import logging
from os import path, remove, makedirs
from sys import exit
from astropy.time import Time
from datetime import datetime
import glob


def start_log(log_dir, log_name):
    """
    Function to initialize a log file for a single entity.  The new file will automatically overwrite any
    previously-existing logfile with the same name

    This function also configures the log file to provide timestamps for
    all entries.

    Parameters
    ----------
    log_dir : str, the path of the log file
    log_name : str, the name of the log file

    Returns
    -------
    log : logger, an open logger object
    """

    # Console output not captured, though code remains for testing purposes
    console = False

    if path.isdir(log_dir) == False:
        makedirs(log_dir)

    log_file = path.join(log_dir, log_name + '.log')

    if path.isfile(log_file) == True:
        remove(log_file)

    # To capture the logging stream from the whole script, create
    # a log instance together with a console handler.  
    # Set formatting as appropriate.
    log = logging.getLogger('analyst_'+log_name)

    if len(log.handlers) == 0:
        log.setLevel(logging.INFO)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        if console == True:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(fmt='%(asctime)s %(message)s', \
                                      datefmt='%Y-%m-%dT%H:%M:%S')
        file_handler.setFormatter(formatter)

        if console == True:
            console_handler.setFormatter(formatter)

        log.addHandler(file_handler)
        if console == True:
            log.addHandler(console_handler)

    log.info('Process start with opening log for ' + log_name + '\n')

    return log

# Convenience functions to add logging if a log is given, to allow the pipeline
# to be more easily tested with and without logs
def log(report, report_type, log=None):
    if log and report_type == 'info':
        log.info(report)
    if log and report_type == 'warning':
        log.warning(report)
    if log and report_type == 'error':
        log.error(report)

def ifverbose(log, setup, string):
    """Function to write to a logfile only if the verbose parameter in the
    metadata is set to True"""

    ### Not sure here, I comment

    #if log != None and setup.verbosity >= 1:
    #    log.info(string)

    #if setup.verbosity == 2:

    #    try:

    #        print(string)

    #    except IOError:

    #        pass


def close_log(log):
    '''
    Function that closes a log.

    Parameters
    ----------
    log: logger, a logger instance to close
    '''

    #log.info( 'Processing complete\n' )
    #logging.shutdown()

    for handler in log.handlers:
        log.info('Processing complete.\n')
        if isinstance(handler, logging.FileHandler):
            handler.close()
        log.removeFilter(handler)

