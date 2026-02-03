# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 12:52:34 2017

@author: rstreet
"""
from os import getcwd, path, remove
from sys import path as systempath
import logging

cwd = getcwd()
systempath.append(path.join(cwd, '../'))

from image_reduction.infrastructure import logs as lcologs


def test_logs():
    """Function to test the initialization and closing of a stage log"""

    log = lcologs.start_log(cwd, 'test_log')

    chk_log = logging.getLogger('test')
    log_name = path.join(cwd, 'test_log.log')

    assert type(log) == type(chk_log)

    assert path.isfile(log_name)

    lcologs.close_log(log)

    file_lines = open(log_name, 'r').readlines()

    assert len(file_lines) > 0

    assert 'Processing complete' in file_lines[-2]

    remove(log_name)
