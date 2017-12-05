#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Eli Lilly and Company
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Base functions and classes for using temp files.
"""

from __future__ import print_function, unicode_literals
import os
import tempfile
import logging
from io import open  # pylint: disable=redefined-builtin

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %('
                           'message)s')


def write_to_temp_file(iterable, path):
    """
    Write iterable to temporary file, an item per line.

    :param iterable: list or tuples of contents to write to file
    :param path: absolute path to file. We use a tempdir as parent.
    :return: path to temporary file
    """

    with open(path, 'w', encoding='utf-8') as file_handle:
        for each in iterable:
            file_handle.write(each + "\n")
    return path


def write_binary_to_temp_file(data, path):
    """Write string directly to file for binary data."""
    with open(path, 'wb') as file_handle:
        file_handle.write(data)
    return path


def create_temp_directory(directory=None):
    """
    Create a temporary directory. Supports setting a parent directory that
    is not temporary and will try to create it if it does not already exist.

    :param directory: specify a parent directory instead of using default
      (None -> /tmp)
    :return: path to directory
    """

    if directory:
        try:
            os.makedirs(directory)
        except OSError as error:
            # py3 raises specifically named exceptions that are subclasses
            # of OSError, but they do not exist in py2 so this is more
            # compatible
            if error.errno == 17:
                # error number 17 file exists error
                pass
            elif error.errno == 13:
                # error number 13 is permission denied
                logging.exception('Cannot create the specified '
                                  'temporary_directory, %s, permission '
                                  'denied.', directory)
                raise error
            else:
                raise error
    return tempfile.mkdtemp(dir=directory)
