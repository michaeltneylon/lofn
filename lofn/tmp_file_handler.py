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
Create temp files for Docker read/write
"""

from __future__ import print_function, unicode_literals
import os
import shutil
import tempfile
from io import open  # pylint: disable=redefined-builtin

from lofn.base import hdfs, tmp


class UDF(object):
    """
    Define how to handle RDD partitions to temp files.

    The return should be a dictionary, with filename as key and list of
    elements as value. These files are written inside of the shared
    mount point temporary directory.

    These file names override the input container name.
    """

    def __init__(self, temporary_directory, user_function, **kwargs):
        self.temporary_directory = temporary_directory
        self.user_function = user_function
        if 'partition' in kwargs.keys():
            self.partition = kwargs['partition']
        if 'partition_1' and 'partition_2' in kwargs.keys():
            self.partition_1 = kwargs['partition_1']
            self.partition_2 = kwargs['partition_2']
        if 'input_mode' in kwargs.keys():
            self.mode = kwargs['input_mode']
            #  allow a mode of iterate vs whole, to allow non iterable
            #  input like binary which is one item only.

    def write_temp_files(self, inner_partitions):
        """
        Write contents to temp file with defined name. Iterables are written
        line by line, while binary data is written as a single string.

        :param inner_partitions: the content to write, either as iterable
          for regular files or string for binary data
        """

        if not isinstance(inner_partitions, dict):
            raise TypeError("The user defined function must return a "
                            "dictionary mapping file names to a list of "
                            "elements to write")
        for key, value in inner_partitions.items():
            temp_file = os.path.join(self.temporary_directory, key)
            if self.mode and self.mode.lower() == 'binary':
                tmp.write_binary_to_temp_file(value, temp_file)
            else:
                tmp.write_to_temp_file(value, temp_file)

    def map_udf(self):
        """Unpack a partition into multiple files based on a user defined
        function. The udf should return a dictionary, with filename as key
        and list of elements as value.These files are written inside of the
        shared mountpoint temporary directory.

        These filenames override the input container name.
        """

        inner_partitions = self.user_function(list(self.partition))
        self.write_temp_files(inner_partitions)

    def reduce_udf(self):
        """
        Define handling of pairs of partitions for the reduce step.
        Pass a function to handle the pair of partitions input and return a
        dictionary mapping file name as key and value as an iterable of
        contents to write to the temp file.

        This will override the input container name.
        """

        inner_partitions = self.user_function(self.partition_1,
                                              self.partition_2)
        self.write_temp_files(inner_partitions)


def handle_binary(origin_directory, destination_directory, input_path,
                  master_type):
    """
    Move, rename, and keep temp file outputs into one directory to be
    read back by user with sc.binaryFiles() and then remove the original
    temp directory used by the containers.

    :param origin_directory: temporary directory mounted by container
    :param destination_directory: the shared temporary directory, either
      locally or on hdfs, for all the output files to be moved
    :param input_path: the full path to the file to be moved to on HDFS
    :param master_type: spark master type, yarn or standalone
    :return: path to new file
    """

    if master_type == 'yarn':
        hdfs.mkdir_p(destination_directory)
        destination_path = os.path.join(destination_directory, next(
            tempfile._get_candidate_names()  # pylint: disable=protected-access
        ))
        hdfs.put(input_path, destination_path)
    elif master_type == 'standalone':
        destination_path = tempfile.mkstemp(dir=destination_directory)[1]
        shutil.move(input_path, destination_path)
    else:
        raise ValueError("Not a valid master_type")
    shutil.rmtree(origin_directory)
    return destination_path


def read_back(shared_dir, output_file):
    """
    Read text files back into an iterable from temp file then destroy the
    temp file and its parent directory.

    :param shared_dir: the temporary directory that the container mounted
    :param output_file: path to output filename
    :return: iterable of output file contents
    """

    with open(output_file, 'r', encoding='utf-8') as file_handle:
        output = [x.rstrip('\r\n') for x in file_handle.readlines()]
    shutil.rmtree(shared_dir)
    return output


def read_binary(shared_dir, output_file):
    """
    Read back binary file output from container into one string, not an
    iterable. Then remove the temporary parent directory the container
    mounted.

    :param shared_dir: temporary directory container mounted
    :param output_file: path to output file
    :return: str of file contents
    """

    with open(output_file, 'rb') as file_handle:
        output = file_handle.read()
    shutil.rmtree(shared_dir)
    return output
