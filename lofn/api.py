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
Wrapper to execute docker containers as spark tasks.
"""


from __future__ import print_function, unicode_literals
import os
import shutil
from contextlib import contextmanager
from getpass import getuser
import logging

from lofn.base import tmp, hdfs
from lofn import tmp_file_handler, docker_handler

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %('
                           'message)s')


class DockerPipe(object):
    """Main entry point for lofn."""

    def __init__(self, spark_configuration, **kwargs):
        """
        :param spark_configuration: spark configuration (e.g.
          pyspark.SparkConf())
        :keyword temporary_directory_parent: specify an absolute path to the
          parent directory to use for temp dirs and files. The default is None,
          which then uses a location specified by a platform-dependent list
          or uses environment variables TMPDIR, TEMP, or TMP. If specifying
          a path, it either needs to exist on all nodes or you must run it
          with appropriate permissions so lofn can attempt to create it.
        :keyword shared_mountpoint:  the path to the directory inside each
          docker container that maps to the temporary directory on the host.
          (Default '/shared/')
        :keyword volumes: User defined volumes to mount in the container.
          This is useful for data that is not being partitioned and needs to
          be read into each container, such as a global reference. This must
          be given as a dictionary. The keys for the dictionary are the
          absolute paths to the directory on the host you want to bind. The
          value of each of these keys is the information on how to bind that
          volume. Provide a 'bind' path, which is the absolute path in the
          container you want that volume to mount on, and optionally provide
          a 'mode', as ro (read-only, the default) or rw (read-write). The
          structure of this input is similar to docker-py volumes,
          and resembles the following structure:
          {'[host_directory_path]': {'bind': '[container_directory_path]',
          'mode': '[ro|rw] } }
        """

        self.shared_mountpoint = kwargs.get('shared_mountpoint', '/shared/')
        self.user_volume = kwargs.get('volumes')
        if self.user_volume:
            docker_handler.validate_user_input(self.user_volume)
        self.master_type = _get_master_type(spark_configuration)
        self.temporary_directory_parent = kwargs.get(
            'temporary_directory_parent', None)

    def _map_docker_files(self, image_name, command, **kwargs):
        records = kwargs.get('records')
        docker_options = kwargs.get('docker_options')
        input_mode = kwargs.get('input_mode')
        container_input_name = kwargs.get('container_input_name')
        container_output_name = kwargs.get('container_output_name')

        shared_temp_dir = tmp.create_temp_directory(
            directory=self.temporary_directory_parent)

        # write partition to temp file
        if 'map_udf' in kwargs.keys():
            user_defined_function = tmp_file_handler.UDF(
                temporary_directory=shared_temp_dir,
                user_function=kwargs['map_udf'],
                partition=records,
                input_mode=input_mode
            )
            user_defined_function.map_udf()
        elif 'reduce_udf' in kwargs.keys():
            user_defined_function = tmp_file_handler.UDF(
                temporary_directory=shared_temp_dir,
                user_function=kwargs['reduce_udf'],
                partition_1=kwargs['partition_1'],
                partition_2=kwargs['partition_2'],
                input_mode=input_mode
            )
            user_defined_function.reduce_udf()
        else:
            tmp.write_to_temp_file(records, os.path.join(
                shared_temp_dir, container_input_name))

        output_file = os.path.join(shared_temp_dir, container_output_name)

        # run docker
        status = docker_handler.run(
            image_name=image_name,
            command=command,
            bind_files=[self.shared_mountpoint],
            volume_files=[shared_temp_dir],
            docker_options=docker_options,
            volumes=self.user_volume,
            master_type=self.master_type,
            temporary_directory_parent=self.temporary_directory_parent)
        if status:  # only has a status message when it has failed
            raise docker_handler.DockerFailure(status)

        return shared_temp_dir, output_file

    @contextmanager
    def map_binary(self, rdd, image_name, command, **kwargs):
        """
        Map binary output as a context manager. This currently takes
        rdd as input and will output a directory of the
        newly written binary files so that they can be read by the user with
        sc.binaryFiles. After finishing with the context manager, the temp
        files are destroyed.

        :param rdd: spark RDD input
        :param image_name: docker image
        :param command: docker command to run
        :keyword container_input_name: the name of the file within the
          shared_mountpoint that is written to before the map steps from
          the host, and read from as the first step in the map in
          the container. (Default 'input.txt')
        :keyword container_binary_output_name: the name of the output file
          in map_binary step inside the container. This path will belong
          inside of the shared_mountpoint which maps to the host temp
          directory for that partition. (Default 'output.bin')
        :keyword docker_options: additional docker options
        :keyword map_udf: function that takes one input, the partition,
          and returns a dictionary of filename: contents (as iterable).
        :keyword hdfs_tempdir: temporary directory on HDFS to hold binary
          files. The default attempts to find the home directory for the user,
          but can be overridden by specifying an absolute path to use.
        :return: directory path containing the output binary files to be read
        """

        docker_options = kwargs.get('docker_options')
        container_input_name = kwargs.get('container_input_name', 'input.txt')
        container_binary_output_name = kwargs.get(
            'container_binary_output_name', 'output.bin')
        if self.master_type == 'standalone':
            binary_tempdir = tmp.create_temp_directory(
                directory=self.temporary_directory_parent)
        elif self.master_type == 'yarn':
            binary_tempdir = kwargs.get(
                'hdfs_tempdir', '/user/{user}/lofn_temp'.format(
                    user=getuser()))
        else:
            raise ValueError("Not a valid master_type")
        try:
            def apply_transformation(partition):
                """
                Apply to partition.
                :param partition: string (rdd partition)
                :return: path to parent directory of output files
                """
                if 'map_udf' in kwargs.keys():
                    # handle the partition writing to temp file via a user
                    # defined function
                    shared_tmp_dir, output = self._map_docker_files(
                        image_name=image_name, command=command,
                        records=partition, docker_options=docker_options,
                        map_udf=kwargs['map_udf'],
                        container_output_name=container_binary_output_name)
                else:
                    shared_tmp_dir, output = self._map_docker_files(
                        image_name=image_name, command=command,
                        records=partition, docker_options=docker_options,
                        container_input_name=container_input_name,
                        container_output_name=container_binary_output_name)
                return tmp_file_handler.handle_binary(
                    shared_tmp_dir, binary_tempdir, output, self.master_type)
            rdd.foreachPartition(apply_transformation)
            yield binary_tempdir
        finally:
            if self.master_type == 'yarn':
                hdfs.rm_r(binary_tempdir)
            elif self.master_type == 'standalone':
                shutil.rmtree(binary_tempdir)

    def map(self, rdd, image_name, command, **kwargs):
        """
        Map step by applying Spark's mapPartitions.
        This writes the partition to temp file(s) and executes a docker
        container to run the commands, which is read back into a new RDD.

        :param rdd: a spark RDD as input
        :param image_name: Docker image name
        :param command: Command to run in the docker container
        :keyword container_input_name: the name of the file within the
          shared_mountpoint that is written to before the map from
          the host, and read from as the first step in the map in
          the container. (Default 'input.txt')
        :keyword container_output_name: the name of the file the map step
          writes to inside the container. This path will belong
          inside of the shared_mountpoint which maps to the host temp
          directory for that partition. (Default 'output.txt')
        :keyword docker_options: additional docker options to provide for
          'docker run'. Must be a list.
        :keyword map_udf: optional keyword argument to pass a function that
          accepts a partition and transforms the data into a dictionary
          with a key as filename and value as contents, a list (iterable)
          of elements to write to that file within the temporary directory.
        :return: transformed RDD
        """

        docker_options = kwargs.get('docker_options')
        container_input_name = kwargs.get('container_input_name', 'input.txt')
        container_output_name = kwargs.get('container_output_name',
                                           'output.txt')

        def apply_transformation(partition):
            """
            Apply to partition.

            :param partition: iterable (RDD partition)
            :return: iterable (transformed partition)
            """

            if 'map_udf' in kwargs.keys():
                # handle the partition writing to temp file via a user
                # defined function
                shared_tmp_dir, output = self._map_docker_files(
                    image_name=image_name, command=command, records=partition,
                    docker_options=docker_options, map_udf=kwargs['map_udf'],
                    container_output_name=container_output_name)
            else:
                shared_tmp_dir, output = self._map_docker_files(
                    image_name=image_name, command=command, records=partition,
                    docker_options=docker_options,
                    container_input_name=container_input_name,
                    container_output_name=container_output_name)
            return tmp_file_handler.read_back(shared_tmp_dir, output)

        return rdd.mapPartitions(apply_transformation)

    def reduce(self, rdd, image_name, command, **kwargs):
        """
        Apply a Spark reduce function to the input RDD. This will take
        rolling pairs and write to temp files, run a docker container,
        execute a command in the container over the temp files, write to
        temp files, and return the result.

        :param rdd: a spark RDD as input
        :param image_name: Docker image name
        :param command: Command to run in the docker container
        :keyword container_input_name: the name of the file within the
          shared_mountpoint that is written to before the reduce from
          the host, and read from as the first step in reduce in
          the container. (Default 'input.txt')
        :keyword container_output_name: the name of the file the reduce
          step writes to inside the container. This path will belong
          inside of the shared_mountpoint which maps to the host temp
          directory for that partition. (Default 'output.txt')
        :keyword docker_options: additional docker options to provide for
          'docker run'. Must be a list.
        :keyword reduce_udf: The default behavior for handling the pairs of
          partitions is to append right to left and write to one temp
          file. This can be overridden by supplying a 'reduce_udf'
          function that takes two inputs, the pair of partitions,
          and transforms them to return a dictionary mapping a key of
          filename to value of contents in a list (iterable) of elements
          to write to a file within the temp directory.
        """

        docker_options = kwargs.get('docker_options')
        container_input_name = kwargs.get('container_input_name', 'input.txt')
        container_output_name = kwargs.get('container_output_name',
                                           'output.txt')

        def combine(left, right):
            """
            combine partitions a pair at a time, rolling result.

            Default behavior is to append partitions to each other,
            otherwise a 'reduce_udf' is accepted.
            """
            if not isinstance(left, list):
                left = [left]
            if not isinstance(right, list):
                right = [right]
            both = left + right
            if 'reduce_udf' in kwargs.keys():
                # handle the two partitions writing to temp files via a user
                # defined function
                shared_tmp_dir, output = self._map_docker_files(
                    image_name=image_name,
                    command=command,
                    docker_options=docker_options,
                    partition_1=left,
                    partition_2=right,
                    reduce_udf=kwargs['reduce_udf'],
                    container_input_name=container_input_name,
                    container_output_name=container_output_name)
            else:
                shared_tmp_dir, output = self._map_docker_files(
                    image_name=image_name, command=command, records=both,
                    docker_options=docker_options,
                    container_input_name=container_input_name,
                    container_output_name=container_output_name)
            return tmp_file_handler.read_back(shared_tmp_dir, output)

        result = rdd.reduce(combine)
        return result

    def reduce_binary(self, rdd, image_name, command, **kwargs):
        """
        Reduce partitions from map_binary output. The format of these
        partitions is different so this handles them and also writes the
        temp files as one string rather than trying to split newlines since
        these are binary.

        :param rdd: spark RDD
        :param image_name: docker image
        :param command: docker command
        :keyword container_binary_input_1_name: the name of the left side
          file in reduce_binary step inside the container. This path will
          belong inside of the shared_mountpoint which maps to the host temp
          directory for that partition. (Default 'input_1.bin')
        :keyword container_binary_input_2_name: the name of the right side
          file in reduce_binary step inside the container. This path will
          belong inside of the shared_mountpoint which maps to the host temp
          directory for that partition. (Default 'input_2.bin')
        :keyword container_binary_output_name: the name of the output file
          in reduce_binary step inside the container. This path will belong
          inside of the shared_mountpoint which maps to the host temp
          directory for that partition. (Default 'output.bin')
        :keyword docker_options: additional docker options
        :keyword reduce_udf: default behavior for reduce_binary is to
          write each input as a temp file to give two binary input files to the
          container. Write a UDF here that takes two inputs and outputs a
          dictionary mapping filename: contents (a string)
        :return: iterable of reduce results
        """

        docker_options = kwargs.get('docker_options')
        container_binary_input_1_name = kwargs.get(
            'container_binary_input_1_name', 'input_1.bin')
        container_binary_input_2_name = kwargs.get(
            'container_binary_input_2_name', 'input_2.bin')
        container_binary_output_name = kwargs.get(
            'container_binary_output_name', 'output.bin')

        def combine(left, right):
            """
            Apply to two partitions. Must be associative and commutative.
            Since this has to unpack the binaryFiles type of partition we
            need to check if the return was from a previous combine or
            directly from the input RDD.

            :param left: left partition
            :param right: right partition
            :return: str of results
            """

            if len(left) > 1:
                if "file" in left[0] or "hdfs" in left[0]:
                    left = left[1]
            if len(right) > 1:
                if "file" in right[0] or "hdfs" in right[0]:
                    right = right[1]
            if 'reduce_udf' in kwargs.keys():
                shared_tmp_dir, output = self._map_docker_files(
                    image_name=image_name,
                    command=command,
                    docker_options=docker_options,
                    partition_1=left,
                    partition_y=right,
                    reduce_udf=kwargs['reduce_udf'],
                    input_mode='binary',
                    container_output_name=container_binary_output_name)
            else:
                def default_reduce_temp_files(part_1, part_2):
                    """
                    Apply attributes as names for files within container.
                    Instead of appending like in `reduce`, both partitions
                    are supplied for the reduce_binary function to combine,
                    so this is overriding that behavior and creating this as
                    the default if a reduce_udf is not specific.

                    :param part_1: left partition
                    :param part_2: right partition
                    :return: dict mapping file name in container as key to
                      the contents to be written to temp files.
                    """

                    return {container_binary_input_1_name: part_1,
                            container_binary_input_2_name: part_2}
                shared_tmp_dir, output = self._map_docker_files(
                    image_name=image_name,
                    command=command,
                    docker_options=docker_options,
                    partition_1=left,
                    partition_2=right,
                    reduce_udf=default_reduce_temp_files,
                    input_mode='binary',
                    container_output_name=container_binary_output_name
                )
            return tmp_file_handler.read_binary(shared_tmp_dir, output)
        return rdd.reduce(combine)


def _get_master_type(configuration):
    """
    Set the master type for lofn to use by reading the SparkConf() dictionary.
    Fallback to standalone if no master is set.

    :param configuration: SparkConf()
    :return: master_type (standalone, yarn)
    """

    master = configuration.get('spark.master')
    if master:
        if 'spark' in master or 'local' in master:
            master_type = 'standalone'
        elif 'yarn' in master:
            master_type = 'yarn'
        else:
            raise ValueError("Not a supported spark master.")
    else:
        logging.warning("No master type set in the spark configuration, "
                        "attempting to run as standalone.")
        master_type = 'standalone'
    return master_type
