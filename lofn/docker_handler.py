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
Run Docker containers.
"""

from __future__ import print_function, unicode_literals
import logging
import subprocess

from lofn import hdfs_handler

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %('
                           'message)s')


def run(image_name, command, bind_files, volume_files, **kwargs):
    """
    Make system calls with subprocess to run our containers.

    :param image_name: docker image to run a container
    :param command: docker command to run in container
    :param bind_files: container paths to set as mount point
    :param volume_files: host paths to mount
    :keyword docker_options: additional options to pass to docker run as a list
    :keyword temporary_directory_parent: specify the parent directory for
      temporary directories and files. Must exist or have permission to
      create the directory. The default is None, which uses the a default
      from a platform-dependent list or the system's environment variable
      for TMP, TMPDIR, or TEMPDIR.
    :return: status of execution, False is no issues otherwise returns
      failure message.
    """

    def build(user_volume_statement=None):
        """
        Build the bash command to run Docker with appropriate bindings and
        file names.

        :param user_volume_statement: docker commands for mounting the user
        volume. This can be None if there is not a user defined volume.
        :return: string to be run in terminal to run a docker container
        """
        arguments = ['docker', 'run']
        if docker_options:
            if not isinstance(docker_options, list):
                raise TypeError("docker_options must be a list")
            arguments += docker_options
        arguments += build_volumes()
        if user_volume_statement:
            arguments += user_volume_statement
        arguments += [image_name, 'bash -c "{command}"'.format(
            command=command)]
        logging.debug(' '.join(arguments))
        return ' '.join(arguments)

    def build_volumes():
        """Build docker volume binding statement"""
        bindings = []
        for idx, volume_path in enumerate(volume_files):
            bind = '{host}:{container}:rw'.format(
                host=volume_path, container=bind_files[idx])
            bindings.append('-v')
            bindings.append(bind)
        return bindings

    def user_volumes(user_defined_volumes):
        """Build docker volume binding statement for user defined
         directories"""
        bindings = []
        for key, value in user_defined_volumes.items():
            bind = '{host}:{container}:{mode}'.format(
                host=key, container=value['bind'], mode=value['mode']
            )
            bindings.append('-v')
            bindings.append(bind)
        return bindings

    docker_options = kwargs.get('docker_options')
    master_type = kwargs.get('master_type')
    volumes = kwargs.get('volumes')
    if volumes:
        if master_type == 'yarn':
            with hdfs_handler.setup_user_volumes_from_hdfs(
                volumes,
                temporary_directory_parent=kwargs.get(
                    'temporary_directory_parent', None)
            ) as new_volumes:
                # make local copies to mount and delete when finished
                command = build(user_volumes(new_volumes))
                return execute(command)
        else:
            command = build(user_volumes(volumes))
            return execute(command)
    else:
        command = build()
        return execute(command)


def execute(command):
    """
    Use the shell to invoke Docker. Catch exceptions and return to master
    to be caught and reported helpfully.

    :param command: bash command to call docker with requested configuration
    :return: if failure, the message about why it failed is returned,
      otherwise it returns False
    """
    try:
        # stderr as stdout should help make different versions of python all
        #  use CalledProcessError.output for the message returned
        subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return False
    except subprocess.CalledProcessError as error:
        failure = "The docker container failed with: {}".format(error.output)
        return failure


class DockerFailure(Exception):
    """Custom exception to communicate the container failed."""
    pass


def validate_user_input(volumes):
    """
    Validate the user input. Checks the type and structure.
    """
    # we require the format like docker-py, volumes={'host_dir': {
    # 'bind': 'container_dir', 'mode': 'rw/ro'}}
    if not isinstance(volumes, dict):
        raise TypeError('The keyword argument "volumes" must be a '
                        'dictionary.')
        # we require the format like docker-py, volumes={'host_dir': {
        # 'bind': 'container_dir', 'mode': 'rw/ro'}}
    binding_keys = ['bind', 'mode']
    updated_volumes = {}
    for key, value in volumes.items():
        provided_binding_keys = [_.lower() for _ in value.keys()]
        illegal_keys = set(provided_binding_keys) - set(binding_keys)
        if illegal_keys:
            raise ValueError("'bind' and 'mode' are the only allowed keys "
                             "to define a volume binding options.")
        if 'mode' not in provided_binding_keys:
            set_default_mode = {'mode': 'ro'}
            value.update(set_default_mode)
        new_value = {k.lower(): v for k, v in value.items()}
        updated_volumes[key] = new_value
    return updated_volumes
