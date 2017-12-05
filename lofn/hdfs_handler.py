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
Interact with HDFS.
"""

from __future__ import print_function, unicode_literals
import shutil
from contextlib import contextmanager
import os

from lofn.base import hdfs, tmp


@contextmanager
def setup_user_volumes_from_hdfs(volumes, **kwargs):
    """
    GET a local copy of the volume to mount in Docker and update and map
    the volumes dictionary with the new local temp directory.

    :param volumes: Docker volumes specification as a dictionary, similar
      to structure seen in DockerPy.
      User defined volumes to mount in the container.
      example:
      {'[host_directory_path]': {'bind': '[container_directory_path]',
      'mode': '[ro|rw] }}
      The host directory path in the dictionary must be the absolute path to
      the directory on HDFS.
    :keyword temporary_directory_parent: manually specify the parent directory
      or the temp files directories, else default is None which uses the
      system's TMP/TMPDIR.
    :return: a volumes dictionary mapping to the new temporary directories
    """

    temporary_directory_parent = kwargs.get('temporary_directory_parent', None)
    local_temp_dirs = []
    updated_volumes = {}
    try:
        for path in volumes.keys():
            tempdir = tmp.create_temp_directory(
                directory=temporary_directory_parent)
            hdfs.get(path, tempdir)
            local_temp_dirs.append(tempdir)
            new_path = os.path.join(tempdir, os.path.basename(path))
            updated_volumes[new_path] = volumes[path]
        yield updated_volumes
    finally:
        for directory in local_temp_dirs:
            shutil.rmtree(directory)
