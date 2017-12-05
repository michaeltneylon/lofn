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
Base classes and functions to interact with HDFS.
"""

from __future__ import print_function, unicode_literals
import logging
import subprocess

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %('
                           'message)s')


def get(remote, local):
    """Get from HDFS"""
    logging.debug("Getting %s from HDFS and placing locally at: %s", remote,
                  local)
    subprocess.call(['hadoop', 'fs', '-get', '{}'.format(
        remote), '{}'.format(local)])


def put(local, remote):
    """Put into HDFS"""
    logging.debug("Putting %s into HDFS at: %s", local, remote)
    subprocess.call(['hadoop', 'fs', '-put', '{}'.format(
        local), '{}'.format(remote)])


def mkdir_p(path):
    """Make directory with parents if they don't exist"""
    logging.debug("Creating %s on HDFS", path)
    subprocess.call(['hadoop', 'fs', '-mkdir', '-p', '{}'.format(path)])


def rm_r(path):
    """Remove recursively"""
    logging.debug("Removing recursively: %s", path)
    subprocess.call(['hadoop', 'fs', '-rm', '-r', '{}'.format(path)])
