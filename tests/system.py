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
Test that the system is ready to run lofn.
"""

from __future__ import print_function, unicode_literals
import unittest

from whichcraft import which


def application_is_installed(application):
    return which(application) is not None


class SystemReady(unittest.TestCase):
    """
    Check if everything is installed for lofn to run.
    """

    def test_docker_installed(self):
        self.assertTrue(application_is_installed('docker'))

    def test_java_installed(self):
        self.assertTrue(application_is_installed('java'))

    def test_spark_installed(self):
        self.assertTrue(application_is_installed('spark-submit'))


def main():
    unittest.main()


if __name__ == "__main__":
    main()
