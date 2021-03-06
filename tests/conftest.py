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
Pytest conf for arg parsing.
"""


def pytest_addoption(parser):
    parser.addoption("--pyfile", action="store", default='None',
                     help="Point to egg or zipball of python package to "
                          "distribute to Spark executors on YARN. This is "
                          "not required if lofn is already installed on all "
                          "nodes of the cluster.")

