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
Count words using lofn.
"""

from __future__ import print_function, unicode_literals
import argparse
import sys
from io import open

from pyspark import SparkConf, SparkContext
from lofn.api import DockerPipe


def command_line():
    """Collect and validate command line arguments."""
    class MyParser(argparse.ArgumentParser):
        """
        Override default behavior, print the whole help message for any CLI
        error.
        """

        def error(self, message):
            print('error: {}\n'.format(message), file=sys.stderr)
            self.print_help()
            sys.exit(2)

    parser = MyParser(description="Word Count with lofn")
    parser.add_argument('input_file', help="An input text file from which "
                                           "to count words")
    parser.add_argument('-o', '--output',
                        help="Output file name. Default is 'word_counts.txt",
                        default='word_counts.txt')
    return parser.parse_args()
arguments = command_line()


conf = SparkConf()
sc = SparkContext(conf=conf)

rdd = sc.textFile(arguments.input_file)
mappedRDD = DockerPipe(SparkConf()).map(
    image_name='ubuntu:xenial',
    command="cat /shared/input.txt | tr -s ' ' | tr ' ' '\\n' | sort | uniq "
            "-c | awk '\$1=\$1' > /shared/output.txt",
    rdd=rdd)


def transform(partition):
    """
    Nest each partition in a list so it gets reduced faster, otherwise
    it takes one pair of elements at a time to start.
    """

    return [(list(partition))]

mappedRDD = mappedRDD.mapPartitions(transform)
result = DockerPipe(SparkConf()).reduce(
    image_name='ubuntu:xenial',
    command="cat /shared/input.txt | awk '{cnt[\$2]==0 ? cnt[\$2]=\$1 : "
            "cnt[\$2]+=\$1} END {for (_ in cnt) print cnt[_],_}'"
            " | sort -nr > /shared/output.txt",
    rdd=mappedRDD)
sc.stop()


def top_word(word_count):
    """
    Find top word, surfacing ties. The input should already be sorted by
    count, most first.
    """

    top_words = []
    last_count = 0
    for each in word_count:
        if len(each.split()) < 2:
            # we compress spaces in our word count but sometimes they still
            # show up as a count, we won't list them as a top word though.
            continue
        word = each.split()[1]
        count = int(each.split()[0])
        if count >= last_count:
            top_words.append(word)
            last_count = count
        else:
            break
    return top_words, last_count

most_frequent, frequency = top_word(result)

print("Most frequent word(s) occurring {} times: {}".format(
    frequency, ', '.join(most_frequent)))
with open(arguments.output, 'w', encoding='utf-8') as output:
    output.write('frequency word\n')
    for each in result:
        output.write(each + '\n')
