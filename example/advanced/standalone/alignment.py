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
Align with gsnap and then merge distributed output with samtools.
"""

from pyspark import SparkConf, SparkContext

from lofn.api import DockerPipe

conf = SparkConf()
sc = SparkContext(conf=conf)

rdd_1 = sc.textFile("/data/rnaseq/GSE67835/SRR1975008_1.fastq")
rdd_2 = sc.textFile("/data/rnaseq/GSE67835/SRR1975008_2.fastq")


def grouper(x):
    """
    Given a line number in a fastq file, return a tuple
        mapping each line to a read number (modulo 4) and
        line number
    """
    return int(x[1] / 4), x[0]


def replace_key_with_read_id(x):
    """
    now that data is grouped by read properly, let's replace the key with
    the read ID instead of file line number to make sure joins are correct.
    """
    return x[1][0], x[1]

index_rdd_1 = rdd_1.zipWithIndex().map(grouper).groupByKey().mapValues(
    list).map(replace_key_with_read_id)
index_rdd_2 = rdd_2.zipWithIndex().map(grouper).groupByKey().mapValues(
    list).map(replace_key_with_read_id)

joined_rdd = index_rdd_1.join(index_rdd_2)


def map_udf(partition):
    """
     Each partition in this pairedRDD is of the following form:
        [
         (index, ([read_1], [read_2])),
         (index, ([read_1], [read_2]))
        ]
    """
    return {'input_1.txt': ('\n'.join(p[1][0]) for p in partition),
            'input_2.txt': ('\n'.join(p[1][1]) for p in partition)}


with DockerPipe(
    SparkConf(),
    volumes={'/data/gmap': {'bind': '/data/gmap'}}).map_binary(
    image_name='gsnap_samtools',
    command="gsnap -B 5 -A sam -N 1 -t 4 -s splicesites "
            "--sam-multiple-primaries --maxsearch=1000 --npaths=100 -D "
            "/data/gmap -d ref_genome /shared/input_1.txt  "
            "/shared/input_2.txt > /shared/output.txt; "
            "samtools view /shared/output.txt -b -o /shared/output.bin",
    docker_options=['--ipc=host'],
    rdd=joined_rdd,
    map_udf=map_udf
        ) as bamfiles:
    rdd = sc.binaryFiles(bamfiles)
    results = DockerPipe(SparkConf()).reduce_binary(
        rdd=rdd,
        command='samtools merge /shared/output.bin /shared/input_1.bin '
                '/shared/input_2.bin',
        image_name='gsnap_samtools')


with open('SRR1975008.bam', 'wb') as fh:
    fh.write(results)

sc.stop()
