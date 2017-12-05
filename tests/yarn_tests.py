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
Test lofn functionality when deploying Spark on YARN.
"""

from __future__ import print_function, unicode_literals
import pytest
import logging
import os
import shutil
import tempfile
import subprocess
import getpass
import sys

import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from lofn.api import DockerPipe


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="module")
def spark_setup(request):
    """
    Fixture for spark context on yarn
    """
    conf = (SparkConf().setMaster("yarn")
            .setAppName("pytest-pyspark-yarn-testing")
            .set('spark.submit.deployMode', 'client')
            .set("spark.executor.memory", "1g")
            .set('spark.yarn.executor.memoryOverhead', '500m')
            .set("spark.executor.instances", "2")
            .set("spark.executor.cores", "1")
            )
    sc = SparkContext(conf=conf)
    # we set a default of None for pyfile so a user can submit tests without
    #  the --pyfile flag if lofn is already installed on all nodes,
    # otherwise the path to the package must exist (as a zipball or egg)
    if request.config.getoption('--pyfile').lower() != 'none':
        if not os.path.exists(os.path.abspath(request.config.getoption(
                '--pyfile'))):
            raise OSError("File not found: {}".format(request.config.getoption(
                '--pyfile')))
        sc.addPyFile(os.path.abspath(request.config.getoption('--pyfile')))
    request.addfinalizer(lambda: sc.stop())
    quiet_py4j()
    return sc, conf


def test_basic_example(spark_setup):
    """
    Check that lofn can do a simple map and reduce with correct output.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'test', 'test', 'words'])
    mapped_rdd = DockerPipe(conf).map(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | tr -s ' ' | tr ' ' '\\n' | sort | "
                "uniq -c | awk '\$1=\$1' > /shared/output.txt",
        rdd=rdd)

    result = DockerPipe(conf).reduce(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | awk '{cnt[\$2]==0 ? cnt[\$2]=\$1 : "
                "cnt[\$2]+=\$1} END {for (_ in cnt) print cnt[_],_}'"
                " | sort -nr > /shared/output.txt",
        rdd=mapped_rdd)
    top_word = result[0].split()[1]
    top_word_count = int(result[0].split()[0])
    assert top_word == 'test'
    assert top_word_count == 3


def test_custom_name(spark_setup):
    """
    Check that lofn can do a simple map and reduce with custom temp file names
    inside the container.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'test', 'test', 'words'])
    mapped_rdd = DockerPipe(conf).map(
        image_name='ubuntu:xenial',
        container_input_name='foo.txt',
        container_output_name='bar.txt',
        command="cat /shared/foo.txt | tr -s ' ' | tr ' ' '\\n' | sort | "
                "uniq -c | awk '\$1=\$1' > /shared/bar.txt",
        rdd=rdd)

    result = DockerPipe(conf).reduce(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | awk '{cnt[\$2]==0 ? cnt[\$2]=\$1 : "
                "cnt[\$2]+=\$1} END {for (_ in cnt) print cnt[_],_}'"
                " | sort -nr > /shared/output.txt",
        rdd=mapped_rdd)
    top_word = result[0].split()[1]
    top_word_count = int(result[0].split()[0])
    assert top_word == 'test'
    assert top_word_count == 3


def test_user_volume_bad_input_format(spark_setup):
    """
    Test exception case of user volume input, failing to provide a dictionary.
    """
    sc, conf = spark_setup
    tempdir = tempfile.mkdtemp()
    temp_input_file = os.path.join(tempdir, 'test.txt')
    with open(temp_input_file, 'w') as f:
        f.write("a\nb\nc")
    rdd = sc.parallelize(['a', 'b'])
    with pytest.raises(TypeError):
        DockerPipe(conf, volumes=(tempdir, ('bind', '/user_volume/'))
    ).map(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt > /shared/output.txt; cat "
                "/user_volume/test.txt "
                ">> /shared/output.txt",
        rdd=rdd)
    shutil.rmtree(tempdir)


def test_user_volume_illegal_input(spark_setup):
    """
    Test exception case of user volume input, using invalid keyword 'binding'
    instead of 'bind'.
    """
    sc, conf = spark_setup
    tempdir = tempfile.mkdtemp()
    temp_input_file = os.path.join(tempdir, 'test.txt')
    with open(temp_input_file, 'w') as f:
        f.write("a\nb\nc")
    rdd = sc.parallelize(['a', 'b'])
    with pytest.raises(ValueError):
        DockerPipe(
            conf, volumes={tempdir: {'binding': '/user_volume/'}}
        ).map(
            image_name='ubuntu:xenial',
            command="cat /shared/input.txt > /shared/output.txt; cat "
                    "/user_volume/test.txt "
                    ">> /shared/output.txt",
            rdd=rdd)
    shutil.rmtree(tempdir)


def test_map_udf(spark_setup):
    """
    Test map and UDF for writing temp files.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'words'])

    def map_udf(input_split):
        part1 = input_split[0][0]
        part2 = input_split[0][1:]
        return {'1.txt': part1, '2.txt': part2}

    mapped_rdd = DockerPipe(conf).map(
        image_name='ubuntu:xenial',
        command="cat /shared/1.txt > /shared/output.txt",
        rdd=rdd,
        map_udf=map_udf)
    result = DockerPipe(conf).reduce(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | sort > /shared/output.txt",
        rdd=mapped_rdd
    )
    assert result == ['t', 'w']


def test_reduce_udf(spark_setup):
    """
    Test reduce UDF for writing temp files.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'words'])

    def reduce_udf(part_1, part_2):
        output = [part_1[0][0]] + [part_2[0][0]]
        return {'input.txt': output}

    mapped_rdd = DockerPipe(conf).map(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt > /shared/output.txt",
        rdd=rdd)
    result = DockerPipe(conf).reduce(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | sort > /shared/output.txt",
        rdd=mapped_rdd,
        reduce_udf=reduce_udf
    )
    assert result == ['t', 'w']


def test_user_volume_on_yarn(spark_setup):
    """
    Test user volume support on yarn/hdfs. These are volumes that are
    mounted in docker
    for a whole file reference, shared among all mappers and reducers.
    """
    sc, conf = spark_setup
    tempdir = tempfile.mkdtemp()
    temp_input_file = os.path.join(tempdir, 'test.txt')
    with open(temp_input_file, 'w') as f:
        f.write("a\nb\nc")
    subprocess.call(['hadoop', 'fs', '-put', '{}'.format(tempdir)])
    user = getpass.getuser()
    hadoop_target = os.path.join('/user/', user + '/')
    hadoop_path = os.path.join(hadoop_target,
                               os.path.basename(tempdir))
    rdd = sc.parallelize(['a', 'b'])
    mapped_rdd = DockerPipe(
        conf,
        volumes={hadoop_path: {'bind': '/user_volume/'}}
    ).map(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt > /shared/output.txt; cat "
                "/user_volume/test.txt "
                ">> /shared/output.txt",
        rdd=rdd)

    result = DockerPipe(conf).reduce(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | sort > /shared/output.txt",
        rdd=mapped_rdd
    )
    shutil.rmtree(tempdir)
    subprocess.call(['hadoop', 'fs', '-rm', '-r', '{}'.format(hadoop_path)])
    assert result == ['a', 'a', 'a', 'b', 'b', 'b', 'c', 'c']


def test_binary_map_context_manager(spark_setup):
    """
    Test lofn binary map context manager. We want a context manager
    because spark's rdd binary file function takes a dictionary of files then
    reads them in, so we want to keep temp files until these are read
     then destroy the temporary directory.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'word'])
    new_object = DockerPipe(conf)
    with new_object.map_binary(
        rdd=rdd,
        image_name='ubuntu:xenial',
        command='cat /shared/input.txt > /shared/output.bin'
    ) as f:
        binary_dir = f
        # should work inside the context manager
        subprocess.call(['hadoop', 'fs', '-ls', '{}'.format(binary_dir)])
    with pytest.raises(subprocess.CalledProcessError):
        # this is outside of the context manager, should raise
        # non-zero exist status
        subprocess.check_call(['hadoop', 'fs', '-ls', '{}'.format(binary_dir)])


def test_binary_map_rdd(spark_setup):
    """
    Test lofn binary map is handling binary as expected. We want to return a
    directory, user side reads in using sc.binaryFiles presumably, and the
    output should be partitions of tuples containing (path, contents).
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'word'])

    new_object = DockerPipe(conf)
    with new_object.map_binary(
        rdd=rdd,
        image_name='ubuntu:xenial',
        command='head -c 1024 </dev/urandom > /shared/output.bin'
    ) as f:
        binary_dir = f
        mapped_rdd = sc.binaryFiles(binary_dir)
        results = mapped_rdd.collect()[0]
    assert results[0].split(':')[0] == 'hdfs' and not results[1].isalnum()


def test_binary_map_user_defined_tempdir(spark_setup):
    """
    Test lofn binary map is handling binary as expected. We want to return a
    directory, user side reads in using sc.binaryFiles presumably, and the
    output should be partitions of tuples containing (path, contents).
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['word', 'word'])

    new_object = DockerPipe(conf)
    with new_object.map_binary(
        rdd=rdd,
        image_name='ubuntu:xenial',
        command='head -c 1024 </dev/urandom > /shared/output.bin',
        hdfs_tempdir='/user/' + getpass.getuser() + '/testing_lofn_tempdir'
    ) as f:
        binary_dir = f
        mapped_rdd = sc.binaryFiles(binary_dir)
        results = mapped_rdd.collect()[0]
    assert results[0].split(':')[0] == 'hdfs' and not results[1].isalnum()


def test_binary_reduce(spark_setup):
    """
    Test the binary reduce. Takes output from binary map, a binary rdd,
    and writes them wholesale into two temp files for operations on the
    binary files.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'word'])

    new_object = DockerPipe(conf)
    with new_object.map_binary(
            rdd=rdd,
            image_name='ubuntu:xenial',
            command='head -c 1024 </dev/urandom > /shared/output.bin'
    ) as f:
        binary_dir = f
        mappedRDD = sc.binaryFiles(binary_dir)
        results = new_object.reduce_binary(
            rdd=mappedRDD,
            image_name='ubuntu:xenial',
            command='cat /shared/input_1.bin > /shared/output.bin; '
                    'cat /shared/input_2.bin >> /shared/output.bin'
        )
    assert not results.isalnum() and 3000 > sys.getsizeof(results) >= 2048


def test_docker_failure(spark_setup):
    """
    Test docker failures are bubbled up - causes lofn to fail and reports why.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'word'])
    test_object = DockerPipe(conf)
    # since pyspark catches the exception (docker fails) this is surfaced as
    # a PythonException from spark, which is a Py4JJavaError
    from py4j.protocol import Py4JJavaError
    with pytest.raises(Py4JJavaError):
        mapped_rdd = test_object.map(
            image_name='ubuntu:xenial',
            command="cat /shared/does_not_exist.txt  > /shared/output.txt",
            rdd=rdd)
        mapped_rdd.collect()  # need to perform an action for the map to happen
