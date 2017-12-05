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
Test lofn functionality.
"""

from __future__ import print_function, unicode_literals
import pytest
import logging
import os
import shutil
import tempfile
import sys

import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from lofn.api import DockerPipe


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_setup(request):
    """
    Fixture for spark context on standalone/local
    """
    conf = (SparkConf().setMaster("local[2]").setAppName(
            "pytest-pyspark-local-tests"))
    sc = SparkContext(conf=conf)
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


def test_user_volume(spark_setup):
    """
    Test user volume support. These are volumes that are mounted in docker
    for a whole file reference, shared among all mappers and reducers.

    The assertion for this relies on there being two partitions as defined
    in the spark fixture 'local[2]'.
    """
    sc, conf = spark_setup
    tempdir = tempfile.mkdtemp()
    temp_input_file = os.path.join(tempdir, 'test.txt')
    with open(temp_input_file, 'w') as f:
        f.write("a\nb\nc")
    rdd = sc.parallelize(['a', 'b'])
    mapped_rdd = DockerPipe(
        conf,
        volumes={tempdir: {'bind': '/user_volume/'}}
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
    assert result == ['a', 'a', 'a', 'b', 'b', 'b', 'c', 'c']


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
        DockerPipe(
            conf, volumes=(tempdir, ('bind', '/user_volume/'))
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
    Test map and reduce UDF for writing temp files.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'words'])

    def map_udf(input_split):
        part1 = input_split[0][0]
        part2 = input_split[0][1:]
        return {'1.txt': part1, '2.txt': part2}

    mappedRDD = DockerPipe(conf).map(
        image_name='ubuntu:xenial',
        command="cat /shared/1.txt > /shared/output.txt",
        rdd=rdd,
        map_udf=map_udf)
    result = DockerPipe(conf).reduce(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | sort > /shared/output.txt",
        rdd=mappedRDD
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

    mappedRDD = DockerPipe(conf).map(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt > /shared/output.txt",
        rdd=rdd)
    result = DockerPipe(conf).reduce(
        image_name='ubuntu:xenial',
        command="cat /shared/input.txt | sort > /shared/output.txt",
        rdd=mappedRDD,
        reduce_udf=reduce_udf
    )
    assert result == ['t', 'w']


def test_binary_map_context_manager(spark_setup):
    """
    Test lofn binary map context manager. We want a context manager
    because spark's rdd binary file function takes a dictionary of files then
    reads them in, so we want to keep temp files until these are read
     then destroy the temporary directory.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'words'])
    new_object = DockerPipe(conf)
    with new_object.map_binary(
        rdd=rdd,
        image_name='ubuntu:xenial',
        command='cat /shared/input.txt > /shared/output.bin',
    ) as f:
        binary_dir = f
        os.listdir(binary_dir)  # should work inside the context manager
    with pytest.raises(OSError):
        # this is outside of the context manager, raises FileNotFoundError
        os.listdir(binary_dir)


def test_binary_map_rdd(spark_setup):
    """
    Test lofn binary map is handling binary as expected. We want to return a
    directory, user side reads in using sc.binaryFiles presumably, and the
    output should be partitions of tuples containing (path, contents).
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'words'])

    new_object = DockerPipe(conf)
    with new_object.map_binary(
        rdd=rdd,
        image_name='ubuntu:xenial',
        command='head -c 1024 </dev/urandom > /shared/output.bin',
    ) as f:
        binary_dir = f
        mappedRDD = sc.binaryFiles(binary_dir)
        results = mappedRDD.collect()[0]
    assert results[0].split(':')[0] == 'file' and not results[1].isalnum()


def test_binary_reduce(spark_setup):
    """
    Test the binary reduce. Takes output from binary map, a binary rdd,
    and writes them wholesale into two temp files for operations on the
    binary files.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'words'])

    new_object = DockerPipe(conf)
    with new_object.map_binary(
            rdd=rdd,
            image_name='ubuntu:xenial',
            command='head -c 1024 </dev/urandom > /shared/output.bin',
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
    rdd = sc.parallelize(['test', 'words'])
    test_object = DockerPipe(conf)
    # since pyspark catches the exception (docker fails) this is surfaced as
    # a PythonException from spark, which is a Py4JJavaError
    from py4j.protocol import Py4JJavaError
    with pytest.raises(Py4JJavaError):
        mapped_RDD = test_object.map(
            image_name='ubuntu:xenial',
            command="cat /shared/does_not_exist.txt  > /shared/output.txt",
            rdd=rdd)
        mapped_RDD.collect()  # need to perform an action for the map to happen


def test_custom_tempfile_directory(spark_setup):
    """
    Test user defined tempfile directory. This allows setting the
    name of the parent directory for the temp directories used to store
    tempfiles by lofn.
    """
    sc, conf = spark_setup
    rdd = sc.parallelize(['test', 'words'])

    def unique_dir_name(root, count=0):
        """Recursive function to find a unique name for testing so we don't
        have to destroy anything"""
        if not os.path.exists(root):
            return root
        else:
            count += 1
            new_name = root
            components = root.split('_')
            if len(components) > 1:
                new_name = '_'.join(components[:-1])
            new_name = new_name + '_' + str(count)
            return unique_dir_name(new_name, count)
    # still writing inside of /tmp to make sure we have write permissions
    tempdir_name = unique_dir_name('/tmp/lofntestdir')
    assert not os.path.exists(tempdir_name)
    test_object = DockerPipe(conf, temporary_directory_parent=tempdir_name)
    # using map_binary since the context manager gives us the ability to 
    # keep the temp dirs open for us to check it's existence and use before 
    # removing it all
    with test_object.map_binary(
            rdd=rdd,
            image_name='ubuntu:xenial',
            command='head -c 1024 </dev/urandom > /shared/output.bin',
    ) as binary_dir:
        assert os.path.exists(tempdir_name)  # make sure the path has been
        # created
        assert tempdir_name in binary_dir  # make sure the specified
        # directory is being used
    shutil.rmtree(tempdir_name)
