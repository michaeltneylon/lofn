# Lightweight Orchestration For Now (LOFN)

Parallelize your serial tool using Spark and Docker.

Build a docker image for your tool and wrap it with this framework along with
a map and reduce command.

See the [docs](https://lofn.readthedocs.org) for more detailed documentation and advanced features.

See [example](example) to get an idea of how this can be used.

## Installation

Install the latest released version with:

`pip install lofn`

## Usage

Dependencies include spark and docker engine.

Import the api module and DockerPipe class.

`from lofn.api import DockerPipe`

General Steps:

1. Use PySpark RDD API to read your input file and decide how to partition it.
2. Create a DockerPipe object
3. Use the map method of this object to input the RDD, a Docker image, and
the command to run in that image. This returns a new RDD.
4. Use the reduce method to input the mapped RDD to bring back a final
result, using a Docker image and command.

See the [documentation](https://lofn.readthedocs.org).

A very basic template:

```
#! /usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf())

from lofn.api import DockerPipe

rdd = sc.textFile([input_file])
mappedRDD = DockerPipe(SparkConf()).map(
    image_name=[docker_image],
    command="[action] /shared/input.txt > /shared/output.txt",
    rdd=rdd
)
result = DockerPipe(SparkConf()).reduce(
    image_name=[docker_image],
    command="[action] /shared/input.txt > /shared/output.txt",
    rdd=mappedRDD
)
sc.stop()
```

Use `spark-submit` to run this script in Spark.
