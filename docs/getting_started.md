# Getting Started


## Installation

```
pip install lofn
```

## Dependencies

You can use python 2 or 3, 2.7+ and 3.6+ preferably.

Running a script on this framework requires Spark and Docker.

### Install Docker

See the [Docker Docs](https://docs.docker.com/engine/installation/) on how to install.

### Install Spark

See the [Downloading Spark](https://spark.apache.org/docs/latest/#downloading) instructions to get started.
It will require Java be installed and in your `PATH` or set `JAVA_HOME` and
downloading the [jar files](http://spark.apache.org/downloads.html). Then set `SPARK_HOME` as the path to this directory
 and add its `bin` directory to `PATH` as well.

## Running on Standalone

lofn can be run on Spark standalone on a cluster or a single node. Use `spark-submit` to submit your application
to Spark.

## Running on YARN

Some configurations are required for lofn to work on YARN.

### Configure the Cluster

Beyond having Spark setup on a YARN cluster ready to submit jobs, follow these steps for lofn to work:

- install lofn on each node
- install Docker on each node
- create a Docker group
- add $USER and `yarn` user to Docker group
- restart yarn daemons and your shell for changes to take effect

See the next page 'Using lofn on AWS' for instructions on how to setup an EMR cluster automatically for `lofn`

### Submission

- User volumes must be in HDFS and your `volumes` dictionary should provide the absolute path to the directory on HDFS
- use `spark-submit` to submit the application to Spark

## Examples

Explore some of our [examples](https://github.com/michaeltneylon/lofn/blob/master/example) to get started.

