# Introduction to lofn

A magical wrapper for serial tools to parallelize them using Spark and Docker.

## How It Works

lofn uses Spark to partition data and schedule tasks. The tasks are carried out by Docker.
lofn writes the partitions to temp files for Docker to mount. The Docker container carries out the commands you set up
and writes the resultant files to the same temp directory to be read back into Spark.

A program that can be broken down into map and reduce steps can be parallelized using lofn. This can work on both serial
and parallelized tools. If a program is built to work in a multi-core environment but not on a cluster, lofn can help
deploy it on a cluster and each task can still run with its native parallel code.

Explore some of our [examples](https://github.com/michaeltneylon/lofn/blob/master/example).

## Supported Features

- Map and Reduce for text files and binary files.
  - `map_binary` takes text as input and produces binary output that can be passed to `reduce_binary`
- User volumes as a global reference that are not partitioned.
- User defined functions can override how to write the temp files from the partition,
  such as unpacking key, value pairs into separate files.
