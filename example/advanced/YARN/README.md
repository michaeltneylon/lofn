# GSNAP on lofn

The following instructions detail how to run on AWS EMR. lofn can be run on any cluster size, but the example will show
just one size in use.

## Cluster Setup

Setup an EMR cluster with 1 master and 3 workers. Set the root device EBS volume size to 100 GiB for all instances.

- 1 master: m4.xlarge with 100 GiB EBS Storage
- 3 worker: r4.4xlarge with 200 GiB EBS Storage

### EMR Setup

See the [documentation](http://lofn.readthedocs.io/en/latest/lofn_on_amazon.html) on how to setup an Amazon EMR cluster for lofn.

## Build and Serve Images

With Docker Swarm running on the cluster, we can host a Docker Registry as a service and push our custom images into it.
This lets us avoid building the image on every node manually.

create an overlay network and the service on the swarm:
```
docker network create --driver overlay lofn-network
docker service create --name registry --publish 5000:5000 --network lofn-network registry:2
```

Build the image, tag it, and push it into the registry:

```
git clone https://github.com/michaeltneylon/lofn.git
cd lofn/example/advanced/gsnap_samtools
docker build -t gsnap_samtools .
docker tag gsnap_samtools localhost:5000/gsnap_samtools
docker push localhost:5000/gsnap_samtools
```

Now in the code, our images will be named as `localhost:5000/gsnap_samtools` so each node in the swam knows from
where to pull the image.

## Get Sequence Data

Use the SRA Toolkit to download public data sets from the Sequence Read Archive (SRA)

```
cd $HOME
wget --output-document sratoolkit.tar.gz http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-ubuntu64.tar.gz
tar -vxzf sratoolkit.tar.gz
export PATH=$PATH:$HOME/sratoolkit.2.8.2-1-ubuntu64/bin
prefetch SRR1975008
fastq-dump --split-files SRR1975008
hadoop fs -put SRR1975008_1.fastq
hadoop fs -put SRR1975008_2.fastq
```

## Get Reference Genome

Follow GMAP guidelines to build a reference genome for GSNAP. We have stored ours in S3 and in this example used `distcp`
to move it into HDFS.

`hadoop distcp -Dmapreduce.map.memory.mb=50000  s3:/<gmap reference genome bucket> gmap`

## Run

For this example, we are using one partition per worker node. After the RDD join in `alignment.py`, we repartition to 3
partitions and then we must set our spark configurations to match this.

On this example cluster, to run `lofn` with 3 partitions and 3 executors, one per node, use the following:

`spark-submit --conf spark.task.cpus=16 --conf spark.executor.instances=0 --conf spark.executor.cores=16 --driver-memory 10G --driver-cores=3 --executor-memory 100G --conf spark.yarn.executor.memoryOverhead=3G --conf spark.dynamicAllocation.enabled=true --conf  spark.dynamicAllocation.maxExecutors=4 alignment.py 3`

To gain some speedup, we can increase the number of partitions and executors. This saves more time than the added `samtools merge` steps add, up to a point.

To use 2 executors per node, or 6 executors and partitions total, pass the command-line argument for 6 partitions along with other appropriate configurations for 6 executors:

`spark-submit --conf spark.task.cpus=8 --conf spark.executor.instances=0 --conf spark.executor.cores=8 --driver-memory 10G --driver-cores=3 --executor-memory 50G --conf spark.yarn.executor.memoryOverhead=3G --conf spark.dynamicAllocation.enabled=true --conf  spark.dynamicAllocation.maxExecutors=7 alignment.py 6`
