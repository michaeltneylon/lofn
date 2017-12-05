# Using lofn on AWS

Setup an Elastic Map Reduce cluster on Amazon with lofn

Setup an EMR cluster with Spark installed. Increase the root volume size from the default since lofn uses this for
its temporary files.

### Security Settings

- make sure port 22 is open on each node for SSH
- For docker swarm ensure ports are open: [docker swarm](https://docs.docker.com/engine/swarm/swarm-tutorial/#use-docker-for-mac-or-docker-for-windows)

## Manual Setup

The manual steps give a good outline for what is necessary to get a YARN cluster setup for lofn but may vary depending
 on the OS.

#### Master Node

Run the following commands on the master node:

```
sudo yum update -y
sudo yum install -y git docker
sudo service docker start
sudo groupadd docker
sudo usermod -a -G docker hadoop
sudo usermod -a -G docker yarn
sudo /sbin/stop hadoop-yarn-resourcemanager
sudo /sbin/start hadoop-yarn-resourcemanager
sudo pip install git+https://github.com/michaeltneylon/lofn.git
```

#### Worker Node

Run the following commands on the worker node:

```
sudo yum update -y
sudo yum install -y git docker
sudo service docker start
sudo groupadd docker
sudo usermod -a -G docker hadoop
sudo usermod -a -G docker yarn
sudo /sbin/stop hadoop-yarn-nodemanager
sudo /sbin/start hadoop-yarn-nodemanager
sudo pip install git+https://github.com/michaeltneylon/lofn.git
```

exit each shell and log back in for Docker group changes to take effect.

#### Build Docker Images

If you are using custom images that are not available in a registry, build the images on each node.


## Automatic Setup

If using EMR, the following steps can setup a template for automatically building a cluster that is ready to use lofn.

#### Elastic Map Reduce (EMR)

You can run a bootstrap script on EMR to automatically install Docker Engine and lofn on each node.
After the cluster is up, we then need to configure Docker and join a swarm (to host our Docker images).

##### Bootstrap

This bootstrap script will install Docker and lofn on each node.
Create a shell script in an S3 bucket to run as a bootstrap step using the commands below:


```
#! /bin/bash

sudo yum update -y
sudo yum install -y git docker
sudo service docker start
sudo groupadd docker
sudo usermod -a -G docker hadoop
sudo pip install git+https://github.com/michaeltneylon/lofn.git
```

##### Step

Run the following as a step, to run after the cluster is running. This only executes on the master, so it will generate
some scripts for you to run upon your first login so they can be executed on the workers.

Store the code below in an S3 bucket and choose a step as a Custom Jar, the path to which is
`s3://<region>.elasticmapreduce/libs/script-runner/script-runner.jar` which allows you to execute a script. Add the
s3 path to your script as an argument.


```
#! /bin/bash

sudo usermod -a -G docker yarn
sudo /sbin/stop hadoop-yarn-resourcemanager
sudo /sbin/start hadoop-yarn-resourcemanager

echo '#! /bin/bash' > $HOME/runme.sh
echo 'docker swarm init' >> $HOME/runme.sh
echo 'command=$(docker swarm join-token worker | sed "s/.*command:*//" | tr --delete "\n" | tr --delete "\\\\")' >> $HOME/runme.sh
echo "echo '#! /bin/bash' > $HOME/worker_setup.sh" >> $HOME/runme.sh
echo "echo 'sudo usermod -a -G docker yarn' >> $HOME/worker_setup.sh" >> $HOME/runme.sh
echo "echo 'sudo /sbin/stop hadoop-yarn-nodemanager' >> $HOME/worker_setup.sh" >> $HOME/runme.sh
echo "echo 'sudo /sbin/start hadoop-yarn-nodemanager' >> $HOME/worker_setup.sh" >> $HOME/runme.sh
echo 'echo $command >> $HOME/worker_setup.sh' >> $HOME/runme.sh

workers=$(hdfs dfsadmin -report | grep ^Name | cut -f2 -d: | cut -f2 -d ' ')

for host in $workers;
do
    echo "ssh -A -oStrictHostKeyChecking=no " $host " 'bash -s' < worker_setup.sh" >> $HOME/runme.sh
done
chmod 700 $HOME/runme.sh

```

#### Login and Finish Setup

When this is finished, log in to the master node with SSH agent forwarding. The agent forwarding will enable SSH into
the worker nodes from the master node.

Make sure you add your AWS identity to your local SSH agent:

`ssh-add <awsid.pem>`

login to the master:

`ssh -A hadoop@MASTER_PUBLIC_DNS`

Execute the script 'runme.sh':

`./runme.sh`

### Build and Serve Images

At this point, Docker Swarm is running on the cluster and can host a Docker Registry as a service.
 This enables lofn to use custom images that are not available in a registry without having to manually build the image
 on each node.

create an overlay network and the registry service on the swarm:
```
docker network create --driver overlay lofn-network
docker service create --name registry --publish 5000:5000 --network lofn-network registry:2
```

Build the image, tag it, and push it into the registry. In the example below we are using an image from one of the
[examples](https://github.com/michaeltneylon/lofn/blob/master/example/advanced)

```
git clone https://github.com/michaeltneylon/lofn.git
cd LRL_INFX_LOFN/example/advanced/gsnap_samtools
docker build -t gsnap_samtools .
docker tag gsnap_samtools localhost:5000/gsnap_samtools
docker push localhost:5000/gsnap_samtools
```

Now in the [code](https://github.com/michaeltneylon/lofn/blob/master/example/advanced/YARN/alignment.py),
our images will be named as `localhost:5000/gsnap_samtools` so each node in the swarm knows from
where to pull the image.
