<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Installing Accumulo

This document covers installing Accumulo on single and multi-node environments.
Either [download][1] or [build][2] a binary distribution of Accumulo from
source code.  Unpack as follows.

    cd <install location>
    tar xzf <some dir>/accumulo-X.Y.Z-bin.tar.gz
    cd accumulo-X.Y.Z

There are four scripts in the `bin` directory of the tarball distribution that are used
to manage Accumulo:

1. `accumulo` - Runs Accumulo command-line tools and starts Accumulo processes
2. `accumulo-service` - Runs Accumulo processes as services
3. `accumulo-cluster` - Manages Accumulo cluster on a single node or several nodes
4. `accumulo-util` - Accumulo utilities for creating configuration, native libraries, etc.

These scripts will be used in the remaining instructions to configure and run Accumulo.
For convenience, consider adding `accumulo-X.Y.Z/bin/` to your shell's path.

## Configuring

Accumulo has some optional native code that improves its performance and
stability. Before configuring Accumulo, attempt to build this native code
with the following command.

    accumulo-util build-native

If the command fails, its OK to continue with setup and resolve the issue later.

Accumulo is configured by the files +accumulo-site.xml+ and +accumulo-env.sh+ in the `conf/`
directory. You can either edit these files for your environment or run the command below which will
overwrite them with files configured for your environment.

    accumulo-util create-config

The script will ask you questions about your set up. Below are some suggestions:

* When the script asks about memory-map type, choose Native if the build native script
  was successful. Otherwise, choose Java.
* The script will prompt for memory usage. Please note that the footprints are
  only for the Accumulo system processes, so ample space should be left for other
  processes like Hadoop, Zookeeper, and the Accumulo client code.  If Accumulo
  worker processes are swapped out and unresponsive, they may be killed.

While `accumulo-util create-config` creates  `accumulo-env.sh` and `accumulo-site.xml` files
targeted for your environment, these files still require a few more edits before starting Accumulo.

### Secret

Accumulo coordination and worker processes can only communicate with each other
if they share the same secret key.  To change the secret key set
`instance.secret` in `accumulo-site.xml`.  Changing this secret key from
the default is highly recommended.

### Dependencies

Accumulo requires running [Zookeeper][3] and [HDFS][4] instances.  Also, the
Accumulo binary distribution does not include jars for Zookeeper and Hadoop.
When configuring Accumulo the following information about these dependencies
must be provided.

 * **Location of Zookeepers** :  Provide this by setting `instance.zookeeper.host`
   in `accumulo-site.xml`.
 * **Where to store data** :  Provide this by setting `instance.volumes` in
   `accumulo-site.xml`.  If your namenode is running at 192.168.1.9:9000
   and you want to store data in `/accumulo` in HDFS, then set
  `instance.volumes` to `hdfs://192.168.1.9:9000/accumulo`.
 * **Location of Zookeeper and Hadoop jars** :  Setting `ZOOKEEPER_HOME` and
   `HADOOP_PREFIX` in `accumulo-env.sh` will help Accumulo find these jars
   when using the default setting for `general.classpaths` in accumulo-site.xml.

If Accumulo has problems later on finding jars, then run `bin/accumulo
classpath` to print out info about where Accumulo is finding jars.  If the
settings mentioned above are correct, then inspect `general.classpaths` in
`accumulo-site.xml`.

## Initialization

Accumulo needs to initialize the locations where it stores data in Zookeeper
and HDFS.  The following command will do this.

    accumulo init

The initialization command will prompt for the following information.

 * **Instance name** : This is the name of the Accumulo instance and its
   Accumulo clients need to know it inorder to connect.
 * **Root password** : Initialization sets up an initial Accumulo root user and
   prompts for its password.  This information will be needed to later connect
   to Accumulo.

## Run Accumulo

There are several methods for running Accumulo:

1. Run individual Accumulo services using `accumulo-service`. Useful if you are
   using a cluster management tool (i.e Ansible, Salt, etc) or init.d scripts to
   start Accumulo.

2. Run an Accumulo cluster on one or more nodes using `accumulo-cluster` (which
   uses `accumulo-service` to run services). Useful for local development and
   testing or if you are not using a cluster management tool in production.

Each method above has instructions below.

### Run Accumulo services

Start Accumulo services (tserver, master, monitor, etc) using command below:

    accumulo-service tserver start

### Run an Accumulo cluster

Before using the `accumulo-cluster` script, additional configuration files need
to be created. Use the command below to create them:

    accumulo-cluster create-config

This creates five files (`masters`, `gc`, `monitor`, `tservers`, & `tracers`)
in the `conf/` directory that contain the node names where Accumulo services
are run on your cluster. By default, all files are configured to `localhost`. If
you are running a single-node Accumulo cluster, theses files do not need to be
changed and the next section should be skipped.

#### Multi-node configuration

If you are running an Accumulo cluster on multiple nodes, the following files
in `conf/` should be configured with a newline separated list of node names:

 * `masters` : Accumulo primary coordinating process. Must specify one node. Can
               specify a few for fault tolerance.
 * `gc`      : Accumulo garbage collector. Must specify one node. Can specify a
               few for fault tolerance.
 * `monitor` : Node where Accumulo monitoring web server is run.
 * `tservers`: Accumulo worker processes. List all of the nodes where tablet servers
               should run in this file.
 * `tracers` : Optional capability. Can specify zero or more nodes. 

The Accumulo, Hadoop, and Zookeeper software should be present at the same
location on every node. Also the files in the `conf` directory must be copied
to every node. There are many ways to replicate the software and configuration,
two possible tools that can help replicate software and/or config are [pdcp][5]
and [prsync][6].

The `accumulo-cluster` script uses ssh to start processes on remote nodes. Before
attempting to start Accumulo, [passwordless ssh][7] must be setup on the cluster.

#### Start cluster

After configuring and initializing Accumulo, use the following command to start
the cluster:

    accumulo-cluster start

## First steps

Once you have started Accumulo, use the following command to run the Accumulo shell:

    accumulo shell -u root

Use your web browser to connect the Accumulo monitor page on port 9995.

    http://<hostname in conf/monitor>:9995/

## Stopping Accumulo

When finished, use the following commands to stop Accumulo:

* Stop Accumulo service: `accumulo-service tserver stop`
* Stop Accumulo cluster: `accumulo-cluster stop`

[1]: http://accumulo.apache.org/
[2]: README.md#building-
[3]: http://zookeeper.apache.org/
[4]: http://http://hadoop.apache.org/
[5]: https://code.google.com/p/pdsh/
[6]: https://code.google.com/p/parallel-ssh/
[7]: https://www.google.com/search?q=hadoop+passwordless+ssh&ie=utf-8&oe=utf-8

