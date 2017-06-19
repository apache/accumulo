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

This document provides basic instructions for installing Accumulo. For detailed instructions,
see the 'In-depth Installation' guide in the Accumulo documentation on the project website.

Either [download] or [build] a binary distribution of Accumulo from source code and
unpack as follows.

    tar xzf /path/to/accumulo-X.Y.Z-bin.tar.gz
    cd accumulo-X.Y.Z

There are four scripts in the `bin` directory of the tarball distribution that are used
to manage Accumulo:

1. `accumulo` - Runs Accumulo command-line tools and starts Accumulo processes
2. `accumulo-service` - Runs Accumulo processes as services
3. `accumulo-cluster` - Manages Accumulo cluster on a single node or several nodes
4. `accumulo-util` - Accumulo utilities for building native libraries, running jars, etc.

These scripts will be used in the remaining instructions to configure and run Accumulo.
For convenience, consider adding `accumulo-X.Y.Z/bin/` to your shell's path.

## Configuring Accumulo

Accumulo requires running [Zookeeper] and [HDFS] instances which should be set up
before configuring Accumulo.

The primary configuration files for Accumulo are `accumulo-env.sh` and `accumulo-site.xml`
which are located in the `conf/` directory.

Follow the steps below to configure `accumulo-site.xml`:

1. Run `accumulo-util build-native` to build native code.  If this command fails, disable
   native maps by setting `tserver.memory.maps.native.enabled` to `false`.

2. Set `instance.volumes` to HDFS location where Accumulo will store data. If your namenode
   is running at 192.168.1.9:8020 and you want to store data in `/accumulo` in HDFS, then set
   `instance.volumes` to `hdfs://192.168.1.9:8020/accumulo`.

3. Set `instance.zookeeper.host` to the location of your Zookeepers

4. (Optional) Change `instance.secret` (which is used by Accumulo processes to communicate)
   from the default. This value should match on all servers.

Follow the steps below to configure `accumulo-env.sh`:

1. Set `HADOOP_PREFIX` and `ZOOKEEPER_HOME` to the location of your Hadoop and Zookeeper
   installations. Accumulo will use these locations to find Hadoop and Zookeeper jars and add
   them to your `CLASSPATH` variable. If you you are running a vendor-specific release of
   Hadoop or Zookeeper, you may need to modify how the `CLASSPATH` variable is built in
   `accumulo-env.sh`. If Accumulo has problems loading classes when you start it, run 
   `accumulo classpath -d` to debug and print Accumulo's classpath.

2. Accumulo tablet servers are configured by default to use 1GB of memory (768MB is allocated to
   JVM and 256MB is allocated for native maps). Native maps are allocated memory equal to 33% of
   the tserver JVM heap. The table below can be used if you would like to change tsever memory
   usage in the `JAVA_OPTS` section of `accumulo-env.sh`:

    | Native? | 512MB             | 1GB               | 2GB                 | 3GB           |
    |---------|-------------------|-------------------|---------------------|---------------|
    | Yes     | -Xmx384m -Xms384m | -Xmx768m -Xms768m | -Xmx1536m -Xms1536m | -Xmx2g -Xms2g |
    | No      | -Xmx512m -Xms512m | -Xmx1g -Xms1g     | -Xmx2g -Xms2g       | -Xmx3g -Xms3g |

3. (Optional) Review the memory settings for the Accumulo master, garbage collector, and monitor
   in the `JAVA_OPTS` section of `accumulo-env.sh`.

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

1. Run Accumulo processes using `accumulo` command which runs processes in foreground and
   will not redirect stderr/stdout. Useful for creating init.d scripts that run Accumulo.

2. Run Accumulo processes as services using `accumulo-service` which uses `accumulo`
   command but backgrounds processes, redirects stderr/stdout and manages pid files.
   Useful if you are using a cluster management tool (i.e Ansible, Salt, etc).

2. Run an Accumulo cluster on one or more nodes using `accumulo-cluster` (which
   uses `accumulo-service` to run services). Useful for local development and
   testing or if you are not using a cluster management tool in production.

Each method above has instructions below.

### Run Accumulo processes

Start Accumulo processes (tserver, master, monitor, etc) using command below:

    accumulo tserver

The process will run in the foreground. Use ctrl-c to quit.

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
two possible tools that can help replicate software and/or config are [pdcp]
and [prsync].

The `accumulo-cluster` script uses ssh to start processes on remote nodes. Before
attempting to start Accumulo, [passwordless ssh][pwl] must be setup on the cluster.

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

[download]: https://accumulo.apache.org/downloads/
[build]: README.md#building
[Zookeeper]: https://zookeeper.apache.org/
[HDFS]: https://hadoop.apache.org/
[pdcp]: https://code.google.com/p/pdsh/
[prsync]: https://code.google.com/p/parallel-ssh/
[pwl]: https://www.google.com/search?q=hadoop+passwordless+ssh&ie=utf-8&oe=utf-8
