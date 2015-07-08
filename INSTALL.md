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

Installing Accumulo
===================

This document covers installing Accumulo on single and multi-node environments.
Either [download][1] or [build][2] a binary distribution of Accumulo from
source code.  Unpack as follows.

    cd <install location>
    tar xzf <some dir>/accumulo-X.Y.Z-bin.tar.gz
    cd accumulo-X.Y.Z

Accumulo has some optional native code that improves its performance and
stability.  Before configuring Accumulo attempt to build this native code
with the following command.

    ./bin/build_native_library.sh

If the command fails, its ok to continue with setup and resolve the issue
later.


Configuring
-----------

The Accumulo conf directory needs to be populated with initial config files.
The following script is provided to assist with this.  Run the script and
answer the questions.  When the script ask about memory-map type, choose Native
if the build native script was successful.  Otherwise choose Java.

    ./bin/bootstrap_config.sh

The script will prompt for memory usage.   Please note that the footprints are
only for the Accumulo system processes, so ample space should be left for other
processes like hadoop, zookeeper, and the accumulo client code.  If Accumulo
worker processes are swapped out and unresponsive, they may be killed.

After this script runs, the conf directory should be populated and now a few
edits are needed.

### Secret

Accumulo coordination and worker processes can only communicate with each other
if they share the same secret key.  To change the secret key set
`instance.secret` in `conf/accumulo-site.xml`.  Changing this secret key from
the default is highly recommended.

### Dependencies

Accumulo requires running [Zookeeper][3] and [HDFS][4] instances.  Also, the
Accumulo binary distribution does not include jars for Zookeeper and Hadoop.
When configuring Accumulo the following information about these dependencies
must be provided.

 * **Location of Zookeepers** :  Provide this by setting `instance.zookeeper.host`
   in `conf/accumulo-site.xml`.
 * **Where to store data** :  Provide this by setting `instance.volumes` in
   `conf/accumulo-site.xml`.  If your namenode is running at 192.168.1.9:9000
   and you want to store data in `/accumulo` in HDFS, then set
  `instance.volumes` to `hdfs://192.168.1.9:9000/accumulo`.
 * **Location of Zoookeeper and Hadoop jars** :  Setting `ZOOKEEPER_HOME` and
   `HADOOP_PREFIX` in `conf/accumulo-env.sh` will help Accumulo find these
   jars.

If Accumulo has problems later on finding jars, then run `bin/accumulo
classpath` to print out info about where Accumulo is finding jars.  If the
settings mentioned above are correct, then inspect `general.classpaths` in
`conf/accumulo-site.xml`.

Initialization
--------------

Accumulo needs to initialize the locations where it stores data in Zookeeper
and HDFS.  The following command will do this.

    ./bin/accumulo init

The initialization command will prompt for the following information.

 * **Instance name** : This is the name of the Accumulo instance and its
   Accumulo clients need to know it inorder to connect.
 * **Root password** : Initialization sets up an initial Accumulo root user and
   prompts for its password.  This information will be needed to later connect
   to Accumulo.

Multiple Nodes
--------------

Skip this section if running Accumulo on a single node.  Accumulo has
coordinating, monitoring, and worker processes that run on specified nodes in
the cluster.  The following files should be populated with a newline separated
list of node names.  Must change from localhost.

 * `conf/masters` : Accumulo primary coordinating process.  Must specify one
                    node.  Can specify a few for fault tolerance.
 * `conf/gc`      : Accumulo garbage collector.  Must specify one node.  Can
                    specify a few for fault tolerance.
 * `conf/monitor` : Node where Accumulo monitoring web server is run.
 * `conf/slaves`  : Accumulo worker processes.   List all of the nodes where
                    tablet servers should run in this file.
 * `conf/tracers` : Optional capability. Can specify zero or more nodes. 

The Accumulo, Hadoop, and Zookeeper software should be present at the same
location on every node.  Also the files in the `conf` directory must be copied
to every node.  There are many ways to replicate the software and
configuration, two possible tools that can help replicate software and/or
config are [pdcp][5] and [prsync][6].

Starting Accumulo
-----------------

The Accumulo scripts use ssh to start processes on remote nodes.  Before
attempting to start Accumulo, [passwordless ssh][7] must be setup on the
cluster.

After configuring and initializing Accumulo, use the following command to start
it.

    ./bin/start-all.sh

First steps
-----------

Once the `start-all.sh` script completes, use the following command to run the
Accumulo shell.

    ./bin/accumulo shell -u root

Use your web browser to connect the Accumulo monitor page on port 9995.

    http://<hostname in conf/monitor>:9995/

When finished, use the following command to stop Accumulo.

    ./bin/stop-all.sh

[1]: http://accumulo.apache.org/
[2]: README.md#building-
[3]: http://zookeeper.apache.org/
[4]: http://http://hadoop.apache.org/
[5]: https://code.google.com/p/pdsh/
[6]: https://code.google.com/p/parallel-ssh/
[7]: https://www.google.com/search?q=hadoop+passwordless+ssh&ie=utf-8&oe=utf-8

