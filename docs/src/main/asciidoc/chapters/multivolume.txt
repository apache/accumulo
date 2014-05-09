// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

== Multi-Volume Installations

This is an advanced configuration setting for very large clusters
under a lot of write pressure.

The HDFS NameNode holds all of the metadata about the files in
HDFS. For fast performance, all of this information needs to be stored
in memory.  A single NameNode with 64G of memory can store the
metadata for tens of millions of files.However, when scaling beyond a
thousand nodes, an active Accumulo system can generate lots of updates
to the file system, especially when data is being ingested.  The large
number of write transactions to the NameNode, and the speed of a
single edit log, can become the limiting factor for large scale
Accumulo installations.

You can see the effect of slow write transactions when the Accumulo
Garbage Collector takes a long time (more than 5 minutes) to delete
the files Accumulo no longer needs.  If your Garbage Collector
routinely runs in less than a minute, the NameNode is performing well.

However, if you do begin to experience slow-down and poor GC
performance, Accumulo can be configured to use multiple NameNode
servers.  The configuration ``instance.volumes'' should be set to a
comma-separated list, using full URI references to different NameNode
servers:

[source,xml]
<property>
    <name>instance.volumes</name>
    <value>hdfs://ns1:9001,hdfs://ns2:9001</value>
</property>

The introduction of multiple volume support in 1.6 changed the way Accumulo
stores pointers to files.  It now stores fully qualified URI references to
files.  Before 1.6, Accumulo stored paths that were relative to a table
directory.   After an upgrade these relative paths will still exist and are
resolved using instance.dfs.dir, instance.dfs.uri, and Hadoop configuration in
the same way they were before 1.6.

If the URI for a namenode changes (e.g. namenode was running on host1 and its
moved to host2), then Accumulo will no longer function.  Even if Hadoop and
Accumulo configurations are changed, the fully qualified URIs stored in
Accumulo will still contain the old URI.  To handle this Accumulo has the
following configuration property for replacing URI stored in its metadata.  The
example configuration below will replace ns1 with nsA and ns2 with nsB in
Accumulo metadata.  For this property to take affect, Accumulo will need to be
restarted.

[source,xml]
<property>
    <name>instance.volumes.replacements</name>
    <value>hdfs://ns1:9001 hdfs://nsA:9001, hdfs://ns2:9001 hdfs://nsB:9001</value>
</property>

Using viewfs or HA namenode, introduced in Hadoop 2, offers another option for
managing the fully qualified URIs stored in Accumulo.  Viewfs and HA namenode
both introduce a level of indirection in the Hadoop configuration.   For
example assume viewfs:///nn1 maps to hdfs://nn1 in the Hadoop configuration.
If viewfs://nn1 is used by Accumulo, then its easy to map viewfs://nn1 to
hdfs://nnA by changing the Hadoop configuration w/o doing anything to Accumulo.
A production system should probably use a HA namenode.  Viewfs may be useful on
a test system with a single non HA namenode.

You may also want to configure your cluster to use Federation,
available in Hadoop 2.0, which allows DataNodes to respond to multiple
NameNode servers, so you do not have to partition your DataNodes by
NameNode.
