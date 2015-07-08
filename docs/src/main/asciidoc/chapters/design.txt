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

== Accumulo Design

=== Data Model

Accumulo provides a richer data model than simple key-value stores, but is not a
fully relational database. Data is represented as key-value pairs, where the key and
value are comprised of the following elements:

[width="75%",cols="^,^,^,^,^,^"]
|===========================================================================
    5+|Key                                                      .3+^.^|Value
.2+^.^|Row ID 3+|Column                        .2+^.^|Timestamp
                |Family |Qualifier |Visibility
|===========================================================================

All elements of the Key and the Value are represented as byte arrays except for
Timestamp, which is a Long. Accumulo sorts keys by element and lexicographically
in ascending order. Timestamps are sorted in descending order so that later
versions of the same Key appear first in a sequential scan. Tables consist of a set of
sorted key-value pairs.

=== Architecture

Accumulo is a distributed data storage and retrieval system and as such consists of
several architectural components, some of which run on many individual servers.
Much of the work Accumulo does involves maintaining certain properties of the
data, such as organization, availability, and integrity, across many commodity-class
machines.

=== Components

An instance of Accumulo includes many TabletServers, one Garbage Collector process,
one Master server and many Clients.

==== Tablet Server

The TabletServer manages some subset of all the tablets (partitions of tables). This includes receiving writes from clients, persisting writes to a
write-ahead log, sorting new key-value pairs in memory, periodically
flushing sorted key-value pairs to new files in HDFS, and responding
to reads from clients, forming a merge-sorted view of all keys and
values from all the files it has created and the sorted in-memory
store.

TabletServers also perform recovery of a tablet
that was previously on a server that failed, reapplying any writes
found in the write-ahead log to the tablet.

==== Garbage Collector

Accumulo processes will share files stored in HDFS. Periodically, the Garbage
Collector will identify files that are no longer needed by any process, and
delete them. Multiple garbage collectors can be run to provide hot-standby support.
They will perform leader election among themselves to choose a single active instance.

==== Master

The Accumulo Master is responsible for detecting and responding to TabletServer
failure. It tries to balance the load across TabletServer by assigning tablets carefully
and instructing TabletServers to unload tablets when necessary. The Master ensures all
tablets are assigned to one TabletServer each, and handles table creation, alteration,
and deletion requests from clients. The Master also coordinates startup, graceful
shutdown and recovery of changes in write-ahead logs when Tablet servers fail.

Multiple masters may be run. The masters will choose among themselves a single master,
and the others will become backups if the master should fail.

==== Tracer

The Accumulo Tracer process supports the distributed timing API provided by Accumulo.
One to many of these processes can be run on a cluster which will write the timing
information to a given Accumulo table for future reference. Seeing the section on
Tracing for more information on this support.

==== Monitor

The Accumulo Monitor is a web application that provides a wealth of information about
the state of an instance. The Monitor shows graphs and tables which contain information
about read/write rates, cache hit/miss rates, and Accumulo table information such as scan
rate and active/queued compactions. Additionally, the Monitor should always be the first
point of entry when attempting to debug an Accumulo problem as it will show high-level problems
in addition to aggregated errors from all nodes in the cluster. See the section on <<monitoring>>
for more information.

Multiple Monitors can be run to provide hot-standby support in the face of failure. Due to the
forwarding of logs from remote hosts to the Monitor, only one Monitor process should be active
at one time. Leader election will be performed internally to choose the active Monitor.

==== Client

Accumulo includes a client library that is linked to every application. The client
library contains logic for finding servers managing a particular tablet, and
communicating with TabletServers to write and retrieve key-value pairs.

=== Data Management

Accumulo stores data in tables, which are partitioned into tablets. Tablets are
partitioned on row boundaries so that all of the columns and values for a particular
row are found together within the same tablet. The Master assigns Tablets to one
TabletServer at a time. This enables row-level transactions to take place without
using distributed locking or some other complicated synchronization mechanism. As
clients insert and query data, and as machines are added and removed from the
cluster, the Master migrates tablets to ensure they remain available and that the
ingest and query load is balanced across the cluster.

image::data_distribution.png[width=500]

=== Tablet Service


When a write arrives at a TabletServer it is written to a Write-Ahead Log and
then inserted into a sorted data structure in memory called a MemTable. When the
MemTable reaches a certain size, the TabletServer writes out the sorted
key-value pairs to a file in HDFS called a Relative Key File (RFile), which is a
kind of Indexed Sequential Access Method (ISAM) file. This process is called a
minor compaction. A new MemTable is then created and the fact of the compaction
is recorded in the Write-Ahead Log.

When a request to read data arrives at a TabletServer, the TabletServer does a
binary search across the MemTable as well as the in-memory indexes associated
with each RFile to find the relevant values. If clients are performing a scan,
several key-value pairs are returned to the client in order from the MemTable
and the set of RFiles by performing a merge-sort as they are read.

=== Compactions

In order to manage the number of files per tablet, periodically the TabletServer
performs Major Compactions of files within a tablet, in which some set of RFiles
are combined into one file. The previous files will eventually be removed by the
Garbage Collector. This also provides an opportunity to permanently remove
deleted key-value pairs by omitting key-value pairs suppressed by a delete entry
when the new file is created.

=== Splitting

When a table is created it has one tablet. As the table grows its initial
tablet eventually splits into two tablets. Its likely that one of these
tablets will migrate to another tablet server. As the table continues to grow,
its tablets will continue to split and be migrated. The decision to
automatically split a tablet is based on the size of a tablets files. The
size threshold at which a tablet splits is configurable per table. In addition
to automatic splitting, a user can manually add split points to a table to
create new tablets. Manually splitting a new table can parallelize reads and
writes giving better initial performance without waiting for automatic
splitting.

As data is deleted from a table, tablets may shrink. Over time this can lead
to small or empty tablets. To deal with this, merging of tablets was
introduced in Accumulo 1.4. This is discussed in more detail later.

=== Fault-Tolerance

If a TabletServer fails, the Master detects it and automatically reassigns the tablets
assigned from the failed server to other servers. Any key-value pairs that were in
memory at the time the TabletServer fails are automatically reapplied from the Write-Ahead
Log(WAL) to prevent any loss of data.

Tablet servers write their WALs directly to HDFS so the logs are available to all tablet
servers for recovery. To make the recovery process efficient, the updates within a log are
grouped by tablet.  TabletServers can quickly apply the mutations from the sorted logs
that are destined for the tablets they have now been assigned.

TabletServer failures are noted on the Master's monitor page, accessible via
+http://master-address:9995/monitor+.

image::failure_handling.png[width=500]
