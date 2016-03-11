---
title: Notable Features
nav: nav_features
---

## Categories

* [Table Design and Configuration](#design)
* [Integrity/Availability](#integrity)
* [Performance](#performance)
* [Testing](#testing)
* [Client API](#client)
* [Extensible Behaviors](#behaviors)
* [General Administration](#admin)
* [Internal Data Management](#internal_dm)
* [On-demand Data Management](#ondemand_dm)

***

## Table Design and Configuration <a id="design"></a>

### Iterators

A server-side programming mechanism to encode functions such as filtering and
aggregation within the data management steps (scopes where data is read from or
written to disk) that happen in the tablet server.

### Cell labels

An additional portion of the Key that sorts after the column qualifier and
before the timestamp. It is called column visibility and enables expressive
cell-level access control. Authorizations are passed with each query to control
what data is returned to the user. The column visibilities are boolean AND and
OR combinations of arbitrary strings (such as "(A&B)|C") and authorizations
are sets of strings (such as {C,D}).

### Constraints

Configurable conditions under which writes to a table will be rejected.
Constraints are written in Java and configurable on a per table basis.

### Sharding

Through the use of specialized iterators, Accumulo can be a parallel sharded
document store. For example wikipedia could be stored and searched for
documents containing certain words.

### Large Rows

When reading rows, there is no requirement that an entire row fits into memory.

### Namespaces

In version 1.6.0, the concept of table "namespaces" was created to allow for logical
grouping and configuration of Accumulo tables. By default, tables are created in a
default namespace which is the empty string to preserve the feel for how tables operate
in previous versions. One application of table namespaces is placing the Accumulo root
and metadata table in an "accumulo" namespace to denote that these tables are used
internally by Accumulo.

### Volume support

Accumulo 1.6.0 migrated away from configuration of HDFS by using a single HDFS host and
directory, to a collection of HDFS URIs (host and path) which allows Accumulo to operate
over multiple disjoint HDFS instances.  This allows Accumulo to scale beyond the limits
of a single namenode.  When used in conjunction with HDFS federation, multiple namenodes
can share a pool of datanodes.

## Integrity/Availability <a id="integrity"></a>

### Master fail over

Multiple masters can be configured.  Zookeeper locks are used to determine
which master is active.  The remaining masters simply wait for the current
master to lose its lock.  Current master state is held in the metadata table
and Zookeeper (see [FATE](#fate)).

### Logical time

A mechanism to ensure that server set times never go backwards, even when time
across the cluster is incorrect. This ensures that updates and deletes are not
lost. If a tablet is served on machine with time a year in the future, then the
tablet will continue to issue new timestamps a year in the future even when it
moves to another server. In this case the timestamps preserve ordering, but
lose their meaning. In addition to logical time, Accumulo has master
authoritative time. The master averages the time of all of the tablet servers
and sends this back to the tablet servers. Tablet servers use this information
to adjust the timestamps they issue. So logical time ensures ordering is
always correct and master authoritative time tries to ensure that timestamps
are meaningful.

### Logical Time for bulk import

Logical time as described above works with streaming (batch) ingest, where the
tablet server assigns the timestamp.  Logical time is also important for bulk
imported data, for which the client code may be choosing a timestamp.  Accumulo
uses specialized system iterators to lazily set times in a bulk imported
file.  This mechanism guarantees that times set by unsynchronized multi-node
applications (such as those running on MapReduce) will maintain some semblance
of causal ordering.  This mitigates the problem of the time being wrong on the
system that created the file for bulk import. These times are not set when the
file is imported, but whenever it is read by scans or compactions. At import, a
time is obtained and always used by the specialized system iterator to set that
time.

### FATE {#fate}

Fault Tolerant Executor. A framework for executing operations in a fault
tolerant manner. In the previous release, if the master process died in the
middle of creating a table it could leave the system in an inconsistent state.
With this new framework, if the master dies in the middle of create table it
will continue on restart. Also the client requesting the create table operation
will never know anything happened. The framework serializes work in Zookeeper
before attempting to do the work. Clients start a FATE transaction, seed it
with work, and then wait for it to finish. Most table operations are executed
using this framework. Persistent, per table, read-write locks are created in
Zookeeper to synchronize operations across process faults.

### Scalable master

Stores its metadata in an Accumulo table and Zookeeper.

### Isolation

Scans will not see data inserted into a row after the scan of that row begins.

## Performance <a id="performance"></a>

### Relative encoding

If consecutive keys have identical portions (row, colf, colq, or colvis), there
is a flag to indicate that a portion is the same as that of the previous key.
This is applied when keys are stored on disk and when transferred over the
network.  Starting with 1.5, prefix erasure is supported.  When its cost 
effective, prefixes repeated in subsequent key fields are not repeated.

### Native In-Memory Map

By default data written is stored outside of Java managed memory into a C++ STL
map of maps.  It maps rows to columns to values.  This hierarchical structure
improves performance of inserting a mutation with multiple column values in a
single row. A custom STL allocator is used to avoid the global malloc lock and
memory fragmentation.

### Scan pipeline

A long running Accumulo scan will eventually cause multiple threads to start.
One server thread to read data from disk, one server thread to serialize and
send data, and one client thread to deserialize and read data. When pipelining
kicks in, it substantially increases scan speed while maintaining key order. It
does not activate for short scans.

### Caching

Recently scanned data is cached into memory.  There are separate caches for
indexes and data.  Caching can be turned on and off for individual tables.

### Multi-level RFile Index

RFiles store an index of the last key in each block. For large files the index
can become quite large. When the index is large a lot of memory is consumed and
files take a long time to open. To avoid this problem, RFiles have a
multi-level index tree. Index blocks can point to other index blocks or data
blocks. The entire index never has to be resident, even when the file is
written. When an index block exceeds the configurable size threshold, its
written out between data blocks. The size of index blocks is configurable on a
per table basis.

### Binary search in RFile blocks

RFile uses its index to locate a block of key values.  Once it reaches a block 
it performs a linear scan to find a key on interest.  Accumulo will generate
indexes of cached blocks in an adaptive manner.  Accumulo indexes the blocks
that are read most frequently.  When a block is read a few times, a small index
is generated.  As a block is read more, larger indexes are generated making
future seeks faster. This strategy allows Accumulo to dynamically respond to
read patterns without precomputing block indexes when RFiles are written.

## Testing <a id="testing"></a>

### Mock

The Accumulo client API has a mock implementation that is useful writing unit
test against Accumulo. Mock Accumulo is in memory and in process.

### Mini Accumulo Cluster

Mini Accumulo cluster is a set of utility code that makes it easy to spin up 
a local Accumulo instance running against the local filesystem.  Mini Accumulo
is slower than Mock Accumulo, but its behavior mirrors a real Accumulo 
instance more closely.

### Accumulo Maven Plugin

Using the Mini Accumulo Cluster in unit and integration tests is a great way for
developers to test their applications against Accumulo in an environment that is
much closer to physical deployments than Mock Accumulo provided. Accumulo 1.6.0 also
introduced a [maven-accumulo-plugin](/release_notes/1.6.0.html#maven-plugin) which
can be used to start a Mini Accumulo Cluster instance as a part of the Maven
lifecycle that your application tests can use.

### Functional Test

Small, system-level tests of basic Accumulo features run in a test harness,
external to the build and unit-tests.  These tests start a complete Accumulo
instance, and require Hadoop and Zookeeper to be running.  They attempt to
simulate the basic functions of Accumulo, as well as common failure conditions,
such as lost disks, killed processes, and read-only file systems.

### Scale Test

A test suite that verifies data is not lost at scale. This test runs many
ingest clients that continually create linked lists containing 25 million
nodes. At some point the clients are stopped and a map reduce job is run to
ensure no linked list has a hole. A hole indicates data was lost by Accumulo.
The Agitator can be run in conjunction with this test to randomly kill tablet
servers. This test suite has uncovered many obscure data loss bugs.  This test
also helps find bugs that impact uptime and stability when run for days or
weeks.

### Random Walk Test

A test suite that looks for unexpected system states that may emerge in
plausible real-world applications.  Application components are defined as test
nodes (such as create table, insert data, scan data, delete table, etc.), and
are programmed as Java classes that implement a specified interface.  The nodes
are connected together in a graph specified in an XML document. Many processes
independently and concurrently execute a random walk of the test graphs. Some
of the test graphs have a concept of correctness and can verify data over time.
Other tests have no concept of data correctness and have the simple goal of
crashing Accumulo. Many obscure bugs have been uncovered by this testing
framework and subsequently corrected.

## Client API <a id="client"></a>

### [Batch Scanner][4]

Takes a list of Ranges, batches them to the appropriate tablet servers, and
returns data as it is received (i.e. not in sorted order).

### [Batch Writer][4]

Clients buffer writes in memory before sending them in batches to the
appropriate tablet servers.

### [Bulk Import][6]

Instead of writing individual mutations to Accumulo, entire files of sorted key
value pairs can be imported. These files are moved into the Accumulo directory
and referenced by Accumulo. This feature is useful for ingesting a large amount
of data. This method of ingest usually offers higher throughput at the cost of
higher latency for data availability for scans.  Usually the data is sorted
using map reduce and then bulk imported. This method of ingest also allows for
flexibility in resource allocation.  The nodes running map reduce to sort data
could be different from the Accumulo nodes.

### [Map Reduce][4]

Accumulo can be a source and/or sink for map reduce jobs.

### Thrift Proxy

The Accumulo client code contains a lot of complexity.  For example, the 
client code locates tablets, retries in the case of failures, and supports 
concurrent reading and writing.  All of this is written in Java.  The thrift
proxy wraps the Accumulo client API with thrift, making this API easily
available to other languages like Python, Ruby, C++, etc.

### Conditional Mutations

In version 1.6.0, Accumulo introduced [ConditionalMutations][7]
which allow users to perform efficient, atomic read-modify-write operations on rows. Conditions can
be defined using on equality checks of the values in a column or the absence of a column. For more
information on using this feature, users can reference the Javadoc for [ConditionalMutation](/1.6/apidocs/org/apache/accumulo/core/data/ConditionalMutation.html) and
[ConditionalWriter](/1.6/apidocs/org/apache/accumulo/core/client/ConditionalWriter.html)

### Lexicoders

Common boilerplate code that exists when interacting with Accumulo is the conversion
of Java objects to lexicographically sorted bytes, e.g. ensure that the byte representation
of the number 9 sorts before the byte representation of the number 11. Version 1.6.0 introduced
Lexicoders which have numerous implementations that support for efficient translation from common
Java primitives to byte arrays and vice versa. These classes can greatly reduce the burden in
re-implementing common programming mistakes in encoding.

## Extensible Behaviors <a id="behaviors"></a>

### Pluggable balancer

Users can provide a balancer plugin that decides how to distribute tablets
across a table.  These plugins can be provided on a per table basis.  This is
useful for ensuring a particular table's tablets are placed optimally for
tables with special query needs.  The default balancer randomly spreads each
table's tablets across the cluster.  It takes into account where a tablet was
previously hosted to leverage locality.  When a tablet splits, the default
balancer moves one child to another tablet server.  The assumption here is that
splitting tablets are being actively written to, so this keeps write load evenly
spread.

### Pluggable memory manager

The plugin that decides when and what tablets to minor compact is configurable.
The default plugin compacts the largest tablet when memory is over a certain
threshold.  It varies the threshold over time depending on minor compaction
speed.  It flushes tablets that are not written to for a configurable time
period.

### Pluggable logger assignment strategy

The plugin that decided which loggers should be assigned to which tablet
servers is configurable.

### Pluggable compaction strategy

The plugin that decides which files should be chosen for major compaction is now
configurable. Given certain workloads, it may be known that once data is written,
it is very unlikely that more data will be written to it, and thus paying the penalty
to re-write a large file can be avoided. Implementations of this compaction strategy
can be used to optimize the data that compactions will write.

## General Administration <a id="admin"></a>

### Monitor page

A simple web server provides basic information about the system health and
performance.  It displays table sizes, ingest and query statistics, server
load, and last-update information.  It also allows the user to view recent
diagnostic logs and traces.

### Tracing

It can be difficult to determine why some operations are taking longer than
expected. For example, you may be looking up items with very low latency, but
sometimes the lookups take much longer. Determining the cause of the delay is
difficult because the system is distributed, and the typical lookup is fast.
Accumulo has been instrumented to record the time that various operations take
when tracing is turned on. The fact that tracing is enabled follows all the
requests made on behalf of the user throughout the distributed infrastructure
of Accumulo, and across all threads of execution.

### Online reconfiguration

System and per table configuration is stored in Zookeeper. Many, but not all,
configuration changes take effect while Accumulo is running. Some do not take
effect until server processes are restarted.

### Table renaming

Tables can be renamed easily because Accumulo uses internal table IDs and
stores mappings between names and IDs in Zookeeper.

## Internal Data Management <a id="internal_dm"></a>

### Locality groups

Groups columns within a single file. There is a default locality group so that
not all columns need be specified. The locality groups can be restructured
while the table is online and the changes will take effect on the next
compaction.  A tablet can have files with different locality group
configurations.  In this case scans may be suboptimal, but correct, until
compactions rewrite all files.  After reconfiguring locality groups, a user can
force a table to compact in order to write all data into the new locality
groups.  Alternatively, the change could be allowed to happen over time as
writes to the table cause compactions to happen.

### Smart compaction algorithm

It is inefficient to merge small files with large files.  Accumulo merges files
only if all files are larger than a configurable ratio (default is 3)
multiplied by the largest file size.  If this cannot be done with all the
files, the largest file is removed from consideration, and the remaining files
are considered for compaction.  This is done until there are no files to merge.

### Merging Minor Compaction

When a max number of files per tablet is reached, minor compactions will merge
data from the in-memory map with the smallest file instead of creating new
files.  This throttles ingest.  In previous releases new files were just created
even if major compactions were falling behind and the number of tablets per file
was growing.  Without this feature, ingest performance can roughly continue at a
constant rate, even as scan performance decreases because tablets have too many
files.

### Loading jars using VFS

User written iterators are a useful way to manipulate data in data in Accumulo. 
Before 1.5., users had to copy their iterators to each tablet server.  Starting 
with 1.5 Accumulo can load iterators from HDFS using Apache commons VFS.

### Encryption

Still a work in progress, Accumulo 1.6.0 introduced encryption at rest (RFiles
and WriteAheadLogs) and wire encryption (Thrift over SSL) to provide enhance the
level of security that Accumulo provides. It is still a work in progress because
the intermediate files created by Accumulo when recovering from a TabletServer
failure are not encrypted.

## On-demand Data Management <a id="ondemand_dm"></a>

### Compactions

Ability to force tablets to compact to one file. Even tablets with one file are
compacted.  This is useful for improving query performance, permanently
applying iterators, or using a new locality group configuration.  One example
of using iterators is applying a filtering iterator to remove data from a
table. Additionally, users can initiate a compaction with iterators only applied to
that compaction event.

### Split points

Arbitrary split points can be added to an online table at any point in time.
This is useful for increasing ingest performance on a new table. It can also be
used to accommodate new data patterns in an existing table.

### Tablet Merging

Tablet merging is a new feature. Merging of tablets can be requested in the
shell; Accumulo does not merge tablets automatically. If desired, the METADATA
tablets can be merged.

### Table Cloning

Allows users to quickly create a new table that references an existing table's
data and copies its configuration. A cloned table and its source table can be
mutated independently. Testing was the motivating reason behind this new
feature. For example to test a new filtering iterator, clone the table, add the
filter to the clone, and force a major compaction.

### Import/Export Table

An offline tables metadata and files can easily be copied to another cluster and 
imported.

### Compact Range

Compact each tablet that falls within a row range down to a single file.  

### Delete Range

Added an operation to efficiently delete a range of rows from a table. Tablets
that fall completely within a range are simply dropped. Tablets overlapping the
beginning and end of the range are split, compacted, and then merged.  

[4]: {{ site.baseurl }}/1.5/accumulo_user_manual.html#_writing_accumulo_clients
[6]: {{ site.baseurl }}/1.5/accumulo_user_manual.html#_bulk_ingest
[7]: {{ site.baseurl }}/1.6/accumulo_user_manual.html#_conditionalwriter
