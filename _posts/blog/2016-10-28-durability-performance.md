---
title: "Durability Performance Implications"
date: 2016-10-28 17:00:00 +0000
author: Keith Turner
---

## Overview

Accumulo stores recently written data in a sorted in memory map.  Before data is
added to this map, it's written to an unsorted WAL (write ahead log).  In the
case when a Tablet Server dies, the recently written data is recovered from the
WAL.

When data is written to Accumulo the following happens :

 * Client sends a batch of mutations to a tablet server
 * Tablet server does the following :
   * Writes mutation to Tablet Servers WAL
   * Sync or flush WAL
   * Adds mutations to sorted in memory maps
   * Reports success back to client.

The sync/flush step above moves data written to the WAL from memory to disk.
Write ahead logs are stored in HDFS. HDFS supports two ways of forcing data to
disk for an open file : `hsync` and `hflush`.  

## HDFS Sync/Flush Details

When `hflush` is called on a WAL, it does not ensure data is on disk.  It only
ensure that data is in OS buffers on each datanode and on its way to disk.  As a
result calls to `hflush` are very fast.  If a WAL is replicated to 3 data nodes
then data may be lost if all three machines reboot.  If the datanode process
dies, thats ok because it flushed to OS.  The machines have to reboot for data
loss to occur.

In order to avoid data loss in the event of reboot, `hsync` can be called.  This
will ensure data is written to disk on all datanodes before returning.  When
using `hsync` for the WAL, if Accumulo reports success to a user it means the
data is on disk.  However `hsync` is much slower than `hflush` and the way it's
implemented exacerbates the problem.  For example `hflush` make take 1ms and
`hsync` may take 50ms.  This difference will impact writes to Accumulo and can
be mitigated in some situations with larger buffers in Accumulo.

HDFS keeps checksum data internally by default.  Datanodes store checksum data
in a separate file in the local filesystem.  This means when `hsync` is called
on a WAL, two files must be synced on each datanode.  Syncing two files doubles
the time. To make matters even worse, when the two files are synced the local
filesystem metadata is also synced.  Depending on the local filesystem and its
configuration, syncing the metadata may or may not take time.  In the worst
case, we need to wait for four sync operations at the local filesystem level on
each datanode. One thing I am not sure about, is if these sync operations occur
in parallel on the replicas on different datanodes.  Lets hope they occur in
parallel.  The following pointers show where sync occurs in the datanode code.

 * [BlockReceiver.flushOrSync()][fos] calls [ReplicaOutputStreams.syncDataOut()][ros1] and [ReplicaOutputStreams.syncChecksumOut()][ros2] when `isSync` is true.
 * The methods in ReplicaOutputStreams call [FileChannel.force(true)][fcf] which
   synchronously flushes data and filesystem metadata.

If files were preallocated (this would avoid syncing local filesystem metadata)
and checksums were stored in-line, then 1 sync could be done instead of 4.  

## Configuring WAL flush/sync in Accumulo 1.6

Accumulo 1.6.0 only supported `hsync` and this caused [performance
problems][160_RN_WAL].  In order to offer better performance, the option to
configure `hflush` was [added in 1.6.1][161_RN_WAL].  The
[tserver.wal.sync.method][16_UM_SM] configuration option was added to support
this feature.  This was a tablet server wide option that applied to everything
written to any table.   

## Group Commit

Each Accumulo Tablet Server has a single WAL.  When multiple clients send
mutations to a tablet server at around the same time, the tablet sever may group
all of this into a single WAL operation.  It will do this instead of writing and
syncing or flushing each client's mutations to the WAL separately.  Doing this
increase throughput and lowers average latency for clients.

## Configuring WAL flush/sync in Accumulo 1.7+

Accumulo 1.7.0 introduced [table.durability][17_UM_TD], a new per table property
for configuring durability.  It also stopped using the `tserver.wal.sync.method`
property.  The `table.durability` property has the following four legal values.

 * **none** : Do not write to WAL            
 * **log**  : Write to WAL, but do not sync  
 * **flush** : Write to WAL and call `hflush` 
 * **sync** : Write to WAL and call `hsync`  

If multiple writes arrive at around the same time with different durability
settings, then the group commit code will choose the most conservative
durability.  This can cause one tables settings to slow down writes to another
table.  

In Accumulo 1.6, it was easy to make all writes use `hflush` because there was
only one tserver setting.  Getting everything to use `flush` in 1.7 and later
can be a little tricky because by default the Accumulo metadata table is set to
use `sync`.  Executing the following command in the Accumulo shell will
accomplish this (assuming no tables or namespaces have been specifically set to
`sync`).  The first command sets a system wide table default for `flush`.  The
second two commands override metadata table specific settings of `sync`.

```
config -s table.durability=flush
config -t accumulo.metadata -s table.durability=flush
config -t accumulo.root -s table.durability=flush
```

Even with these settings adjusted, minor compactions could still force `hsync`
to be called in 1.7.0 and 1.7.1.  This was fixed in 1.7.2 and 1.8.0.  See the
[1.7.2 release notes][172_RN_MCHS] and [ACCUMULO-4112] for more details.

In addition to the per table durability setting, a per batch writer durability
setting was also added in 1.7.0.  See
[BatchWriterConfig.setDurability(...)][SD].  This means any client could
potentially cause a `hsync` operation to occur, even if the system is
configured to use `hflush`.

## Improving the situation

The more granular durability settings introduced in 1.7.0 can cause some
unexpected problems.  [ACCUMULO-4146] suggest one possible way to solve these
problems with Per-durability write ahead logs.

[fcf]: https://docs.oracle.com/javase/8/docs/api/java/nio/channels/FileChannel.html#force-boolean-
[ros1]: https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/ReplicaOutputStreams.java#L78
[ros2]: https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/ReplicaOutputStreams.java#L87
[fos]: https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java#L358
[ACCUMULO-4146]: https://issues.apache.org/jira/browse/ACCUMULO-4146
[ACCUMULO-4112]: https://issues.apache.org/jira/browse/ACCUMULO-4112
[160_RN_WAL]: /release_notes/1.6.0#slower-writes-than-previous-accumulo-versions
[161_RN_WAL]: /release_notes/1.6.1#write-ahead-log-sync-implementation
[16_UM_SM]: /1.6/accumulo_user_manual#_tserver_wal_sync_method
[17_UM_TD]: /1.7/accumulo_user_manual#_table_durability
[172_RN_MCHS]: /release_notes/1.7.2#minor-performance-improvements
[SD]: /1.8/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setDurability(org.apache.accumulo.core.client.Durability)
