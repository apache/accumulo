---
title: "Durability Performance Implications"
date: 2016-11-02 17:00:00 +0000
author: Keith Turner
reviewers: Josh Elser, Dave Marion, Christopher Tubbs
---

## Overview

Accumulo stores recently written data in a sorted in memory map.  Before data is
added to this map, it's written to an unsorted write ahead log(WAL).  In the
case when a tablet server dies, the recently written data is recovered from the
WAL.

When data is written to Accumulo the following happens :

 * Client sends a batch of mutations to a tablet server
 * Tablet server does the following :
   * Writes mutation to tablet servers' WAL
   * Sync or flush tablet servers' WAL
   * Adds mutations to sorted in memory map of each tablet.
   * Reports success back to client.

The sync/flush step above moves data written to the WAL from memory to disk.
Write ahead logs are stored in HDFS. HDFS supports two ways of forcing data to
disk for an open file : `hsync` and `hflush`.  

## HDFS Sync/Flush Details

When `hflush` is called on a WAL, it does not guarantee data is on disk.  It
only guarantees that data is in OS buffers on each datanode and on its way to disk.
As a result calls to `hflush` are very fast.  If a WAL is replicated to 3 data
nodes then data may be lost if all three machines reboot or die.  If the datanode
process dies, then data loss will not happen because the data was in OS buffers
waiting to be written to disk.  The machines have to reboot or die for data loss to
occur.

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
in parallel on the replicas on different datanodes.  If anyone can answer this
question, please let us know on the [dev list][ML]. The following pointers show
where sync occurs in the datanode code.

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

Each Accumulo tablet server has a single WAL.  When multiple clients send
mutations to a tablet server at around the same time, the tablet sever may group
all of this into a single WAL operation.  It will do this instead of writing and
syncing or flushing each client's mutations to the WAL separately.  Doing this
increase throughput and lowers average latency for clients.

## Configuring WAL flush/sync in Accumulo 1.7+

Accumulo 1.7.0 introduced [table.durability][17_UM_TD], a new per table property
for configuring durability.  It also stopped using the `tserver.wal.sync.method`
property.  The `table.durability` property has the following four legal values.
This property defaults to the most durable option which is `sync`.

 * **none** : Do not write to WAL            
 * **log**  : Write to WAL, but do not sync  
 * **flush** : Write to WAL and call `hflush` 
 * **sync** : Write to WAL and call `hsync`  

If multiple writes arrive at around the same time with different durability
settings, then the group commit code will choose the most durable.  This can
cause one tables settings to slow down writes to another table.  Basically, one
table that is set to `sync` can impact the entire system.

In Accumulo 1.6, it was easy to make all writes use `hflush` because there was
only one tserver setting.  Getting everything to use `flush` in 1.7 and later
can be a little tricky because by default the Accumulo metadata table is set to
use `sync`.  The following shell commands show this. The first command sets
`table.durability=flush` as a system wide default for all tables.  However, the
metadata table is still set to `sync`, because it has a per table override for
that setting.  This override is set when Accumulo is initialized.  To get this
table to use `flush`, the per table override must be deleted.  After deleting
those properties, the metadata tables will inherit the system wide setting.

```
root@uno> config -s table.durability=flush
root@uno> createtable foo
root@uno foo> config -t foo -f table.durability
-----------+---------------------+----------------------------------------------
SCOPE      | NAME                | VALUE
-----------+---------------------+----------------------------------------------
default    | table.durability .. | sync
system     |    @override ...... | flush
-----------+---------------------+----------------------------------------------
root@uno> config -t accumulo.metadata -f table.durability
-----------+---------------------+----------------------------------------------
SCOPE      | NAME                | VALUE
-----------+---------------------+----------------------------------------------
default    | table.durability .. | sync
system     |    @override ...... | flush
table      |    @override ...... | sync
-----------+---------------------+----------------------------------------------
root@uno> config -t accumulo.metadata -d table.durability
root@uno> config -t accumulo.metadata -f table.durability
-----------+---------------------+----------------------------------------------
SCOPE      | NAME                | VALUE
-----------+---------------------+----------------------------------------------
default    | table.durability .. | sync
system     |    @override ...... | flush
-----------+---------------------+----------------------------------------------
```

In short, executing the following commands will make all writes use `flush`
(assuming no other tables or namespaces have been specifically set to `sync`).

```
config -s table.durability=flush
config -t accumulo.metadata -d table.durability
config -t accumulo.root -d table.durability
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
unexpected problems.  [ACCUMULO-4146] suggests one possible way to solve these
problems with Per-durability write ahead logs.

[fcf]: https://docs.oracle.com/javase/8/docs/api/java/nio/channels/FileChannel.html#force-boolean-
[ros1]: https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/ReplicaOutputStreams.java#L78
[ros2]: https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/ReplicaOutputStreams.java#L87
[fos]: https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java#L358
[ACCUMULO-4146]: https://issues.apache.org/jira/browse/ACCUMULO-4146
[ACCUMULO-4112]: https://issues.apache.org/jira/browse/ACCUMULO-4112
[160_RN_WAL]: {{ site.baseurl }}/release/accumulo-1.6.0#slower-writes-than-previous-accumulo-versions
[161_RN_WAL]: {{ site.baseurl }}/release/accumulo-1.6.1#write-ahead-log-sync-implementation
[16_UM_SM]: {{ site.baseurl }}/1.6/accumulo_user_manual#_tserver_wal_sync_method
[17_UM_TD]: {{ site.baseurl }}/1.7/accumulo_user_manual#_table_durability
[172_RN_MCHS]: {{ site.baseurl }}/release/accumulo-1.7.2#minor-performance-improvements
[SD]: {{ site.baseurl }}/1.8/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setDurability(org.apache.accumulo.core.client.Durability)
[ML]: {{ site.baseurl }}/mailing_list
