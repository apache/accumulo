---
title: "Scaling Accumulo with Multi-Volume Support"
date: 2014-06-25 17:00:00 +0000
author: Dave Marion & Eric Newton
---

Originally posted at [https://blogs.apache.org/accumulo/entry/scaling_accumulo_with_multi_volume](https://blogs.apache.org/accumulo/entry/scaling_accumulo_with_multi_volume)

MapReduce is a commonly used approach to querying or analyzing large amounts of data. Typically MapReduce jobs are created using using some set of files in HDFS to produce a result. When new files come in, they get added to the set, and the job gets run again. A common Accumulo approach to this scenario is to load all of the data into a single instance of Accumulo.

A single instance of Accumulo can scale quite largely([1][1], [2][2]) to accommodate high levels of ingest and query. The manner in which ingest is performed typically depends on latency requirements. When the desired latency is small, inserts are performed directly into Accumulo. When the desired latency is allowed to be large, then a [bulk style of ingest][3] can be used. There are other factors to consider as well, but they are outside the scope of this article.

On large clusters using the bulk style of ingest input files are typically batched into MapReduce jobs to create a set of output RFiles for import into Accumulo. The number of files per job is typically determined by the required latency and the number of MapReduce tasks that the cluster can complete in the given time-frame. The resulting RFiles, when imported into Accumulo, are added to the list of files for their associated tablets. Depending on the configuration this will cause Accumulo to major compact these tablets. If the configuration is tweaked to allow more files per tablet, to reduce the major compactions, then more files need to be opened at query time when performing scans on the tablet. Note that no single node is burdened by the file management; but, the number of file operations in aggregate is very large. If each server has several hundred tablets, and there are a thousand tablet servers, and each tablet compacts some files every few imports, we easily have 50,000 file operations (create, allocate a block, rename and delete) every ingest cycle.

In addition to the NameNode operations caused by bulk ingest, other Accumulo processes (e.g. master, gc) require interaction with the NameNode. Single processes, like the garbage collector, can be starved of responses from the NameNode as the NameNode is limited on the number of concurrent operations. It is not unusual for an operator's request for “hadoop fs -ls /accumulo” to take a minute before returning results during the peak file-management periods. In particular, the file garbage collector can fall behind, not finishing a cycle of unreferenced file removal before the next ingest cycle creates a new batch of files to be deleted.

The Hadoop community addressed the NameNode bottleneck issue with [HDFS federation][4] which allows a datanode to serve up blocks for multiple namenodes. Additionally, ViewFS allows clients to communicate with multiple namenodes through the use of a client-side mount table. This functionality was insufficient for Accumulo in the 1.6.0 release as ViewFS works at a directory level; as an example, /dirA is mapped to one NameNode and /dirB is mapped to another, and Accumulo uses a single HDFS directory for its storage.

Multi-Volume support (MVS), included in 1.6.0, includes the changes that allow Accumulo to work across multiple HDFS clusters (called volumes in Accumulo) while continuing to use a single HDFS directory. A new property, instance.volumes, can be configured with multiple HDFS nameservices and Accumulo will use them all to balance out NameNode operations. The nameservices configured in instance.volumes may optionally use the High Availability NameNode feature as it is transparent to Accumulo. With MVS you have two options to horizontally scale your Accumulo instance. You can use an HDFS cluster with Federation and multiple NameNodes or you can use separate HDFS clusters.

By default Accumulo will perform round-robin file allocation for each tablet, spreading the files across the different volumes. The file balancer is pluggable, allowing for custom implementations. For example, if you don't use Federation and use multiple HDFS clusters, you may want to allocate all files for a particular table to one volume.

Comments in the [JIRA][5] regarding backups could lead to follow-on work. With the inclusion of snapshots in HDFS, you could easily envision an application that quiesces the database or some set of tables, flushes their entries from memory, and snapshots their directories. These snapshots could then be copied to another HDFS instance either for an on-disk backup, or bulk-imported into another instance of Accumulo for testing or some other use.

The example configuration below shows how to set up Accumulo with HA NameNodes and Federation, as it is likely the most complex. We had to reference several web sites, one of the HDFS mailing lists, and the source code to find all of the configuration parameters that were needed. The configuration below includes two sets of HA namenodes, each set servicing an HDFS nameservice in a single HDFS cluster. In the example below, nameserviceA is serviced by name nodes 1 and 2, and nameserviceB is serviced by name nodes 3 and 4.

### core-site.xml

```xml
<property>
  <name>fs.defaultFS</name>
  <value>viewfs:///</value>
</property>
<property>
  <name>fs.viewfs.mounttable.default.link./nameserviceA</name>
  <value>hdfs://nameserviceA</value>
</property>
<property>
  <name>fs.viewfs.mounttable.default.link./nameserviceB</name>
  <value>hdfs://nameserviceB</value>
</property>
<property>
  <name>fs.viewfs.mounttable.default.link./nameserviceA/accumulo/instance_id</name>
  <value>hdfs://nameserviceA/accumulo/instance_id</value>
  <description>Workaround for ACCUMULO-2719</description>
</property>
<property>
  <name>dfs.ha.fencing.methods</name>
  <value>sshfence(hdfs:22)      
         shell(/bin/true)</value>
</property>
<property>   
  <name>dfs.ha.fencing.ssh.private-key-files</name>
  <value><PRIVATE_KEY_LOCATION></value>
</property>
<property>
  <name>dfs.ha.fencing.ssh.connect-timeout</name>
  <value>30000</value>
</property>
<property>
  <name>ha.zookeeper.quorum</name>   
  <value>zkHost1:2181,zkHost2:2181,zkHost3:2181</value>
</property>
```

### hdfs-site.xml

```xml
<property>
  <name>dfs.nameservices</name>
  <value>nameserviceA,nameserviceB</value>
</property>
<property>
  <name>dfs.ha.namenodes.nameserviceA</name>
  <value>nn1,nn2</value>
</property>
<property> 
  <name>dfs.ha.namenodes.nameserviceB</name>
  <value>nn3,nn4</value>
</property>
<property>
  <name>dfs.namenode.rpc-address.nameserviceA.nn1</name>
  <value>host1:8020</value>
</property>
<property>
  <name>dfs.namenode.rpc-address.nameserviceA.nn2</name>
  <value>host2:8020</value>
</property>
<property>
  <name>dfs.namenode.http-address.nameserviceA.nn1</name>
  <value>host1:50070</value>
</property>
<property>
  <name>dfs.namenode.http-address.nameserviceA.nn2</name>
  <value>host2:50070</value>
</property>
<property>
  <name>dfs.namenode.rpc-address.nameserviceB.nn3</name>
  <value>host3:8020</value>
</property>
<property>
  <name>dfs.namenode.rpc-address.nameserviceB.nn4</name>
  <value>host4:8020</value>
</property>
<property> 
  <name>dfs.namenode.http-address.nameserviceB.nn3</name>
  <value>host3:50070</value>
</property>
<property>
  <name>dfs.namenode.http-address.nameserviceB.nn4</name>
  <value>host4:50070</value>
</property>
<property> 
  <name>dfs.namenode.shared.edits.dir.nameserviceA.nn1</name>
  <value>qjournal://jHost1:8485;jHost2:8485;jHost3:8485/nameserviceA</value>
</property>
<property>
  <name>dfs.namenode.shared.edits.dir.nameserviceA.nn2</name>   
  <value>qjournal://jHost1:8485;jHost2:8485;jHost3:8485/nameserviceA</value>
</property>
<property>
  <name>dfs.namenode.shared.edits.dir.nameserviceB.nn3</name>
  <value>qjournal://jHost1:8485;jHost2:8485;jHost3:8485/nameserviceB</value>
</property>
<property>
  <name>dfs.namenode.shared.edits.dir.nameserviceB.nn4</name>
  <value>qjournal://jHost1:8485;jHost2:8485;jHost3:8485/nameserviceB</value>
</property>
<property>
  <name>dfs.client.failover.proxy.provider.nameserviceA</name>
  <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
</property>
<property> 
  <name>dfs.client.failover.proxy.provider.nameserviceB</name>
  <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
</property>
<property>
  <name>dfs.ha.automatic-failover.enabled.nameserviceA</name>
  <value>true</value>
</property>
<property>
  <name>dfs.ha.automatic-failover.enabled.nameserviceB</name>
  <value>true</value>
</property>
```

### accumulo-site.xml

```xml
<property>
  <name>instance.volumes</name>
  <value>hdfs://nameserviceA/accumulo,hdfs://nameserviceB/accumulo</value>
</property>
```


[1]: http://ieeexplore.ieee.org/zpl/login.jsp?arnumber=6597155
[2]: http://www.pdl.cmu.edu/SDI/2013/slides/big_graph_nsa_rd_2013_56002v1.pdf
[3]: http://accumulo.apache.org/1.6/examples/bulkIngest.html
[4]: https://issues.apache.org/jira/browse/HDFS-1052
[5]: https://issues.apache.org/jira/browse/ACCUMULO-118
