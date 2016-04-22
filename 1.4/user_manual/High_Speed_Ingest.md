---
title: "User Manual: High Speed Ingest"
---

** Next:** [Analytics][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Table Design][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Pre-Splitting New Tables][9]
* [Multiple Ingester Clients][10]
* [Bulk Ingest][11]
* [Logical Time for Bulk Ingest][12]
* [MapReduce Ingest][13]

* * *

## <a id="High-Speed_Ingest"></a> High-Speed Ingest

Accumulo is often used as part of a larger data processing and storage system. To maximize the performance of a parallel system involving Accumulo, the ingestion and query components should be designed to provide enough parallelism and concurrency to avoid creating bottlenecks for users and other systems writing to and reading from Accumulo. There are several ways to achieve high ingest performance. 

## <a id="Pre-Splitting_New_Tables"></a> Pre-Splitting New Tables

New tables consist of a single tablet by default. As mutations are applied, the table grows and splits into multiple tablets which are balanced by the Master across TabletServers. This implies that the aggregate ingest rate will be limited to fewer servers than are available within the cluster until the table has reached the point where there are tablets on every TabletServer. 

Pre-splitting a table ensures that there are as many tablets as desired available before ingest begins to take advantage of all the parallelism possible with the cluster hardware. Tables can be split anytime by using the shell: 
    
    
    user@myinstance mytable> addsplits -sf /local_splitfile -t mytable
    

For the purposes of providing parallelism to ingest it is not necessary to create more tablets than there are physical machines within the cluster as the aggregate ingest rate is a function of the number of physical machines. Note that the aggregate ingest rate is still subject to the number of machines running ingest clients, and the distribution of rowIDs across the table. The aggregation ingest rate will be suboptimal if there are many inserts into a small number of rowIDs. 

## <a id="Multiple_Ingester_Clients"></a> Multiple Ingester Clients

Accumulo is capable of scaling to very high rates of ingest, which is dependent upon not just the number of TabletServers in operation but also the number of ingest clients. This is because a single client, while capable of batching mutations and sending them to all TabletServers, is ultimately limited by the amount of data that can be processed on a single machine. The aggregate ingest rate will scale linearly with the number of clients up to the point at which either the aggregate I/O of TabletServers or total network bandwidth capacity is reached. 

In operational settings where high rates of ingest are paramount, clusters are often configured to dedicate some number of machines solely to running Ingester Clients. The exact ratio of clients to TabletServers necessary for optimum ingestion rates will vary according to the distribution of resources per machine and by data type. 

## <a id="Bulk_Ingest"></a> Bulk Ingest

Accumulo supports the ability to import files produced by an external process such as MapReduce into an existing table. In some cases it may be faster to load data this way rather than via ingesting through clients using BatchWriters. This allows a large number of machines to format data the way Accumulo expects. The new files can then simply be introduced to Accumulo via a shell command. 

To configure MapReduce to format data in preparation for bulk loading, the job should be set to use a range partitioner instead of the default hash partitioner. The range partitioner uses the split points of the Accumulo table that will receive the data. The split points can be obtained from the shell and used by the MapReduce RangePartitioner. Note that this is only useful if the existing table is already split into multiple tablets. 
    
    
    user@myinstance mytable> getsplits
    aa
    ab
    ac
    ...
    zx
    zy
    zz
    

Run the MapReduce job, using the AccumuloFileOutputFormat to create the files to be introduced to Accumulo. Once this is complete, the files can be added to Accumulo via the shell: 
    
    
    user@myinstance mytable> importdirectory /files_dir /failures
    

Note that the paths referenced are directories within the same HDFS instance over which Accumulo is running. Accumulo places any files that failed to be added to the second directory specified. 

A complete example of using Bulk Ingest can be found at   
accumulo/docs/examples/README.bulkIngest 

## <a id="Logical_Time_for_Bulk_Ingest"></a> Logical Time for Bulk Ingest

Logical time is important for bulk imported data, for which the client code may be choosing a timestamp. At bulk import time, the user can choose to enable logical time for the set of files being imported. When its enabled, Accumulo uses a specialized system iterator to lazily set times in a bulk imported file. This mechanism guarantees that times set by unsynchronized multi-node applications (such as those running on MapReduce) will maintain some semblance of causal ordering. This mitigates the problem of the time being wrong on the system that created the file for bulk import. These times are not set when the file is imported, but whenever it is read by scans or compactions. At import, a time is obtained and always used by the specialized system iterator to set that time. 

The timestamp assigned by accumulo will be the same for every key in the file. This could cause problems if the file contains multiple keys that are identical except for the timestamp. In this case, the sort order of the keys will be undefined. This could occur if an insert and an update were in the same bulk import file. 

## <a id="MapReduce_Ingest"></a> MapReduce Ingest

It is possible to efficiently write many mutations to Accumulo in parallel via a MapReduce job. In this scenario the MapReduce is written to process data that lives in HDFS and write mutations to Accumulo using the AccumuloOutputFormat. See the MapReduce section under Analytics for details. 

An example of using MapReduce can be found under   
accumulo/docs/examples/README.mapred 

* * *

** Next:** [Analytics][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Table Design][6]   ** [Contents][8]**

[2]: Analytics.html
[4]: accumulo_user_manual.html
[6]: Table_Design.html
[8]: Contents.html
[9]: High_Speed_Ingest.html#Pre-Splitting_New_Tables
[10]: High_Speed_Ingest.html#Multiple_Ingester_Clients
[11]: High_Speed_Ingest.html#Bulk_Ingest
[12]: High_Speed_Ingest.html#Logical_Time_for_Bulk_Ingest
[13]: High_Speed_Ingest.html#MapReduce_Ingest

