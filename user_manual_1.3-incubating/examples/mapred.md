---
title: MapReduce Example
---

This example uses mapreduce and accumulo to compute word counts for a set of
documents.  This is accomplished using a map-only mapreduce job and a
accumulo table with aggregators.

To run this example you will need a directory in HDFS containing text files.
The accumulo readme will be used to show how to run this example.

    $ hadoop fs -copyFromLocal $ACCUMULO_HOME/README /user/username/wc/Accumulo.README
    $ hadoop fs -ls /user/username/wc
    Found 1 items
    -rw-r--r--   2 username supergroup       9359 2009-07-15 17:54 /user/username/wc/Accumulo.README

The first part of running this example is to create a table with aggregation
for the column family count.

    $ ./bin/accumulo shell -u username -p password
    Shell - Apache Accumulo Interactive Shell
    - version: 1.3.x-incubating
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> createtable wordCount -a count=org.apache.accumulo.core.iterators.aggregation.StringSummation 
    username@instance wordCount> quit

After creating the table, run the word count map reduce job.

    [user1@instance accumulo]$ bin/tool.sh lib/accumulo-examples-*[^c].jar org.apache.accumulo.examples.mapreduce.WordCount instance zookeepers /user/user1/wc wordCount -u username -p password
    
    11/02/07 18:20:11 INFO input.FileInputFormat: Total input paths to process : 1
    11/02/07 18:20:12 INFO mapred.JobClient: Running job: job_201102071740_0003
    11/02/07 18:20:13 INFO mapred.JobClient:  map 0% reduce 0%
    11/02/07 18:20:20 INFO mapred.JobClient:  map 100% reduce 0%
    11/02/07 18:20:22 INFO mapred.JobClient: Job complete: job_201102071740_0003
    11/02/07 18:20:22 INFO mapred.JobClient: Counters: 6
    11/02/07 18:20:22 INFO mapred.JobClient:   Job Counters 
    11/02/07 18:20:22 INFO mapred.JobClient:     Launched map tasks=1
    11/02/07 18:20:22 INFO mapred.JobClient:     Data-local map tasks=1
    11/02/07 18:20:22 INFO mapred.JobClient:   FileSystemCounters
    11/02/07 18:20:22 INFO mapred.JobClient:     HDFS_BYTES_READ=10487
    11/02/07 18:20:22 INFO mapred.JobClient:   Map-Reduce Framework
    11/02/07 18:20:22 INFO mapred.JobClient:     Map input records=255
    11/02/07 18:20:22 INFO mapred.JobClient:     Spilled Records=0
    11/02/07 18:20:22 INFO mapred.JobClient:     Map output records=1452

After the map reduce job completes, query the accumulo table to see word
counts.

    $ ./bin/accumulo shell -u username -p password
    username@instance> table wordCount
    username@instance wordCount> scan -b the
    the count:20080906 []    75
    their count:20080906 []    2
    them count:20080906 []    1
    then count:20080906 []    1
    there count:20080906 []    1
    these count:20080906 []    3
    this count:20080906 []    6
    through count:20080906 []    1
    time count:20080906 []    3
    time. count:20080906 []    1
    to count:20080906 []    27
    total count:20080906 []    1
    tserver, count:20080906 []    1
    tserver.compaction.major.concurrent.max count:20080906 []    1
    ...
