---
title: Examples
---

Before running any of the examples, the following steps must be performed.

1. Install and run Accumulo via the instructions found in $ACCUMULO_HOME/README.
   Remember the instance name. It will be referred to as "instance" throughout
   the examples. A comma-separated list of zookeeper servers will be referred
   to as "zookeepers".

2. Create an Accumulo user (see the [user manual][1]), or use the root user.
   The "username" Accumulo user name with password "password" is used
   throughout the examples. This user needs the ability to create tables.

In all commands, you will need to replace "instance", "zookeepers",
"username", and "password" with the values you set for your Accumulo instance.

Commands intended to be run in bash are prefixed by '$'. These are always
assumed to be run from the $ACCUMULO_HOME directory.

Commands intended to be run in the Accumulo shell are prefixed by '>'.

Each README in the examples directory highlights the use of particular
features of Apache Accumulo.

   [batch](batch):       Using the batch writer and batch scanner.

   [bloom](bloom):       Creating a bloom filter enabled table to increase query
                       performance.

   [bulkIngest](bulkIngest):  Ingesting bulk data using map/reduce jobs on Hadoop.

   [classpath](classpath):   Using per-table classpaths.

   [client](client):      Using table operations, reading and writing data in Java.

   [combiner](combiner):    Using example StatsCombiner to find min, max, sum, and
                       count.

   [constraints](constraints): Using constraints with tables.

   [dirlist](dirlist):     Storing filesystem information.

   [export](export):      Exporting and importing tables.

   [filedata](filedata):    Storing file data.

   [filter](filter):      Using the AgeOffFilter to remove records more than 30
                       seconds old.

   [helloworld](helloworld):  Inserting records both inside map/reduce jobs and
                       outside. And reading records between two rows.

   [isolation](isolation):   Using the isolated scanner to ensure partial changes
                       are not seen.

   [mapred](mapred):      Using MapReduce to read from and write to Accumulo
                       tables.

   [maxmutation](maxmutation): Limiting mutation size to avoid running out of memory.

   [regex](regex):       Using MapReduce and Accumulo to find data using regular
                       expressions.

   [reservations](reservations): Using conditional mutations to implement a reservation service.

   [rowhash](rowhash):     Using MapReduce to read a table and write to a new
                       column in the same table.

   [shard](shard):       Using the intersecting iterator with a term index
                       partitioned by document.

   [tabletofile](tabletofile): Using MapReduce to read a table and write one of its
                       columns to a file in HDFS.

   [terasort](terasort):    Generating random data and sorting it using Accumulo.

   [visibility](visibility):  Using visibilities (or combinations of authorizations).
                       Also shows user permissions.


[1]: ../accumulo_user_manual#_user_administration
