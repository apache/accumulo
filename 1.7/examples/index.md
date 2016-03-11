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

   [batch](batch.html):       Using the batch writer and batch scanner.

   [bloom](bloom.html):       Creating a bloom filter enabled table to increase query
                       performance.

   [bulkIngest](bulkIngest.html):  Ingesting bulk data using map/reduce jobs on Hadoop.

   [classpath](classpath.html):   Using per-table classpaths.

   [client](client.html):      Using table operations, reading and writing data in Java.

   [combiner](combiner.html):    Using example StatsCombiner to find min, max, sum, and
                       count.

   [constraints](constraints.html): Using constraints with tables.

   [dirlist](dirlist.html):     Storing filesystem information.

   [export](export.html):      Exporting and importing tables.

   [filedata](filedata.html):    Storing file data.

   [filter](filter.html):      Using the AgeOffFilter to remove records more than 30
                       seconds old.

   [helloworld](helloworld.html):  Inserting records both inside map/reduce jobs and
                       outside. And reading records between two rows.

   [isolation](isolation.html):   Using the isolated scanner to ensure partial changes
                       are not seen.

   [mapred](mapred.html):      Using MapReduce to read from and write to Accumulo
                       tables.

   [maxmutation](maxmutation.html): Limiting mutation size to avoid running out of memory.

   [regex](regex.html):       Using MapReduce and Accumulo to find data using regular
                       expressions.

   [reservations](reservations.html): Using conditional mutations to implement a reservation service.

   [rowhash](rowhash.html):     Using MapReduce to read a table and write to a new
                       column in the same table.

   [shard](shard.html):       Using the intersecting iterator with a term index
                       partitioned by document.

   [tabletofile](tabletofile.html): Using MapReduce to read a table and write one of its
                       columns to a file in HDFS.

   [terasort](terasort.html):    Generating random data and sorting it using Accumulo.

   [visibility](visibility.html):  Using visibilities (or combinations of authorizations).
                       Also shows user permissions.


[1]: {{ site.baseurl }}/1.6/user_manual/Accumulo_Shell.html#User_Administration
