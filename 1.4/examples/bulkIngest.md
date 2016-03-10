---
title: Bulk Ingest Example
---

This is an example of how to bulk ingest data into accumulo using map reduce.

The following commands show how to run this example.  This example creates a
table called test_bulk which has two initial split points. Then 1000 rows of
test data are created in HDFS. After that the 1000 rows are ingested into
accumulo.  Then we verify the 1000 rows are in accumulo. The
first two arguments to all of the commands except for GenerateTestData are the
accumulo instance name, and a comma-separated list of zookeepers.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.mapreduce.bulk.SetupTable instance zookeepers username password test_bulk row_00000333 row_00000666
    $ ./bin/accumulo org.apache.accumulo.examples.simple.mapreduce.bulk.GenerateTestData 0 1000 bulk/test_1.txt
    
    $ ./bin/tool.sh lib/examples-simple-*[^cs].jar org.apache.accumulo.examples.simple.mapreduce.bulk.BulkIngestExample instance zookeepers username password test_bulk bulk tmp/bulkWork
    $ ./bin/accumulo org.apache.accumulo.examples.simple.mapreduce.bulk.VerifyIngest instance zookeepers username password test_bulk 0 1000

For a high level discussion of bulk ingest, see the docs dir.
