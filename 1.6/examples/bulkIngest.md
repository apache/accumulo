---
title: Bulk Ingest Example
---

This is an example of how to bulk ingest data into Accumulo using MapReduce.

The following commands show how to run this example. This example creates a
table called test_bulk which has two initial split points. Then 1000 rows of
test data are created in HDFS. After that the 1000 rows are ingested into
Accumulo. Then we verify the 1000 rows are in Accumulo.

    $ PKG=org.apache.accumulo.examples.simple.mapreduce.bulk
    $ ARGS="-i instance -z zookeepers -u username -p password"
    $ ./bin/accumulo $PKG.SetupTable $ARGS -t test_bulk row_00000333 row_00000666
    $ ./bin/accumulo $PKG.GenerateTestData --start-row 0 --count 1000 --output bulk/test_1.txt
    $ ./bin/tool.sh lib/accumulo-examples-simple.jar $PKG.BulkIngestExample $ARGS -t test_bulk --inputDir bulk --workDir tmp/bulkWork
    $ ./bin/accumulo $PKG.VerifyIngest $ARGS -t test_bulk --start-row 0 --count 1000

For a high level discussion of bulk ingest, see the docs dir.
