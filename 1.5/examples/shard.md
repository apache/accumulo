---
title: Shard Example
---

Accumulo has an iterator called the intersecting iterator which supports querying a term index that is partitioned by 
document, or "sharded". This example shows how to use the intersecting iterator through these four programs:

 * Index.java - Indexes a set of text files into an Accumulo table
 * Query.java - Finds documents containing a given set of terms.
 * Reverse.java - Reads the index table and writes a map of documents to terms into another table.
 * ContinuousQuery.java  Uses the table populated by Reverse.java to select N random terms per document.  Then it continuously and randomly queries those terms.

To run these example programs, create two tables like below.

    username@instance> createtable shard
    username@instance shard> createtable doc2term

After creating the tables, index some files.  The following command indexes all of the java files in the Accumulo source code.

    $ cd /local/username/workspace/accumulo/
    $ find core/src server/src -name "*.java" | xargs ./bin/accumulo org.apache.accumulo.examples.simple.shard.Index -i instance -z zookeepers -t shard -u username -p password --partitions 30

The following command queries the index to find all files containing 'foo' and 'bar'.

    $ cd $ACCUMULO_HOME
    $ ./bin/accumulo org.apache.accumulo.examples.simple.shard.Query -i instance -z zookeepers -t shard -u username -p password foo bar
    /local/username/workspace/accumulo/src/core/src/test/java/accumulo/core/security/ColumnVisibilityTest.java
    /local/username/workspace/accumulo/src/core/src/test/java/accumulo/core/client/mock/MockConnectorTest.java
    /local/username/workspace/accumulo/src/core/src/test/java/accumulo/core/security/VisibilityEvaluatorTest.java
    /local/username/workspace/accumulo/src/server/src/main/java/accumulo/test/functional/RowDeleteTest.java
    /local/username/workspace/accumulo/src/server/src/test/java/accumulo/server/logger/TestLogWriter.java
    /local/username/workspace/accumulo/src/server/src/main/java/accumulo/test/functional/DeleteEverythingTest.java
    /local/username/workspace/accumulo/src/core/src/test/java/accumulo/core/data/KeyExtentTest.java
    /local/username/workspace/accumulo/src/server/src/test/java/accumulo/server/constraints/MetadataConstraintsTest.java
    /local/username/workspace/accumulo/src/core/src/test/java/accumulo/core/iterators/WholeRowIteratorTest.java
    /local/username/workspace/accumulo/src/server/src/test/java/accumulo/server/util/DefaultMapTest.java
    /local/username/workspace/accumulo/src/server/src/test/java/accumulo/server/tabletserver/InMemoryMapTest.java

In order to run ContinuousQuery, we need to run Reverse.java to populate doc2term.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.shard.Reverse -i instance -z zookeepers --shardTable shard --doc2Term doc2term -u username -p password

Below ContinuousQuery is run using 5 terms.  So it selects 5 random terms from each document, then it continually 
randomly selects one set of 5 terms and queries.  It prints the number of matching documents and the time in seconds.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.shard.ContinuousQuery -i instance -z zookeepers --shardTable shard --doc2Term doc2term -u username -p password --terms 5
    [public, core, class, binarycomparable, b] 2  0.081
    [wordtodelete, unindexdocument, doctablename, putdelete, insert] 1  0.041
    [import, columnvisibilityinterpreterfactory, illegalstateexception, cv, columnvisibility] 1  0.049
    [getpackage, testversion, util, version, 55] 1  0.048
    [for, static, println, public, the] 55  0.211
    [sleeptime, wrappingiterator, options, long, utilwaitthread] 1  0.057
    [string, public, long, 0, wait] 12  0.132
