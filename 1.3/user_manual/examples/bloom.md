---
title: Bloom Filter Example
---

This example shows how to create a table with bloom filters enabled.  It also
shows how bloom filters increase query performance when looking for values that
do not exist in a table.

Below table named bloom_test is created and bloom filters are enabled.

    $ ./accumulo shell -u username -p password
    Shell - Apache Accumulo Interactive Shell
    - version: 1.3.x-incubating
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> setauths -u username -s exampleVis
    username@instance> createtable bloom_test
    username@instance bloom_test> config -t bloom_test -s table.bloom.enabled=true
    username@instance bloom_test> exit

Below 1 million random values are inserted into accumulo.  The randomly
generated rows range between 0 and 1 billion.  The random number generator is
initialized with the seed 7.

    $ ./bin/accumulo org.apache.accumulo.examples.client.RandomBatchWriter -s 7 instance zookeepers username password bloom_test 1000000 0 1000000000 50 2000000 60000 3 exampleVis

Below the table is flushed, look at the monitor page and wait for the flush to
complete.  

    $ ./bin/accumulo shell -u username -p password
    username@instance> flush -t bloom_test
    Flush of table bloom_test initiated...
    username@instance> exit

The flush will be finished when there are no entries in memory and the 
number of minor compactions goes to zero. Refresh the page to see changes to the table.

After the flush completes, 500 random queries are done against the table.  The
same seed is used to generate the queries, therefore everything is found in the
table.

    $ ./bin/accumulo org.apache.accumulo.examples.client.RandomBatchScanner -s 7 instance zookeepers username password bloom_test 500 0 1000000000 50 20 exampleVis
    Generating 500 random queries...finished
    96.19 lookups/sec   5.20 secs
    num results : 500
    Generating 500 random queries...finished
    102.35 lookups/sec   4.89 secs
    num results : 500

Below another 500 queries are performed, using a different seed which results
in nothing being found.  In this case the lookups are much faster because of
the bloom filters.

    $ ../bin/accumulo org.apache.accumulo.examples.client.RandomBatchScanner -s 8 instance zookeepers username password bloom_test 500 0 1000000000 50 20 exampleVis
    Generating 500 random queries...finished
    2212.39 lookups/sec   0.23 secs
    num results : 0
    Did not find 500 rows
    Generating 500 random queries...finished
    4464.29 lookups/sec   0.11 secs
    num results : 0
    Did not find 500 rows

********************************************************************************

Bloom filters can also speed up lookups for entries that exist.  In accumulo
data is divided into tablets and each tablet has multiple map files. Every
lookup in accumulo goes to a specific tablet where a lookup is done on each
map file in the tablet.  So if a tablet has three map files, lookup performance
can be three times slower than a tablet with one map file.  However if the map
files contain unique sets of data, then bloom filters can help eliminate map
files that do not contain the row being looked up.  To illustrate this two
identical tables were created using the following process.  One table had bloom
filters, the other did not.  Also the major compaction ratio was increased to
prevent the files from being compacted into one file.

 * Insert 1 million entries using  RandomBatchWriter with a seed of 7
 * Flush the table using the shell
 * Insert 1 million entries using  RandomBatchWriter with a seed of 8
 * Flush the table using the shell
 * Insert 1 million entries using  RandomBatchWriter with a seed of 9
 * Flush the table using the shell

After following the above steps, each table will have a tablet with three map
files.  Each map file will contain 1 million entries generated with a different
seed. 

Below 500 lookups are done against the table without bloom filters using random
NG seed 7.  Even though only one map file will likely contain entries for this
seed, all map files will be interrogated.

    $ ./bin/accumulo org.apache.accumulo.examples.client.RandomBatchScanner -s 7 instance zookeepers username password bloom_test1 500 0 1000000000 50 20 exampleVis
    Generating 500 random queries...finished
    35.09 lookups/sec  14.25 secs
    num results : 500
    Generating 500 random queries...finished
    35.33 lookups/sec  14.15 secs
    num results : 500

Below the same lookups are done against the table with bloom filters.  The
lookups were 2.86 times faster because only one map file was used, even though three
map files existed.

    $ ./bin/accumulo org.apache.accumulo.examples.client.RandomBatchScanner -s 7 instance zookeepers username password bloom_test2 500 0 1000000000 50 20 exampleVis
    Generating 500 random queries...finished
    99.03 lookups/sec   5.05 secs
    num results : 500
    Generating 500 random queries...finished
    101.15 lookups/sec   4.94 secs
    num results : 500
