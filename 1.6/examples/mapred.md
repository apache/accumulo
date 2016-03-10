---
title: MapReduce Example
---

This example uses mapreduce and accumulo to compute word counts for a set of
documents. This is accomplished using a map-only mapreduce job and a
accumulo table with combiners.

To run this example you will need a directory in HDFS containing text files.
The accumulo readme will be used to show how to run this example.

    $ hadoop fs -copyFromLocal $ACCUMULO_HOME/README /user/username/wc/Accumulo.README
    $ hadoop fs -ls /user/username/wc
    Found 1 items
    -rw-r--r--   2 username supergroup       9359 2009-07-15 17:54 /user/username/wc/Accumulo.README

The first part of running this example is to create a table with a combiner
for the column family count.

    $ ./bin/accumulo shell -u username -p password
    Shell - Apache Accumulo Interactive Shell
    - version: 1.6.0
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    -
    - type 'help' for a list of available commands
    -
    username@instance> createtable wordCount
    username@instance wordCount> setiter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -t wordCount -majc -minc -scan
    SummingCombiner interprets Values as Longs and adds them together. A variety of encodings (variable length, fixed length, or string) are available
    ----------> set SummingCombiner parameter all, set to true to apply Combiner to every column, otherwise leave blank. if true, columns option will be ignored.: false
    ----------> set SummingCombiner parameter columns, <col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non-alphanum chars using %<hex>.: count
    ----------> set SummingCombiner parameter lossy, if true, failed decodes are ignored. Otherwise combiner will error on failed decodes (default false): <TRUE|FALSE>: false
    ----------> set SummingCombiner parameter type, <VARLEN|FIXEDLEN|STRING|fullClassName>: STRING
    username@instance wordCount> quit

After creating the table, run the word count map reduce job.

    $ bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.mapreduce.WordCount -i instance -z zookeepers  --input /user/username/wc -t wordCount -u username -p password

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

Another example to look at is
org.apache.accumulo.examples.simple.mapreduce.UniqueColumns. This example
computes the unique set of columns in a table and shows how a map reduce job
can directly read a tables files from HDFS.

One more example available is
org.apache.accumulo.examples.simple.mapreduce.TokenFileWordCount.
The TokenFileWordCount example works exactly the same as the WordCount example
explained above except that it uses a token file rather than giving the
password directly to the map-reduce job (this avoids having the password
displayed in the job's configuration which is world-readable).

To create a token file, use the create-token utility

    $ ./bin/accumulo create-token

It defaults to creating a PasswordToken, but you can specify the token class
with -tc (requires the fully qualified class name). Based on the token class,
it will prompt you for each property required to create the token.

The last value it prompts for is a local filename to save to. If this file
exists, it will append the new token to the end. Multiple tokens can exist in
a file, but only the first one for each user will be recognized.

Rather than waiting for the prompts, you can specify some options when calling
create-token, for example

    $ ./bin/accumulo create-token -u root -p secret -f root.pw

would create a token file containing a PasswordToken for
user 'root' with password 'secret' and saved to 'root.pw'

This local file needs to be uploaded to hdfs to be used with the
map-reduce job. For example, if the file were 'root.pw' in the local directory:

    $ hadoop fs -put root.pw root.pw

This would put 'root.pw' in the user's home directory in hdfs.

Because the basic WordCount example uses Opts to parse its arguments
(which extends ClientOnRequiredTable), you can use a token file with
the basic WordCount example by calling the same command as explained above
except replacing the password with the token file (rather than -p, use -tf).

    $ ./bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.mapreduce.WordCount -i instance -z zookeepers  --input /user/username/wc -t wordCount -u username -tf tokenfile

In the above examples, username was 'root' and tokenfile was 'root.pw'

However, if you don't want to use the Opts class to parse arguments,
the TokenFileWordCount is an example of using the token file manually.

    $ bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.mapreduce.TokenFileWordCount instance zookeepers username tokenfile /user/username/wc wordCount

The results should be the same as the WordCount example except that the
authentication token was not stored in the configuration. It was instead
stored in a file that the map-reduce job pulled into the distributed cache.
(If you ran either of these on the same table right after the
WordCount example, then the resulting counts should just double.)
