---
title: Regex Example
---

This example uses mapreduce and accumulo to find items using regular expressions.
This is accomplished using a map-only mapreduce job and a scan-time iterator.

To run this example you will need some data in a table.  The following will
put a trivial amount of data into accumulo using the accumulo shell:

    $ ./bin/accumulo shell -u username -p password
    Shell - Apache Accumulo Interactive Shell
    - version: 1.5.0
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> createtable input
    username@instance> insert dogrow dogcf dogcq dogvalue
    username@instance> insert catrow catcf catcq catvalue
    username@instance> quit

The RegexExample class sets an iterator on the scanner.  This does pattern matching
against each key/value in accumulo, and only returns matching items.  It will do this
in parallel and will store the results in files in hdfs.

The following will search for any rows in the input table that starts with "dog":

    $ bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.mapreduce.RegexExample -u user -p passwd -i instance -t input --rowRegex 'dog.*' --output /tmp/output

    $ hadoop fs -ls /tmp/output
    Found 3 items
    -rw-r--r--   1 username supergroup          0 2013-01-10 14:11 /tmp/output/_SUCCESS
    drwxr-xr-x   - username supergroup          0 2013-01-10 14:10 /tmp/output/_logs
    -rw-r--r--   1 username supergroup         51 2013-01-10 14:10 /tmp/output/part-m-00000

We can see the output of our little map-reduce job:

    $ hadoop fs -text /tmp/output/output/part-m-00000
    dogrow dogcf:dogcq [] 1357844987994 false    dogvalue
    $


