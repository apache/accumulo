---
title: Table-to-File Example
---

This example uses mapreduce to extract specified columns from an existing table.

To run this example you will need some data in a table. The following will
put a trivial amount of data into accumulo using the accumulo shell:

    $ ./bin/accumulo shell -u username -p password
    Shell - Apache Accumulo Interactive Shell
    - version: 1.6.0
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    -
    - type 'help' for a list of available commands
    -
    username@instance> createtable input
    username@instance> insert dog cf cq dogvalue
    username@instance> insert cat cf cq catvalue
    username@instance> insert junk family qualifier junkvalue
    username@instance> quit

The TableToFile class configures a map-only job to read the specified columns and
write the key/value pairs to a file in HDFS.

The following will extract the rows containing the column "cf:cq":

    $ bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.mapreduce.TableToFile -u user -p passwd -i instance -t input --columns cf:cq --output /tmp/output

    $ hadoop fs -ls /tmp/output
    -rw-r--r--   1 username supergroup          0 2013-01-10 14:44 /tmp/output/_SUCCESS
    drwxr-xr-x   - username supergroup          0 2013-01-10 14:44 /tmp/output/_logs
    drwxr-xr-x   - username supergroup          0 2013-01-10 14:44 /tmp/output/_logs/history
    -rw-r--r--   1 username supergroup       9049 2013-01-10 14:44 /tmp/output/_logs/history/job_201301081658_0011_1357847072863_username_TableToFile%5F1357847071434
    -rw-r--r--   1 username supergroup      26172 2013-01-10 14:44 /tmp/output/_logs/history/job_201301081658_0011_conf.xml
    -rw-r--r--   1 username supergroup         50 2013-01-10 14:44 /tmp/output/part-m-00000

We can see the output of our little map-reduce job:

    $ hadoop fs -text /tmp/output/output/part-m-00000
    catrow cf:cq []    catvalue
    dogrow cf:cq []    dogvalue
    $

