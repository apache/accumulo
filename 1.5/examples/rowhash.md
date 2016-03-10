---
title: RowHash Example
---

This example shows a simple map/reduce job that reads from an accumulo table and
writes back into that table.

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
    username@instance> insert a-row cf cq value
    username@instance> insert b-row cf cq value
    username@instance> quit

The RowHash class will insert a hash for each row in the database if it contains a 
specified colum.  Here's how you run the map/reduce job

    $ bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.mapreduce.RowHash -u user -p passwd -i instance -t input --column cf:cq 

Now we can scan the table and see the hashes:

    $ ./bin/accumulo shell -u username -p password
    Shell - Apache Accumulo Interactive Shell
    - version: 1.5.0
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> scan -t input
    a-row cf:cq []    value
    a-row cf-HASHTYPE:cq-MD5BASE64 []    IGPBYI1uC6+AJJxC4r5YBA==
    b-row cf:cq []    value
    b-row cf-HASHTYPE:cq-MD5BASE64 []    IGPBYI1uC6+AJJxC4r5YBA==
    username@instance> 

