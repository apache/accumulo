---
title: Aggregation Example
---

This is a simple aggregation example.  To build this example run maven and then
copy the produced jar into the accumulo lib dir.  This is already done in the
tar distribution.

    $ bin/accumulo shell -u username
    Enter current password for 'username'@'instance': ***
    
    Shell - Apache Accumulo Interactive Shell
    - 
    - version: 1.3.x-incubating
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> createtable aggtest1 -a app=org.apache.accumulo.examples.aggregation.SortedSetAggregator
    username@instance aggtest1> insert foo app 1 a
    username@instance aggtest1> insert foo app 1 b
    username@instance aggtest1> scan
    foo app:1 []  a,b
    username@instance aggtest1> insert foo app 1 z,1,foo,w
    username@instance aggtest1> scan
    foo app:1 []  1,a,b,foo,w,z
    username@instance aggtest1> insert foo app 2 cat,dog,muskrat
    username@instance aggtest1> insert foo app 2 mouse,bird
    username@instance aggtest1> scan
    foo app:1 []  1,a,b,foo,w,z
    foo app:2 []  bird,cat,dog,mouse,muskrat
    username@instance aggtest1> 

In this example a table is created and the example set aggregator is
applied to the column family app.
