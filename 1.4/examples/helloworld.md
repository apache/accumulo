---
title: Hello World Example
---

This tutorial uses the following Java classes, which can be found in org.apache.accumulo.examples.simple.helloworld in the simple-examples module: 

 * InsertWithBatchWriter.java - Inserts 10K rows (50K entries) into accumulo with each row having 5 entries
 * InsertWithOutputFormat.java - Example of inserting data in MapReduce
 * ReadData.java - Reads all data between two rows

Log into the accumulo shell:

    $ ./bin/accumulo shell -u username -p password

Create a table called 'hellotable':

    username@instance> createtable hellotable

Launch a Java program that inserts data with a BatchWriter:

    $ ./bin/accumulo org.apache.accumulo.examples.simple.helloworld.InsertWithBatchWriter instance zookeepers username password hellotable 

Alternatively, the same data can be inserted using MapReduce writers:

    $ ./bin/accumulo org.apache.accumulo.examples.simple.helloworld.InsertWithOutputFormat instance zookeepers username password hellotable 

On the accumulo status page at the URL below (where 'master' is replaced with the name or IP of your accumulo master), you should see 50K entries

    http://master:50095/

To view the entries, use the shell to scan the table:

    username@instance> table hellotable
    username@instance hellotable> scan

You can also use a Java class to scan the table:

    $ ./bin/accumulo org.apache.accumulo.examples.simple.helloworld.ReadData instance zookeepers username password hellotable row_0 row_1001
