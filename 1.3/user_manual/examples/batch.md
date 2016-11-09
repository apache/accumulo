---
title: Batch Writing and Scanning Example
---

This is an example of how to use the batch writer and batch scanner. To compile
the example, run maven and copy the produced jar into the accumulo lib dir.
This is already done in the tar distribution. 

Below are commands that add 10000 entries to accumulo and then do 100 random
queries.  The write command generates random 50 byte values. 

Be sure to use the name of your instance (given as instance here) and the appropriate 
list of zookeeper nodes (given as zookeepers here).

Before you run this, you must ensure that the user you are running has the
"exampleVis" authorization. (you can set this in the shell with "setauths -u username -s exampleVis")

    $ ./bin/accumulo shell -u root
    > setauths -u username -s exampleVis
    > exit

You must also create the table, batchtest1, ahead of time. (In the shell, use "createtable batchtest1")

    $ ./bin/accumulo shell -u username
    > createtable batchtest1
    > exit
    $ ./bin/accumulo org.apache.accumulo.examples.client.SequentialBatchWriter instance zookeepers username password batchtest1 0 10000 50 20000000 500 20 exampleVis
    $ ./bin/accumulo org.apache.accumulo.examples.client.RandomBatchScanner instance zookeepers username password batchtest1 100 0 10000 50 20 exampleVis
    07 11:33:11,103 [client.CountingVerifyingReceiver] INFO : Generating 100 random queries...
    07 11:33:11,112 [client.CountingVerifyingReceiver] INFO : finished
    07 11:33:11,260 [client.CountingVerifyingReceiver] INFO : 694.44 lookups/sec   0.14 secs
    
    07 11:33:11,260 [client.CountingVerifyingReceiver] INFO : num results : 100
    
    07 11:33:11,364 [client.CountingVerifyingReceiver] INFO : Generating 100 random queries...
    07 11:33:11,370 [client.CountingVerifyingReceiver] INFO : finished
    07 11:33:11,416 [client.CountingVerifyingReceiver] INFO : 2173.91 lookups/sec   0.05 secs
    
    07 11:33:11,416 [client.CountingVerifyingReceiver] INFO : num results : 100
