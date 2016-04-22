---
title: "User Manual: Development Clients"
---

** Next:** [Table Configuration][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Writing Accumulo Clients][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Mock Accumulo][9]
* [Mini Accumulo Cluster][10]

* * *

## <a id="Development_Clients"></a> Development Clients

Normally, Accumulo consists of lots of moving parts. Even a stand-alone version of Accumulo requires Hadoop, Zookeeper, the Accumulo master, a tablet server, etc. If you want to write a unit test that uses Accumulo, you need a lot of infrastructure in place before your test can run. 

## <a id="Mock_Accumulo"></a> Mock Accumulo

Mock Accumulo supplies mock implementations for much of the client API. It presently does not enforce users, logins, permissions, etc. It does support Iterators and Combiners. Note that MockAccumulo holds all data in memory, and will not retain any data or settings between runs. 

While normal interaction with the Accumulo client looks like this: 
    
    
    Instance instance = new ZooKeeperInstance(...);
    Connector conn = instance.getConnector(user, passwd);
    

To interact with the MockAccumulo, just replace the ZooKeeperInstance with MockInstance: 
    
    
    Instance instance = new MockInstance();
    

In fact, you can use the "-fake" option to the Accumulo shell and interact with MockAccumulo: 
    
    
    $ ./bin/accumulo shell --fake -u root -p ''
    
    Shell - Apache Accumulo Interactive Shell
    -
    - version: 1.4.4
    - instance name: mock-instance
    - instance id: mock-instance-id
    -
    - type 'help' for a list of available commands
    -
    root@mock-instance> createtable test
    root@mock-instance test> insert row1 cf cq value
    root@mock-instance test> insert row2 cf cq value2
    root@mock-instance test> insert row3 cf cq value3
    root@mock-instance test> scan
    row1 cf:cq []    value
    row2 cf:cq []    value2
    row3 cf:cq []    value3
    root@mock-instance test> scan -b row2 -e row2
    row2 cf:cq []    value2
    root@mock-instance test>
    

When testing Map Reduce jobs, you can also set the Mock Accumulo on the AccumuloInputFormat and AccumuloOutputFormat classes: 
    
    
    // ... set up job configuration
    AccumuloInputFormat.setMockInstance(job, "mockInstance");
    AccumuloOutputFormat.setMockInstance(job, "mockInstance");
    

## <a id="Mini_Accumulo_Cluster"></a> Mini Accumulo Cluster

While the Mock Accumulo provides a lightweight implementation of the client API for unit testing, it is often necessary to write more realistic end-to-end integration tests that take advantage of the entire ecosystem. The Mini Accumulo Cluster makes this possible by configuring and starting Zookeeper, initializing Accumulo, and starting the Master as well as some Tablet Servers. It runs against the local filesystem instead of having to start up HDFS. 

To start it up, you will need to supply an empty directory and a root password as arguments: 
    
    
    File tempDirectory = // JUnit and Guava supply mechanisms for creating temp directories
    MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDirectory, "password");
    accumulo.start();
    

Once we have our mini cluster running, we will want to interact with the Accumulo client API: 
    
    
    Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    Connector conn = instance.getConnector("root", "password");
    

Upon completion of our development code, we will want to shutdown our MiniAccumuloCluster: 
    
    
    accumulo.stop()
    // delete your temporary folder
    

* * *

** Next:** [Table Configuration][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Writing Accumulo Clients][6]   ** [Contents][8]**

[2]: Table_Configuration.html
[4]: accumulo_user_manual.html
[6]: Writing_Accumulo_Clients.html
[8]: Contents.html
[9]: Development_Clients.html#Mock_Accumulo
[10]: Development_Clients.html#Mini_Accumulo_Cluster

