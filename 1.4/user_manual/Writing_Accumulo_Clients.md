---
title: "User Manual: Writing Accumulo Clients"
---

** Next:** [Development Clients][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Accumulo Shell][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Running Client Code][9]
* [Connecting][10]
* [Writing Data][11]
* [Reading Data][12]
* [Proxy][13]

* * *

## <a id="Writing_Accumulo_Clients"></a> Writing Accumulo Clients

## <a id="Running_Client_Code"></a> Running Client Code

There are multiple ways to run Java code that uses Accumulo. Below is a list of the different ways to execute client code. 

* using java executable 
* using the accumulo script 
* using the tool script 

Inorder to run client code written to run against Accumulo, you will need to include the jars that Accumulo depends on in your classpath. Accumulo client code depends on Hadoop and Zookeeper. For Hadoop add the hadoop core jar, all of the jars in the Hadoop lib directory, and the conf directory to the classpath. For Zookeeper 3.3 you only need to add the Zookeeper jar, and not what is in the Zookeeper lib directory. You can run the following command on a configured Accumulo system to see what its using for its classpath. 
    
     
    $ACCUMULO_HOME/bin/accumulo classpath
    

Another option for running your code is to put a jar file in $ACCUMULO_HOME/lib/ext. After doing this you can use the accumulo script to execute your code. For example if you create a jar containing the class com.foo.Client and placed that in lib/ext, then you could use the command $ACCUMULO_HOME/bin/accumulo com.foo.Client to execute your code. 

If you are writing map reduce job that access Accumulo, then you can use the bin/tool.sh script to run those jobs. See the map reduce example. 

## <a id="Connecting"></a> Connecting

All clients must first identify the Accumulo instance to which they will be communicating. Code to do this is as follows: 
    
    
    String instanceName = "myinstance";
    String zooServers = "zooserver-one,zooserver-two"
    Instance inst = new ZooKeeperInstance(instanceName, zooServers);
    
    Connector conn = inst.getConnector("user", "passwd");
    

## <a id="Writing_Data"></a> Writing Data

Data are written to Accumulo by creating Mutation objects that represent all the changes to the columns of a single row. The changes are made atomically in the TabletServer. Clients then add Mutations to a BatchWriter which submits them to the appropriate TabletServers. 

Mutations can be created thus: 
    
    
    Text rowID = new Text("row1");
    Text colFam = new Text("myColFam");
    Text colQual = new Text("myColQual");
    ColumnVisibility colVis = new ColumnVisibility("public");
    long timestamp = System.currentTimeMillis();
    
    Value value = new Value("myValue".getBytes());
    
    Mutation mutation = new Mutation(rowID);
    mutation.put(colFam, colQual, colVis, timestamp, value);
    

### <a id="BatchWriter"></a> BatchWriter

The BatchWriter is highly optimized to send Mutations to multiple TabletServers and automatically batches Mutations destined for the same TabletServer to amortize network overhead. Care must be taken to avoid changing the contents of any Object passed to the BatchWriter since it keeps objects in memory while batching. 

Mutations are added to a BatchWriter thus: 
    
    
    long memBuf = 1000000L; // bytes to store before sending a batch
    long timeout = 1000L; // milliseconds to wait before sending
    int numThreads = 10;
    
    BatchWriter writer =
        conn.createBatchWriter("table", memBuf, timeout, numThreads)
    
    writer.add(mutation);
    
    writer.close();
    

An example of using the batch writer can be found at   
accumulo/docs/examples/README.batch 

## <a id="Reading_Data"></a> Reading Data

Accumulo is optimized to quickly retrieve the value associated with a given key, and to efficiently return ranges of consecutive keys and their associated values. 

### <a id="Scanner"></a> Scanner

To retrieve data, Clients use a Scanner, which provides acts like an Iterator over keys and values. Scanners can be configured to start and stop at particular keys, and to return a subset of the columns available. 
    
    
    // specify which visibilities we are allowed to see
    Authorizations auths = new Authorizations("public");
    
    Scanner scan =
        conn.createScanner("table", auths);
    
    scan.setRange(new Range("harry","john"));
    scan.fetchFamily("attributes");
    
    for(Entry<Key,Value> entry : scan) {
        String row = e.getKey().getRow();
        Value value = e.getValue();
    }
    

### <a id="Isolated_Scanner"></a> Isolated Scanner

Accumulo supports the ability to present an isolated view of rows when scanning. There are three possible ways that a row could change in accumulo : 

* a mutation applied to a table 
* iterators executed as part of a minor or major compaction 
* bulk import of new files 

Isolation guarantees that either all or none of the changes made by these operations on a row are seen. Use the IsolatedScanner to obtain an isolated view of an accumulo table. When using the regular scanner it is possible to see a non isolated view of a row. For example if a mutation modifies three columns, it is possible that you will only see two of those modifications. With the isolated scanner either all three of the changes are seen or none. 

The IsolatedScanner buffers rows on the client side so a large row will not crash a tablet server. By default rows are buffered in memory, but the user can easily supply their own buffer if they wish to buffer to disk when rows are large. 

For an example, look at the following   
src/examples/src/main/java/org/apache/accumulo/examples/isolation/InterferenceTest.java

### <a id="BatchScanner"></a> BatchScanner

For some types of access, it is more efficient to retrieve several ranges simultaneously. This arises when accessing a set of rows that are not consecutive whose IDs have been retrieved from a secondary index, for example. 

The BatchScanner is configured similarly to the Scanner; it can be configured to retrieve a subset of the columns available, but rather than passing a single Range, BatchScanners accept a set of Ranges. It is important to note that the keys returned by a BatchScanner are not in sorted order since the keys streamed are from multiple TabletServers in parallel. 
    
    
    ArrayList<Range> ranges = new ArrayList<Range>();
    // populate list of ranges ...
    
    BatchScanner bscan =
        conn.createBatchScanner("table", auths, 10);
    
    bscan.setRanges(ranges);
    bscan.fetchFamily("attributes");
    
    for(Entry<Key,Value> entry : scan)
        System.out.println(e.getValue());
    

An example of the BatchScanner can be found at   
accumulo/docs/examples/README.batch 

## <a id="Proxy"></a> Proxy

The proxy API allows the interaction with Accumulo with languages other than Java. A proxy server is provided in the codebase and a client can further be generated. 

### <a id="Prequisites"></a> Prequisites

The proxy server can live on any node in which the basic client API would work. That means it must be able to communicate with the Master, ZooKeepers, NameNode, and the Data nodes. A proxy client only needs the ability to communicate with the proxy server. 

### <a id="Configuration"></a> Configuration

The configuration options for the proxy server live inside of a properties file. At the very least, you need to supply the following properties: 
    
    
    protocolFactory=org.apache.thrift.protocol.TCompactProtocol$Factory
    tokenClass=org.apache.accumulo.core.client.security.tokens.PasswordToken
    port=42424
    instance=test
    zookeepers=localhost:2181
    

You can find a sample configuration file in your distribution: 
    
    
    $ACCUMULO_HOME/proxy/proxy.properties.
    

This sample configuration file further demonstrates an ability to back the proxy server by MockAccumulo or the MiniAccumuloCluster. 

### <a id="Running_the_Proxy_Server"></a> Running the Proxy Server

After the properties file holding the configuration is created, the proxy server can be started using the following command in the Accumulo distribution (assuming your properties file is named config.properties): 
    
    
    $ACCUMULO_HOME/bin/accumulo proxy -p config.properties
    

### <a id="Creating_a_Proxy_Client"></a> Creating a Proxy Client

Aside from installing the Thrift compiler, you will also need the language-specific library for Thrift installed to generate client code in that language. Typically, your operating system's package manager will be able to automatically install these for you in an expected location such as /usr/lib/python/site-packages/thrift. 

You can find the thrift file for generating the client: 
    
    
    $ACCUMULO_HOME/proxy/proxy.thrift.
    

After a client is generated, the port specified in the configuration properties above will be used to connect to the server. 

### <a id="Using_a_Proxy_Client"></a> Using a Proxy Client

The following examples have been written in Java and the method signatures may be slightly different depending on the language specified when generating client with the Thrift compiler. After initiating a connection to the Proxy (see Apache Thrift's documentation for examples of connecting to a Thrift service), the methods on the proxy client will be available. The first thing to do is log in: 
    
    
    Map password = new HashMap<String,String>();
    password.put("password", "secret");
    ByteBuffer token = client.login("root", password);
    

Once logged in, the token returned will be used for most subsequent calls to the client. Let's create a table, add some data, scan the table, and delete it. 

First, create a table. 
    
    
    client.createTable(token, "myTable", true, TimeType.MILLIS);
    

Next, add some data: 
    
    
    // first, create a writer on the server
    String writer = client.createWriter(token, "myTable", new WriterOptions());
    
    // build column updates
    Map<ByteBuffer, List<ColumnUpdate> cells> cellsToUpdate = //...
    
    // send updates to the server
    client.updateAndFlush(writer, "myTable", cellsToUpdate);
    
    client.closeWriter(writer);
    

Scan for the data and batch the return of the results on the server: 
    
    
    String scanner = client.createScanner(token, "myTable", new ScanOptions());
    ScanResult results = client.nextK(scanner, 100);
    
    for(KeyValue keyValue : results.getResultsIterator()) {
      // do something with results
    }
    
    client.closeScanner(scanner);
    

* * *

** Next:** [Development Clients][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Accumulo Shell][6]   ** [Contents][8]**

   [2]: Development_Clients.html
   [4]: accumulo_user_manual.html
   [6]: Accumulo_Shell.html
   [8]: Contents.html
   [9]: Writing_Accumulo_Clients.html#Running_Client_Code
   [10]: Writing_Accumulo_Clients.html#Connecting
   [11]: Writing_Accumulo_Clients.html#Writing_Data
   [12]: Writing_Accumulo_Clients.html#Reading_Data
   [13]: Writing_Accumulo_Clients.html#Proxy

