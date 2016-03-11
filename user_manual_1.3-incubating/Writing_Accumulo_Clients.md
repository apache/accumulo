---
title: "User Manual: Writing Accumulo Clients"
---

** Next:** [Table Configuration][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Accumulo Shell][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Writing Data][9]
* [Reading Data][10]

* * *

## <a id="Writing_Accumulo_Clients"></a> Writing Accumulo Clients

All clients must first identify the Accumulo instance to which they will be communicating. Code to do this is as follows: 
    
    
    String instanceName = "myinstance";
    String zooServers = "zooserver-one,zooserver-two"
    Instance inst = new ZooKeeperInstance(instanceName, zooServers);
    
    Connector conn = new Connector(inst, "user","passwd".getBytes());
    

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

* * *

** Next:** [Table Configuration][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Accumulo Shell][6]   ** [Contents][8]**

   [2]: Table_Configuration.html
   [4]: accumulo_user_manual.html
   [6]: Accumulo_Shell.html
   [8]: Contents.html
   [9]: Writing_Accumulo_Clients.html#Writing_Data
   [10]: Writing_Accumulo_Clients.html#Reading_Data

