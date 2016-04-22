---
title: "User Manual: Table Configuration"
---

** Next:** [Table Design][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Writing Accumulo Clients][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Locality Groups][9]
* [Constraints][10]
* [Bloom Filters][11]
* [Iterators][12]
* [Aggregating Iterators][13]
* [Block Cache][14]

* * *

## <a id="Table_Configuration"></a> Table Configuration

Accumulo tables have a few options that can be configured to alter the default behavior of Accumulo as well as improve performance based on the data stored. These include locality groups, constraints, and iterators. 

## <a id="Locality_Groups"></a> Locality Groups

Accumulo supports storing of sets of column families separately on disk to allow clients to scan over columns that are frequently used together efficient and to avoid scanning over column families that are not requested. After a locality group is set Scanner and BatchScanner operations will automatically take advantage of them whenever the fetchColumnFamilies() method is used. 

By default tables place all column families into the same ``default" locality group. Additional locality groups can be configured anytime via the shell or programmatically as follows: 

### <a id="Managing_Locality_Groups_via_the_Shell"></a> Managing Locality Groups via the Shell
    
    
    usage: setgroups <group>=<col fam>{,<col fam>}{ <group>=<col fam>{,<col
    fam>}} [-?] -t <table>
    
    user@myinstance mytable> setgroups -t mytable group_one=colf1,colf2
    
    user@myinstance mytable> getgroups -t mytable
    group_one=colf1,colf2
    

### <a id="Managing_Locality_Groups_via_the_Client_API"></a> Managing Locality Groups via the Client API
    
    
    Connector conn;
    
    HashMap<String,Set<Text>> localityGroups =
        new HashMap<String, Set<Text>>();
    
    HashSet<Text> metadataColumns = new HashSet<Text>();
    metadataColumns.add(new Text("domain"));
    metadataColumns.add(new Text("link"));
    
    HashSet<Text> contentColumns = new HashSet<Text>();
    contentColumns.add(new Text("body"));
    contentColumns.add(new Text("images"));
    
    localityGroups.put("metadata", metadataColumns);
    localityGroups.put("content", contentColumns);
    
    conn.tableOperations().setLocalityGroups("mytable", localityGroups);
    
    // existing locality groups can be obtained as follows
    Map<String, Set<Text>> groups =
        conn.tableOperations().getLocalityGroups("mytable");
    

The assignment of Column Families to Locality Groups can be changed anytime. The physical movement of column families into their new locality groups takes place via the periodic Major Compaction process that takes place continuously in the background. Major Compaction can also be scheduled to take place immediately through the shell: 
    
    
    user@myinstance mytable> compact -t mytable
    

## <a id="Constraints"></a> Constraints

Accumulo supports constraints applied on mutations at insert time. This can be used to disallow certain inserts according to a user defined policy. Any mutation that fails to meet the requirements of the constraint is rejected and sent back to the client. 

Constraints can be enabled by setting a table property as follows: 
    
    
    user@myinstance mytable> config -t mytable -s table.constraint.1=com.test.ExampleConstraint
    user@myinstance mytable> config -t mytable -s table.constraint.2=com.test.AnotherConstraint
    user@myinstance mytable> config -t mytable -f constraint
    ---------+--------------------------------+----------------------------
    SCOPE    | NAME                           | VALUE
    ---------+--------------------------------+----------------------------
    table    | table.constraint.1............ | com.test.ExampleConstraint
    table    | table.constraint.2............ | com.test.AnotherConstraint
    ---------+--------------------------------+----------------------------
    

Currently there are no general-purpose constraints provided with the Accumulo distribution. New constraints can be created by writing a Java class that implements the org.apache.accumulo.core.constraints.Constraint interface. 

To deploy a new constraint, create a jar file containing the class implementing the new constraint and place it in the lib directory of the Accumulo installation. New constraint jars can be added to Accumulo and enabled without restarting but any change to an existing constraint class requires Accumulo to be restarted. 

An example of constraints can be found in   
accumulo/docs/examples/README.constraints with corresponding code under   
accumulo/src/examples/main/java/accumulo/examples/constraints . 

## <a id="Bloom_Filters"></a> Bloom Filters

As mutations are applied to an Accumulo table, several files are created per tablet. If bloom filters are enabled, Accumulo will create and load a small data structure into memory to determine whether a file contains a given key before opening the file. This can speed up lookups considerably. 

To enable bloom filters, enter the following command in the Shell: 
    
    
    user@myinstance> config -t mytable -s table.bloom.enabled=true
    

An extensive example of using Bloom Filters can be found at   
accumulo/docs/examples/README.bloom . 

## <a id="Iterators"></a> Iterators

Iterators provide a modular mechanism for adding functionality to be executed by TabletServers when scanning or compacting data. This allows users to efficiently summarize, filter, and aggregate data. In fact, the built-in features of cell-level security and age-off are implemented using Iterators. 

### <a id="Setting_Iterators_via_the_Shell"></a> Setting Iterators via the Shell
    
    
    usage: setiter [-?] -agg | -class <name> | -filter | -nolabel | 
    -regex | -vers [-majc] [-minc] [-n <itername>] -p <pri> [-scan] 
    [-t <table>]
    
    user@myinstance mytable> setiter -t mytable -scan -p 10 -n myiter
    

### <a id="Setting_Iterators_Programmatically"></a> Setting Iterators Programmatically
    
    
    scanner.setScanIterators(
        15, // priority
        "com.company.MyIterator", // class name
        "myiter"); // name this iterator
    

Some iterators take additional parameters from client code, as in the following example: 
    
    
    bscan.setIteratorOption(
        "myiter", // iterator reference
        "myoptionname",
        "myoptionvalue");
    

Tables support separate Iterator settings to be applied at scan time, upon minor compaction and upon major compaction. For most uses, tables will have identical iterator settings for all three to avoid inconsistent results. 

### <a id="Versioning_Iterators_and_Timestamps"></a> Versioning Iterators and Timestamps

Accumulo provides the capability to manage versioned data through the use of timestamps within the Key. If a timestamp is not specified in the key created by the client then the system will set the timestamp to the current time. Two keys with identical rowIDs and columns but different timestamps are considered two versions of the same key. If two inserts are made into accumulo with the same rowID, column, and timestamp, then the behavior is non-deterministic. 

Timestamps are sorted in descending order, so the most recent data comes first. Accumulo can be configured to return the top k versions, or versions later than a given date. The default is to return the one most recent version. 

The version policy can be changed by changing the VersioningIterator options for a table as follows: 
    
    
    user@myinstance mytable> config -t mytable -s
    table.iterator.scan.vers.opt.maxVersions=3
    
    user@myinstance mytable> config -t mytable -s
    table.iterator.minc.vers.opt.maxVersions=3
    
    user@myinstance mytable> config -t mytable -s
    table.iterator.majc.vers.opt.maxVersions=3
    

#### <a id="Logical_Time"></a> Logical Time

Accumulo 1.2 introduces the concept of logical time. This ensures that timestamps set by accumulo always move forward. This helps avoid problems caused by TabletServers that have different time settings. The per tablet counter gives unique one up time stamps on a per mutation basis. When using time in milliseconds, if two things arrive within the same millisecond then both receive the same timestamp. 

A table can be configured to use logical timestamps at creation time as follows: 
    
    
    user@myinstance> createtable -tl logical
    

#### <a id="Deletes"></a> Deletes

Deletes are special keys in accumulo that get sorted along will all the other data. When a delete key is inserted, accumulo will not show anything that has a timestamp less than or equal to the delete key. During major compaction, any keys older than a delete key are omitted from the new file created, and the omitted keys are removed from disk as part of the regular garbage collection process. 

### <a id="Filtering_Iterators"></a> Filtering Iterators

When scanning over a set of key-value pairs it is possible to apply an arbitrary filtering policy through the use of a FilteringIterator. These types of iterators return only key-value pairs that satisfy the filter logic. Accumulo has two built-in filtering iterators that can be configured on any table: AgeOff and RegEx. More can be added by writing a Java class that implements the   
org.apache.accumulo.core.iterators.filter.Filter interface. 

To configure the AgeOff filter to remove data older than a certain date or a fixed amount of time from the present. The following example sets a table to delete everything inserted over 30 seconds ago: 
    
    
    user@myinstance> createtable filtertest
    user@myinstance filtertest> setiter -t filtertest -scan -minc -majc -p
    10 -n myfilter -filter
    
    FilteringIterator uses Filters to accept or reject key/value pairs
    ----------> entering options: <filterPriorityNumber>
    <ageoff|regex|filterClass>
    
    ----------> set org.apache.accumulo.core.iterators.FilteringIterator option
    (<name> <value>, hit enter to skip): 0 ageoff
    
    ----------> set org.apache.accumulo.core.iterators.FilteringIterator option
    (<name> <value>, hit enter to skip):
    AgeOffFilter removes entries with timestamps more than <ttl>
    milliseconds old
    
    ----------> set org.apache.accumulo.core.iterators.filter.AgeOffFilter parameter
    currentTime, if set, use the given value as the absolute time in
    milliseconds as the current time of day:
    
    ----------> set org.apache.accumulo.core.iterators.filter.AgeOffFilter parameter
    ttl, time to live (milliseconds): 30000
    
    user@myinstance filtertest>
    user@myinstance filtertest> scan
    user@myinstance filtertest> insert foo a b c
    insert successful
    user@myinstance filtertest> scan
    foo a:b [] c
    
    ... wait 30 seconds ...
    
    user@myinstance filtertest> scan
    user@myinstance filtertest>
    

To see the iterator settings for a table, use: 
    
    
    user@example filtertest> config -t filtertest -f iterator
    ---------+------------------------------------------+------------------
    SCOPE    | NAME                                     | VALUE
    ---------+------------------------------------------+------------------
    table    | table.iterator.majc.myfilter ........... |
    10,org.apache.accumulo.core.iterators.FilteringIterator
    table    | table.iterator.majc.myfilter.opt.0 ..... |
    org.apache.accumulo.core.iterators.filter.AgeOffFilter
    table    | table.iterator.majc.myfilter.opt.0.ttl . | 30000
    table    | table.iterator.minc.myfilter ........... |
    10,org.apache.accumulo.core.iterators.FilteringIterator
    table    | table.iterator.minc.myfilter.opt.0 ..... |
    org.apache.accumulo.core.iterators.filter.AgeOffFilter
    table    | table.iterator.minc.myfilter.opt.0.ttl . | 30000
    table    | table.iterator.scan.myfilter ........... |
    10,org.apache.accumulo.core.iterators.FilteringIterator
    table    | table.iterator.scan.myfilter.opt.0 ..... |
    org.apache.accumulo.core.iterators.filter.AgeOffFilter
    table    | table.iterator.scan.myfilter.opt.0.ttl . | 30000
    ---------+------------------------------------------+------------------
    

## <a id="Aggregating_Iterators"></a> Aggregating Iterators

Accumulo allows aggregating iterators to be configured on tables and column families. When an aggregating iterator is set, the iterator is applied across the values associated with any keys that share rowID, column family, and column qualifier. This is similar to the reduce step in MapReduce, which applied some function to all the values associated with a particular key. 

For example, if an aggregating iterator were configured on a table and the following mutations were inserted: 
    
    
    Row     Family Qualifier Timestamp  Value
    rowID1  colfA  colqA     20100101   1
    rowID1  colfA  colqA     20100102   1
    

The table would reflect only one aggregate value: 
    
    
    rowID1  colfA  colqA     -          2
    

Aggregating iterators can be enabled for a table as follows: 
    
    
    user@myinstance> createtable perDayCounts -a
    day=org.apache.accumulo.core.iterators.aggregation.StringSummation
    
    user@myinstance perDayCounts> insert row1 day 20080101 1
    user@myinstance perDayCounts> insert row1 day 20080101 1
    user@myinstance perDayCounts> insert row1 day 20080103 1
    user@myinstance perDayCounts> insert row2 day 20080101 1
    user@myinstance perDayCounts> insert row3 day 20080101 1
    
    user@myinstance perDayCounts> scan
    row1 day:20080101 [] 2
    row1 day:20080103 [] 1
    row2 day:20080101 [] 2
    

Accumulo includes the following aggregators: 

* **LongSummation**: expects values of type long and adds them. 
* **StringSummation**: expects numbers represented as strings and adds them. 
* **StringMax**: expects numbers as strings and retains the maximum number inserted. 
* **StringMin**: expects numbers as strings and retains the minimum number inserted. 

Additional Aggregators can be added by creating a Java class that implements   
**org.apache.accumulo.core.iterators.aggregation.Aggregator** and adding a jar containing that class to Accumulo's lib directory. 

An example of an aggregator can be found under   
accumulo/src/examples/main/java/org/apache/accumulo/examples/aggregation/SortedSetAggregator.java 

## <a id="Block_Cache"></a> Block Cache

In order to increase throughput of commonly accessed entries, Accumulo employs a block cache. This block cache buffers data in memory so that it doesn't have to be read off of disk. The RFile format that Accumulo prefers is a mix of index blocks and data blocks, where the index blocks are used to find the appropriate data blocks. Typical queries to Accumulo result in a binary search over several index blocks followed by a linear scan of one or more data blocks. 

The block cache can be configured on a per-table basis, and all tablets hosted on a tablet server share a single resource pool. To configure the size of the tablet server's block cache, set the following properties: 
    
    
    tserver.cache.data.size: Specifies the size of the cache for file data blocks.
    tserver.cache.index.size: Specifies the size of the cache for file indices.
    

To enable the block cache for your table, set the following properties: 
    
    
    table.cache.block.enable: Determines whether file (data) block cache is enabled.
    table.cache.index.enable: Determines whether index cache is enabled.
    

The block cache can have a significant effect on alleviating hot spots, as well as reducing query latency. It is enabled by default for the !METADATA table. 

* * *

** Next:** [Table Design][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Writing Accumulo Clients][6]   ** [Contents][8]**

[2]: Table_Design.html
[4]: accumulo_user_manual.html
[6]: Writing_Accumulo_Clients.html
[8]: Contents.html
[9]: Table_Configuration.html#Locality_Groups
[10]: Table_Configuration.html#Constraints
[11]: Table_Configuration.html#Bloom_Filters
[12]: Table_Configuration.html#Iterators
[13]: Table_Configuration.html#Aggregating_Iterators
[14]: Table_Configuration.html#Block_Cache

