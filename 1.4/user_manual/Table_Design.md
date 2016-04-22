---
title: "User Manual: Table Design"
---

** Next:** [High-Speed Ingest][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Table Configuration][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Basic Table][9]
* [RowID Design][10]
* [Indexing][11]
* [Entity-Attribute and Graph Tables][12]
* [Document-Partitioned Indexing][13]

* * *

## <a id="Table_Design"></a> Table Design

## <a id="Basic_Table"></a> Basic Table

Since Accumulo tables are sorted by row ID, each table can be thought of as being indexed by the row ID. Lookups performed row ID can be executed quickly, by doing a binary search, first across the tablets, and then within a tablet. Clients should choose a row ID carefully in order to support their desired application. A simple rule is to select a unique identifier as the row ID for each entity to be stored and assign all the other attributes to be tracked to be columns under this row ID. For example, if we have the following data in a comma-separated file: 
    
    
        userid,age,address,account-balance
    

We might choose to store this data using the userid as the rowID and the rest of the data in column families: 
    
    
    Mutation m = new Mutation(new Text(userid));
    m.put(new Text("age"), age);
    m.put(new Text("address"), address);
    m.put(new Text("balance"), account_balance);
    
    writer.add(m);
    

We could then retrieve any of the columns for a specific userid by specifying the userid as the range of a scanner and fetching specific columns: 
    
    
    Range r = new Range(userid, userid); // single row
    Scanner s = conn.createScanner("userdata", auths);
    s.setRange(r);
    s.fetchColumnFamily(new Text("age"));
    
    for(Entry<Key,Value> entry : s)
        System.out.println(entry.getValue().toString());
    

## <a id="RowID_Design"></a> RowID Design

Often it is necessary to transform the rowID in order to have rows ordered in a way that is optimal for anticipated access patterns. A good example of this is reversing the order of components of internet domain names in order to group rows of the same parent domain together: 
    
    
    com.google.code
    com.google.labs
    com.google.mail
    com.yahoo.mail
    com.yahoo.research
    

Some data may result in the creation of very large rows - rows with many columns. In this case the table designer may wish to split up these rows for better load balancing while keeping them sorted together for scanning purposes. This can be done by appending a random substring at the end of the row: 
    
    
    com.google.code_00
    com.google.code_01
    com.google.code_02
    com.google.labs_00
    com.google.mail_00
    com.google.mail_01
    

It could also be done by adding a string representation of some period of time such as date to the week or month: 
    
    
    com.google.code_201003
    com.google.code_201004
    com.google.code_201005
    com.google.labs_201003
    com.google.mail_201003
    com.google.mail_201004
    

Appending dates provides the additional capability of restricting a scan to a given date range. 

## <a id="Indexing"></a> Indexing

In order to support lookups via more than one attribute of an entity, additional indexes can be built. However, because Accumulo tables can support any number of columns without specifying them beforehand, a single additional index will often suffice for supporting lookups of records in the main table. Here, the index has, as the rowID, the Value or Term from the main table, the column families are the same, and the column qualifier of the index table contains the rowID from the main table. 

![converted table][14]

Note: We store rowIDs in the column qualifier rather than the Value so that we can have more than one rowID associated with a particular term within the index. If we stored this in the Value we would only see one of the rows in which the value appears since Accumulo is configured by default to return the one most recent value associated with a key. 

Lookups can then be done by scanning the Index Table first for occurrences of the desired values in the columns specified, which returns a list of row ID from the main table. These can then be used to retrieve each matching record, in their entirety, or a subset of their columns, from the Main Table. 

To support efficient lookups of multiple rowIDs from the same table, the Accumulo client library provides a BatchScanner. Users specify a set of Ranges to the BatchScanner, which performs the lookups in multiple threads to multiple servers and returns an Iterator over all the rows retrieved. The rows returned are NOT in sorted order, as is the case with the basic Scanner interface. 
    
    
    // first we scan the index for IDs of rows matching our query
    
    Text term = new Text("mySearchTerm");
    
    HashSet<Text> matchingRows = new HashSet<Text>();
    
    Scanner indexScanner = createScanner("index", auths);
    indexScanner.setRange(new Range(term, term));
    
    // we retrieve the matching rowIDs and create a set of ranges
    for(Entry<Key,Value> entry : indexScanner)
        matchingRows.add(new Text(entry.getKey().getColumnQualifier()));
    
    // now we pass the set of rowIDs to the batch scanner to retrieve them
    BatchScanner bscan = conn.createBatchScanner("table", auths, 10);
    
    bscan.setRanges(matchingRows);
    bscan.fetchFamily("attributes");
    
    for(Entry<Key,Value> entry : scan)
        System.out.println(entry.getValue());
    

One advantage of the dynamic schema capabilities of Accumulo is that different fields may be indexed into the same physical table. However, it may be necessary to create different index tables if the terms must be formatted differently in order to maintain proper sort order. For example, real numbers must be formatted differently than their usual notation in order to be sorted correctly. In these cases, usually one index per unique data type will suffice. 

## <a id="Entity-Attribute_and_Graph_Tables"></a> Entity-Attribute and Graph Tables

Accumulo is ideal for storing entities and their attributes, especially of the attributes are sparse. It is often useful to join several datasets together on common entities within the same table. This can allow for the representation of graphs, including nodes, their attributes, and connections to other nodes. 

Rather than storing individual events, Entity-Attribute or Graph tables store aggregate information about the entities involved in the events and the relationships between entities. This is often preferrable when single events aren't very useful and when a continuously updated summarization is desired. 

The physical schema for an entity-attribute or graph table is as follows: 

![converted table][15]

For example, to keep track of employees, managers and products the following entity-attribute table could be used. Note that the weights are not always necessary and are set to 0 when not used. 

![converted table][16]   
  


To allow efficient updating of edge weights, an aggregating iterator can be configured to add the value of all mutations applied with the same key. These types of tables can easily be created from raw events by simply extracting the entities, attributes, and relationships from individual events and inserting the keys into Accumulo each with a count of 1. The aggregating iterator will take care of maintaining the edge weights. 

## <a id="Document-Partitioned_Indexing"></a> Document-Partitioned Indexing

Using a simple index as described above works well when looking for records that match one of a set of given criteria. When looking for records that match more than one criterion simultaneously, such as when looking for documents that contain all of the words `the' and `white' and `house', there are several issues. 

First is that the set of all records matching any one of the search terms must be sent to the client, which incurs a lot of network traffic. The second problem is that the client is responsible for performing set intersection on the sets of records returned to eliminate all but the records matching all search terms. The memory of the client may easily be overwhelmed during this operation. 

For these reasons Accumulo includes support for a scheme known as sharded indexing, in which these set operations can be performed at the TabletServers and decisions about which records to include in the result set can be made without incurring network traffic. 

This is accomplished via partitioning records into bins that each reside on at most one TabletServer, and then creating an index of terms per record within each bin as follows: 

![converted table][17]

Documents or records are mapped into bins by a user-defined ingest application. By storing the BinID as the RowID we ensure that all the information for a particular bin is contained in a single tablet and hosted on a single TabletServer since Accumulo never splits rows across tablets. Storing the Terms as column families serves to enable fast lookups of all the documents within this bin that contain the given term. 

Finally, we perform set intersection operations on the TabletServer via a special iterator called the Intersecting Iterator. Since documents are partitioned into many bins, a search of all documents must search every bin. We can use the BatchScanner to scan all bins in parallel. The Intersecting Iterator should be enabled on a BatchScanner within user query code as follows: 
    
    
    Text[] terms = {new Text("the"), new Text("white"), new Text("house")};
    
    BatchScanner bs = conn.createBatchScanner(table, auths, 20);
    IteratorSetting iter = new IteratorSetting(20, "ii", IntersectingIterator.class);
    IntersectingIterator.setColumnFamilies(iter, terms);
    bs.addScanIterator(iter);
    bs.setRanges(Collections.singleton(new Range()));
    
    for(Entry<Key,Value> entry : bs) {
        System.out.println(" " + entry.getKey().getColumnQualifier());
    }
    

This code effectively has the BatchScanner scan all tablets of a table, looking for documents that match all the given terms. Because all tablets are being scanned for every query, each query is more expensive than other Accumulo scans, which typically involve a small number of TabletServers. This reduces the number of concurrent queries supported and is subject to what is known as the `straggler' problem in which every query runs as slow as the slowest server participating. 

Of course, fast servers will return their results to the client which can display them to the user immediately while they wait for the rest of the results to arrive. If the results are unordered this is quite effective as the first results to arrive are as good as any others to the user. 

* * *

** Next:** [High-Speed Ingest][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Table Configuration][6]   ** [Contents][8]**

[2]: High_Speed_Ingest.html
[4]: accumulo_user_manual.html
[6]: Table_Configuration.html
[8]: Contents.html
[9]: Table_Design.html#Basic_Table
[10]: Table_Design.html#RowID_Design
[11]: Table_Design.html#Indexing
[12]: Table_Design.html#Entity-Attribute_and_Graph_Tables
[13]: Table_Design.html#Document-Partitioned_Indexing
[14]: img4.png
[15]: img5.png
[16]: img6.png
[17]: img7.png

