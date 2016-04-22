---
title: "User Manual: Accumulo Design"
---

** Next:** [Accumulo Shell][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Introduction][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Data Model][9]
* [Architecture][10]
* [Components][11]
* [Data Management][12]
* [Tablet Service][13]
* [Compactions][14]
* [Fault-Tolerance][15]

* * *

## <a id="Accumulo_Design"></a> Accumulo Design

## <a id="Data_Model"></a> Data Model

Accumulo provides a richer data model than simple key-value stores, but is not a fully relational database. Data is represented as key-value pairs, where the key and value are comprised of the following elements: 

![converted table][16]

All elements of the Key and the Value are represented as byte arrays except for Timestamp, which is a Long. Accumulo sorts keys by element and lexicographically in ascending order. Timestamps are sorted in descending order so that later versions of the same Key appear first in a sequential scan. Tables consist of a set of sorted key-value pairs. 

## <a id="Architecture"></a> Architecture

Accumulo is a distributed data storage and retrieval system and as such consists of several architectural components, some of which run on many individual servers. Much of the work Accumulo does involves maintaining certain properties of the data, such as organization, availability, and integrity, across many commodity-class machines. 

## <a id="Components"></a> Components

An instance of Accumulo includes many TabletServers, write-ahead Logger servers, one Garbage Collector process, one Master server and many Clients. 

### <a id="Tablet_Server"></a> Tablet Server

The TabletServer manages some subset of all the tablets (partitions of tables). This includes receiving writes from clients, persisting writes to a write-ahead log, sorting new key-value pairs in memory, periodically flushing sorted key-value pairs to new files in HDFS, and responding to reads from clients, forming a merge-sorted view of all keys and values from all the files it has created and the sorted in-memory store. 

TabletServers also perform recovery of a tablet that was previously on a server that failed, reapplying any writes found in the write-ahead log to the tablet. 

### <a id="Loggers"></a> Loggers

The Loggers accept updates to Tablet servers and write them to local on-disk storage. Each tablet server will write their updates to multiple loggers to preserve data in case of hardware failure. 

### <a id="Garbage_Collector"></a> Garbage Collector

Accumulo processes will share files stored in HDFS. Periodically, the Garbage Collector will identify files that are no longer needed by any process, and delete them. 

### <a id="Master"></a> Master

The Accumulo Master is responsible for detecting and responding to TabletServer failure. It tries to balance the load across TabletServer by assigning tablets carefully and instructing TabletServers to migrate tablets when necessary. The Master ensures all tablets are assigned to one TabletServer each, and handles table creation, alteration, and deletion requests from clients. The Master also coordinates startup, graceful shutdown and recovery of changes in write-ahead logs when Tablet servers fail. 

### <a id="Client"></a> Client

Accumulo includes a client library that is linked to every application. The client library contains logic for finding servers managing a particular tablet, and communicating with TabletServers to write and retrieve key-value pairs. 

## <a id="Data_Management"></a> Data Management

Accumulo stores data in tables, which are partitioned into tablets. Tablets are partitioned on row boundaries so that all of the columns and values for a particular row are found together within the same tablet. The Master assigns Tablets to one TabletServer at a time. This enables row-level transactions to take place without using distributed locking or some other complicated synchronization mechanism. As clients insert and query data, and as machines are added and removed from the cluster, the Master migrates tablets to ensure they remain available and that the ingest and query load is balanced across the cluster. 

![Image data_distribution][17]

## <a id="Tablet_Service"></a> Tablet Service

When a write arrives at a TabletServer it is written to a Write-Ahead Log and then inserted into a sorted data structure in memory called a MemTable. When the MemTable reaches a certain size the TabletServer writes out the sorted key-value pairs to a file in HDFS called Indexed Sequential Access Method (ISAM) file. This process is called a minor compaction. A new MemTable is then created and the fact of the compaction is recorded in the Write-Ahead Log. 

When a request to read data arrives at a TabletServer, the TabletServer does a binary search across the MemTable as well as the in-memory indexes associated with each ISAM file to find the relevant values. If clients are performing a scan, several key-value pairs are returned to the client in order from the MemTable and the set of ISAM files by performing a merge-sort as they are read. 

## <a id="Compactions"></a> Compactions

In order to manage the number of files per tablet, periodically the TabletServer performs Major Compactions of files within a tablet, in which some set of ISAM files are combined into one file. The previous files will eventually be removed by the Garbage Collector. This also provides an opportunity to permanently remove deleted key-value pairs by omitting key-value pairs suppressed by a delete entry when the new file is created. 

## <a id="Fault-Tolerance"></a> Fault-Tolerance

If a TabletServer fails, the Master detects it and automatically reassigns the tablets assigned from the failed server to other servers. Any key-value pairs that were in memory at the time the TabletServer are automatically reapplied from the Write-Ahead Log to prevent any loss of data. 

The Master will coordinate the copying of write-ahead logs to HDFS so the logs are available to all tablet servers. To make recovery efficient, the updates within a log are grouped by tablet. The sorting process can be performed by Hadoops MapReduce or the Logger server. TabletServers can quickly apply the mutations from the sorted logs that are destined for the tablets they have now been assigned. 

TabletServer failures are noted on the Master's monitor page, accessible via   
http://master-address:50095/monitor. 

![Image failure_handling][18]

* * *

** Next:** [Accumulo Shell][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Introduction][6]   ** [Contents][8]**

[2]: Accumulo_Shell.html
[4]: accumulo_user_manual.html
[6]: Introduction.html
[8]: Contents.html
[9]: Accumulo_Design.html#Data_Model
[10]: Accumulo_Design.html#Architecture
[11]: Accumulo_Design.html#Components
[12]: Accumulo_Design.html#Data_Management
[13]: Accumulo_Design.html#Tablet_Service
[14]: Accumulo_Design.html#Compactions
[15]: Accumulo_Design.html#Fault-Tolerance
[16]: img1.png
[17]: ./data_distribution.png
[18]: ./failure_handling.png

