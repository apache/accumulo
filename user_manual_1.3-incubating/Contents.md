---
title: "User Manual: Contents"
---

** Next:** [Introduction][2] ** Up:** [Apache Accumulo User Manual Version 1.3][4] ** Previous:** [Apache Accumulo User Manual Version 1.3][4]   
  
  


### <a id="Contents"></a> Contents

* [Introduction][2]
* [Accumulo Design][6]

    * [Data Model][7]
    * [Architecture][8]
    * [Components][9]

        * [Tablet Server][10]
        * [Loggers][11]
        * [Garbage Collector][12]
        * [Master][13]
        * [Client][14]

    * [Data Management][15]
    * [Tablet Service][16]
    * [Compactions][17]
    * [Fault-Tolerance][18]

  

* [Accumulo Shell][19]

    * [Basic Administration][20]
    * [Table Maintenance][21]
    * [User Administration][22]

  

* [Writing Accumulo Clients][23]

    * [Writing Data][24]

        * [BatchWriter][25]

    * [Reading Data][26]

        * [Scanner][27]
        * [BatchScanner][28]

  

* [Table Configuration][29]

    * [Locality Groups][30]

        * [Managing Locality Groups via the Shell][31]
        * [Managing Locality Groups via the Client API][32]

    * [Constraints][33]
    * [Bloom Filters][34]
    * [Iterators][35]

        * [Setting Iterators via the Shell][36]
        * [Setting Iterators Programmatically][37]
        * [Versioning Iterators and Timestamps][38]
        * [Filtering Iterators][39]

    * [Aggregating Iterators][40]
    * [Block Cache][41]

  

* [Table Design][42]

    * [Basic Table][43]
    * [RowID Design][44]
    * [Indexing][45]
    * [Entity-Attribute and Graph Tables][46]
    * [Document-Partitioned Indexing][47]

  

* [High-Speed Ingest][48]

    * [Pre-Splitting New Tables][49]
    * [Multiple Ingester Clients][50]
    * [Bulk Ingest][51]
    * [MapReduce Ingest][52]

  

* [Analytics][53]

    * [MapReduce][54]

        * [Mapper and Reducer classes][55]
        * [AccumuloInputFormat options][56]
        * [AccumuloOutputFormat options][57]

    * [Aggregating Iterators][58]

        * [Feature Vectors][59]

    * [Statistical Modeling][60]

  

* [Security][61]

    * [Security Label Expressions][62]
    * [Security Label Expression Syntax][63]
    * [Authorization][64]
    * [Secure Authorizations Handling][65]
    * [Query Services Layer][66]

  

* [Administration][67]

    * [Hardware][68]
    * [Network][69]
    * [Installation][70]
    * [Dependencies][71]
    * [Configuration][72]

        * [Edit conf/accumulo-env.sh][73]
        * [Cluster Specification][74]
        * [Accumulo Settings][75]
        * [Deploy Configuration][76]

    * [Initialization][77]
    * [Running][78]

        * [Starting Accumulo][79]
        * [Stopping Accumulo][80]

    * [Monitoring][81]
    * [Logging][82]
    * [Recovery][83]

  

* [Shell Commands][84]

  


* * *

[2]: Introduction.html
[4]: accumulo_user_manual.html
[6]: Accumulo_Design.html
[7]: Accumulo_Design.html#Data_Model
[8]: Accumulo_Design.html#Architecture
[9]: Accumulo_Design.html#Components
[10]: Accumulo_Design.html#Tablet_Server
[11]: Accumulo_Design.html#Loggers
[12]: Accumulo_Design.html#Garbage_Collector
[13]: Accumulo_Design.html#Master
[14]: Accumulo_Design.html#Client
[15]: Accumulo_Design.html#Data_Management
[16]: Accumulo_Design.html#Tablet_Service
[17]: Accumulo_Design.html#Compactions
[18]: Accumulo_Design.html#Fault-Tolerance
[19]: Accumulo_Shell.html
[20]: Accumulo_Shell.html#Basic_Administration
[21]: Accumulo_Shell.html#Table_Maintenance
[22]: Accumulo_Shell.html#User_Administration
[23]: Writing_Accumulo_Clients.html
[24]: Writing_Accumulo_Clients.html#Writing_Data
[25]: Writing_Accumulo_Clients.html#BatchWriter
[26]: Writing_Accumulo_Clients.html#Reading_Data
[27]: Writing_Accumulo_Clients.html#Scanner
[28]: Writing_Accumulo_Clients.html#BatchScanner
[29]: Table_Configuration.html
[30]: Table_Configuration.html#Locality_Groups
[31]: Table_Configuration.html#Managing_Locality_Groups_via_the_Shell
[32]: Table_Configuration.html#Managing_Locality_Groups_via_the_Client_API
[33]: Table_Configuration.html#Constraints
[34]: Table_Configuration.html#Bloom_Filters
[35]: Table_Configuration.html#Iterators
[36]: Table_Configuration.html#Setting_Iterators_via_the_Shell
[37]: Table_Configuration.html#Setting_Iterators_Programmatically
[38]: Table_Configuration.html#Versioning_Iterators_and_Timestamps
[39]: Table_Configuration.html#Filtering_Iterators
[40]: Table_Configuration.html#Aggregating_Iterators
[41]: Table_Configuration.html#Block_Cache
[42]: Table_Design.html
[43]: Table_Design.html#Basic_Table
[44]: Table_Design.html#RowID_Design
[45]: Table_Design.html#Indexing
[46]: Table_Design.html#Entity-Attribute_and_Graph_Tables
[47]: Table_Design.html#Document-Partitioned_Indexing
[48]: High_Speed_Ingest.html
[49]: High_Speed_Ingest.html#Pre-Splitting_New_Tables
[50]: High_Speed_Ingest.html#Multiple_Ingester_Clients
[51]: High_Speed_Ingest.html#Bulk_Ingest
[52]: High_Speed_Ingest.html#MapReduce_Ingest
[53]: Analytics.html
[54]: Analytics.html#MapReduce
[55]: Analytics.html#Mapper_and_Reducer_classes
[56]: Analytics.html#AccumuloInputFormat_options
[57]: Analytics.html#AccumuloOutputFormat_options
[58]: Analytics.html#Aggregating_Iterators
[59]: Analytics.html#Feature_Vectors
[60]: Analytics.html#Statistical_Modeling
[61]: Security.html
[62]: Security.html#Security_Label_Expressions
[63]: Security.html#Security_Label_Expression_Syntax
[64]: Security.html#Authorization
[65]: Security.html#Secure_Authorizations_Handling
[66]: Security.html#Query_Services_Layer
[67]: Administration.html
[68]: Administration.html#Hardware
[69]: Administration.html#Network
[70]: Administration.html#Installation
[71]: Administration.html#Dependencies
[72]: Administration.html#Configuration
[73]: Administration.html#Edit_conf/accumulo-env.sh
[74]: Administration.html#Cluster_Specification
[75]: Administration.html#Accumulo_Settings
[76]: Administration.html#Deploy_Configuration
[77]: Administration.html#Initialization
[78]: Administration.html#Running
[79]: Administration.html#Starting_Accumulo
[80]: Administration.html#Stopping_Accumulo
[81]: Administration.html#Monitoring
[82]: Administration.html#Logging
[83]: Administration.html#Recovery
[84]: Shell_Commands.html

