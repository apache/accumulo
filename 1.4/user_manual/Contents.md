---
title: "User Manual: Contents"
---

** Next:** [Introduction][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Apache Accumulo User Manual Version 1.4][4]   
  
  


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

    * [Running Client Code][24]
    * [Connecting][25]
    * [Writing Data][26]

        * [BatchWriter][27]

    * [Reading Data][28]

        * [Scanner][29]
        * [Isolated Scanner][30]
        * [BatchScanner][31]

    * [Proxy][32]

        * [Prequisites][33]
        * [Configuration][34]
        * [Running the Proxy Server][35]
        * [Creating a Proxy Client][36]
        * [Using a Proxy Client][37]

  

* [Development Clients][38]

    * [Mock Accumulo][39]
    * [Mini Accumulo Cluster][40]

  

* [Table Configuration][41]

    * [Locality Groups][42]

        * [Managing Locality Groups via the Shell][43]
        * [Managing Locality Groups via the Client API][44]

    * [Constraints][45]
    * [Bloom Filters][46]
    * [Iterators][47]

        * [Setting Iterators via the Shell][48]
        * [Setting Iterators Programmatically][49]
        * [Versioning Iterators and Timestamps][50]
        * [Filters][51]
        * [Combiners][52]

    * [Block Cache][53]
    * [Compaction][54]
    * [Pre-splitting tables][55]
    * [Merging tablets][56]
    * [Delete Range][57]
    * [Cloning Tables][58]

  

* [Table Design][59]

    * [Basic Table][60]
    * [RowID Design][61]
    * [Indexing][62]
    * [Entity-Attribute and Graph Tables][63]
    * [Document-Partitioned Indexing][64]

  

* [High-Speed Ingest][65]

    * [Pre-Splitting New Tables][66]
    * [Multiple Ingester Clients][67]
    * [Bulk Ingest][68]
    * [Logical Time for Bulk Ingest][69]
    * [MapReduce Ingest][70]

  

* [Analytics][71]

    * [MapReduce][72]

        * [Mapper and Reducer classes][73]
        * [AccumuloInputFormat options][74]
        * [AccumuloOutputFormat options][75]

    * [Combiners][76]

        * [Feature Vectors][77]

    * [Statistical Modeling][78]

  

* [Security][79]

    * [Security Label Expressions][80]
    * [Security Label Expression Syntax][81]
    * [Authorization][82]
    * [User Authorizations][83]
    * [Secure Authorizations Handling][84]
    * [Query Services Layer][85]

  

* [Administration][86]

    * [Hardware][87]
    * [Network][88]
    * [Installation][89]
    * [Dependencies][90]
    * [Configuration][91]

        * [Edit conf/accumulo-env.sh][92]
        * [Cluster Specification][93]
        * [Accumulo Settings][94]
        * [Deploy Configuration][95]

    * [Initialization][96]
    * [Running][97]

        * [Starting Accumulo][98]
        * [Stopping Accumulo][99]
        * [Adding a Node][100]
        * [Decomissioning a Node][101]

    * [Monitoring][102]
    * [Logging][103]
    * [Recovery][104]

  

* [Shell Commands][105]

  


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
[24]: Writing_Accumulo_Clients.html#Running_Client_Code
[25]: Writing_Accumulo_Clients.html#Connecting
[26]: Writing_Accumulo_Clients.html#Writing_Data
[27]: Writing_Accumulo_Clients.html#BatchWriter
[28]: Writing_Accumulo_Clients.html#Reading_Data
[29]: Writing_Accumulo_Clients.html#Scanner
[30]: Writing_Accumulo_Clients.html#Isolated_Scanner
[31]: Writing_Accumulo_Clients.html#BatchScanner
[32]: Writing_Accumulo_Clients.html#Proxy
[33]: Writing_Accumulo_Clients.html#Prequisites
[34]: Writing_Accumulo_Clients.html#Configuration
[35]: Writing_Accumulo_Clients.html#Running_the_Proxy_Server
[36]: Writing_Accumulo_Clients.html#Creating_a_Proxy_Client
[37]: Writing_Accumulo_Clients.html#Using_a_Proxy_Client
[38]: Development_Clients.html
[39]: Development_Clients.html#Mock_Accumulo
[40]: Development_Clients.html#Mini_Accumulo_Cluster
[41]: Table_Configuration.html
[42]: Table_Configuration.html#Locality_Groups
[43]: Table_Configuration.html#Managing_Locality_Groups_via_the_Shell
[44]: Table_Configuration.html#Managing_Locality_Groups_via_the_Client_API
[45]: Table_Configuration.html#Constraints
[46]: Table_Configuration.html#Bloom_Filters
[47]: Table_Configuration.html#Iterators
[48]: Table_Configuration.html#Setting_Iterators_via_the_Shell
[49]: Table_Configuration.html#Setting_Iterators_Programmatically
[50]: Table_Configuration.html#Versioning_Iterators_and_Timestamps
[51]: Table_Configuration.html#Filters
[52]: Table_Configuration.html#Combiners
[53]: Table_Configuration.html#Block_Cache
[54]: Table_Configuration.html#Compaction
[55]: Table_Configuration.html#Pre-splitting_tables
[56]: Table_Configuration.html#Merging_tablets
[57]: Table_Configuration.html#Delete_Range
[58]: Table_Configuration.html#Cloning_Tables
[59]: Table_Design.html
[60]: Table_Design.html#Basic_Table
[61]: Table_Design.html#RowID_Design
[62]: Table_Design.html#Indexing
[63]: Table_Design.html#Entity-Attribute_and_Graph_Tables
[64]: Table_Design.html#Document-Partitioned_Indexing
[65]: High_Speed_Ingest.html
[66]: High_Speed_Ingest.html#Pre-Splitting_New_Tables
[67]: High_Speed_Ingest.html#Multiple_Ingester_Clients
[68]: High_Speed_Ingest.html#Bulk_Ingest
[69]: High_Speed_Ingest.html#Logical_Time_for_Bulk_Ingest
[70]: High_Speed_Ingest.html#MapReduce_Ingest
[71]: Analytics.html
[72]: Analytics.html#MapReduce
[73]: Analytics.html#Mapper_and_Reducer_classes
[74]: Analytics.html#AccumuloInputFormat_options
[75]: Analytics.html#AccumuloOutputFormat_options
[76]: Analytics.html#Combiners
[77]: Analytics.html#Feature_Vectors
[78]: Analytics.html#Statistical_Modeling
[79]: Security.html
[80]: Security.html#Security_Label_Expressions
[81]: Security.html#Security_Label_Expression_Syntax
[82]: Security.html#Authorization
[83]: Security.html#User_Authorizations
[84]: Security.html#Secure_Authorizations_Handling
[85]: Security.html#Query_Services_Layer
[86]: Administration.html
[87]: Administration.html#Hardware
[88]: Administration.html#Network
[89]: Administration.html#Installation
[90]: Administration.html#Dependencies
[91]: Administration.html#Configuration
[92]: Administration.html#Edit_conf/accumulo-env.sh
[93]: Administration.html#Cluster_Specification
[94]: Administration.html#Accumulo_Settings
[95]: Administration.html#Deploy_Configuration
[96]: Administration.html#Initialization
[97]: Administration.html#Running
[98]: Administration.html#Starting_Accumulo
[99]: Administration.html#Stopping_Accumulo
[100]: Administration.html#Adding_a_Node
[101]: Administration.html#Decomissioning_a_Node
[102]: Administration.html#Monitoring
[103]: Administration.html#Logging
[104]: Administration.html#Recovery
[105]: Shell_Commands.html

