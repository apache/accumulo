<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Import test data created with Accumulo version 2.1.3

This data was created using the Accumulo shell to
 - create a table
 - add some splits so there is more than one file (optional)
 - insert some data
 - compact the table so the data is written to the tablet's files
 - offline and export the table to an hdfs directory in the /accumulo namespace

The data can be recreated using the following commands: 

Using hdfs create a directory in the accumulo namespace 
`hadoop fs -mkdir /accumulo/export_test`

Using the Accumulo shell with any version prior to 3.1

``` 
> createtable tableA
> addsplits -t tableA 2 4 6
> insert -t tableA 1 1
> insert -t tableA 1 cf cq 1
> insert -t tableA 2 cf cq 2
> insert -t tableA 3 cf cq 3
> insert -t tableA 4 cf cq 4
> insert -t tableA 5 cf cq 5
> insert -t tableA 6 cf cq 6
> insert -t tableA 7 cf cq 7

> compact -w -t tableA

to see the current tablet files: 

> scan -t accumulo.metadata -c file -np
> offline -t tableA

> exporttable -t tableA /accumulo/export_test

```

The export command will create two files:
 - /accumulo/export_test/distcp.txt
 - /accumulo/export_test/exportMetadata.zip

The distcp files lists the tablet files:

```
>  hadoop fs -cat /accumulo/export_test/distcp.txt

hdfs://localhost:8020/accumulo/tables/1/default_tablet/A000000b.rf
hdfs://localhost:8020/accumulo/tables/1/t-0000002/A000000a.rf
hdfs://localhost:8020/accumulo/tables/1/t-0000001/A0000009.rf
hdfs://localhost:8020/accumulo/tables/1/t-0000003/A0000008.rf
hdfs://localhost:8020/accumulo/export_test/exportMetadata.zip
```

The files distcp.txt, exportMetadata.zip and the files listed in distcp where copied from
hdfs to make this data set.