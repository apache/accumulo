<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
---
title: Apache Accumulo Examples
---

## Setup instructions

Before running any of the examples, the following steps must be performed.

1. Install and run Accumulo via the instructions found in INSTALL.md.
   Remember the instance name. It will be referred to as "instance" throughout
   the examples. A comma-separated list of zookeeper servers will be referred
   to as "zookeepers".

2. Create an Accumulo user (for help see the 'User Administration' section of the 
   [user manual][manual]), or use the root user. This user and their password
   should replace any reference to "username" or "password" in the examples. This
   user needs the ability to create tables.

In all commands, you will need to replace "instance", "zookeepers",
"username", and "password" with the values you set for your Accumulo instance.

Commands intended to be run in bash are prefixed by '$'. These are always
assumed to be run the from the root of your Accumulo installation.

Commands intended to be run in the Accumulo shell are prefixed by '>'.

## Accumulo Examples

Each example below highlights a feature of Apache Accumulo.

| Accumulo Example | Description |
|------------------|-------------|
| [batch] | Using the batch writer and batch scanner |
| [bloom] | Creating a bloom filter enabled table to increase query performance |
| [bulkIngest] | Ingesting bulk data using map/reduce jobs on Hadoop |
| [classpath] | Using per-table classpaths |
| [client] | Using table operations, reading and writing data in Java. |
| [combiner] | Using example StatsCombiner to find min, max, sum, and count. |
| [compactionStrategy] | Configuring a compaction strategy |
| [constraints] | Using constraints with tables. |
| [dirlist] | Storing filesystem information. |
| [export] | Exporting and importing tables. |
| [filedata] | Storing file data. |
| [filter] | Using the AgeOffFilter to remove records more than 30 seconds old. |
| [helloworld] | Inserting records both inside map/reduce jobs and outside. And reading records between two rows. |
| [isolation] | Using the isolated scanner to ensure partial changes are not seen. |
| [mapred] | Using MapReduce to read from and write to Accumulo tables. |
| [maxmutation] | Limiting mutation size to avoid running out of memory. |
| [regex] | Using MapReduce and Accumulo to find data using regular expressions. |
| [reservations] | Using conditional mutations to implement simple reservation system. |
| [rgbalancer] | Using a balancer to spread groups of tablets within a table evenly |
| [rowhash] | Using MapReduce to read a table and write to a new column in the same table. |
| [sample] | Building and using sample data in Accumulo. |
| [shard] | Using the intersecting iterator with a term index partitioned by document. |
| [tabletofile] | Using MapReduce to read a table and write one of its columns to a file in HDFS. |
| [terasort] | Generating random data and sorting it using Accumulo. |
| [visibility] | Using visibilities (or combinations of authorizations). Also shows user permissions. |

[manual]: https://accumulo.apache.org/latest/accumulo_user_manual/
[batch]: batch.md
[bloom]: bloom.md
[bulkIngest]: bulkIngest.md
[classpath]: classpath.md
[client]: client.md 
[combiner]: combiner.md
[compactionStrategy]: compactionStrategy.md
[constraints]: constraints.md
[dirlist]: dirlist.md
[export]: export.md
[filedata]: filedata.md
[filter]: filter.md
[helloworld]: helloworld.md
[isolation]: isolation.md
[mapred]: mapred.md
[maxmutation]: maxmutation.md
[regex]: regex.md
[reservations]: reservations.md
[rgbalancer]: rgbalancer.md
[rowhash]: rowhash.md
[sample]: sample.md
[shard]: shard.md
[tabletofile]: tabletofile.md
[terasort]: terasort.md
[visibility]: visibility.md
