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
title: Apache Accumulo Customizing the Compaction Strategy
---

This tutorial uses the following Java classes, which can be found in org.apache.accumulo.tserver.compaction: 

 * DefaultCompactionStrategy.java - determines which files to compact based on table.compaction.major.ratio and table.file.max
 * EverythingCompactionStrategy.java - compacts all files
 * SizeLimitCompactionStrategy.java - compacts files no bigger than table.majc.compaction.strategy.opts.sizeLimit
 * TwoTierCompactionStrategy.java - uses default compression for smaller files and table.majc.compaction.strategy.opts.file.large.compress.type for larger files

This is an example of how to configure a compaction strategy. By default Accumulo will always use the DefaultCompactionStrategy, unless 
these steps are taken to change the configuration.  Use the strategy and settings that best fits your Accumulo setup. This example shows
how to configure and test one of the more complicated strategies, the TwoTierCompactionStrategy. Note that this example requires hadoop
native libraries built with snappy in order to use snappy compression.

To begin, run the command to create a table for testing:

    $ ./bin/accumulo shell -u root -p secret -e "createtable test1"

The command below sets the compression for smaller files and minor compactions for that table.

    $ ./bin/accumulo shell -u root -p secret -e "config -s table.file.compress.type=snappy -t test1"

The commands below will configure the TwoTierCompactionStrategy to use gz compression for files larger than 1M. 

    $ ./bin/accumulo shell -u root -p secret -e "config -s table.majc.compaction.strategy.opts.file.large.compress.threshold=1M -t test1"
    $ ./bin/accumulo shell -u root -p secret -e "config -s table.majc.compaction.strategy.opts.file.large.compress.type=gz -t test1"
    $ ./bin/accumulo shell -u root -p secret -e "config -s table.majc.compaction.strategy=org.apache.accumulo.tserver.compaction.TwoTierCompactionStrategy -t test1"

Generate some data and files in order to test the strategy:

    $ ./bin/accumulo org.apache.accumulo.examples.simple.client.SequentialBatchWriter -i instance17 -z localhost:2181 -u root -p secret -t test1 --start 0 --num 10000 --size 50 --batchMemory 20M --batchLatency 500 --batchThreads 20
    $ ./bin/accumulo shell -u root -p secret -e "flush -t test1"
    $ ./bin/accumulo org.apache.accumulo.examples.simple.client.SequentialBatchWriter -i instance17 -z localhost:2181 -u root -p secret -t test1 --start 0 --num 11000 --size 50 --batchMemory 20M --batchLatency 500 --batchThreads 20
    $ ./bin/accumulo shell -u root -p secret -e "flush -t test1"
    $ ./bin/accumulo org.apache.accumulo.examples.simple.client.SequentialBatchWriter -i instance17 -z localhost:2181 -u root -p secret -t test1 --start 0 --num 12000 --size 50 --batchMemory 20M --batchLatency 500 --batchThreads 20
    $ ./bin/accumulo shell -u root -p secret -e "flush -t test1"
    $ ./bin/accumulo org.apache.accumulo.examples.simple.client.SequentialBatchWriter -i instance17 -z localhost:2181 -u root -p secret -t test1 --start 0 --num 13000 --size 50 --batchMemory 20M --batchLatency 500 --batchThreads 20
    $ ./bin/accumulo shell -u root -p secret -e "flush -t test1"

View the tserver log in <accumulo_home>/logs for the compaction and find the name of the <rfile> that was compacted for your table. Print info about this file using the PrintInfo tool:

    $ ./bin/accumulo rfile-info <rfile>

Details about the rfile will be printed and the compression type should match the type used in the compaction...
Meta block     : RFile.index
      Raw size             : 512 bytes
      Compressed size      : 278 bytes
      Compression type     : gz

