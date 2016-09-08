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
Benchmark Tests
===============

Running the Benchmarks
----------------------

Syntax for running run.py:

> `$ ./run.py [-l -v <log_level> -s <run_speed> -u <user> -p <password> -i <instance>] [Benchmark1 ... BenchmarkN]`

Specifying a specific benchmark or set of benchmarks runs only those, while
not specifying any runs all benchmarks.

`-l` lists the benchmarks that will be run  
`-v <run_speed>` can either be slow, medium or fast  
`-s <log_level>` is a number representing the verbosity of the debugging output: 10 is debug, 20 is info, 30 is warning, etc.  
`-u <user>` user to use when connecting with accumulo.  If not set you will be prompted to input it.  
`-p <password>` password to use when connecting with accumulo.  If not set you will be prompted to input it.  
`-z <zookeepers>` comma delimited lit of zookeeper host:port pairs to use when connecting with accumulo.  If not set you will be prompted to input it.  
`-i <instance>` instance to use when connecting with accumulo.  If not set you will be prompted to input it.  

The Benchmarks
--------------

Values in a 3-tuple are the slow,medium,fast speeds at which you can run the benchmarks.

* CloudStone1: Test the speed at which we can check that accumulo is up and we can reach all the tservers. Lower is better.  
* CloudStone2: Ingest 10000,100000,1000000 rows of values 50 bytes on every tserver.  Higher is better.  
* CloudStone3: Ingest 1000,5000,10000 rows of values 1024,8192,65535 bytes on every tserver.  Higher is better.  
* CloudStone4 (TeraSort): Ingests 10000,10000000,10000000000 rows. Lower score is better.  
* CloudStone5: Creates 100,500,1000 tables named TestTableX and then deletes them. Lower is better.  
* CloudStone6: Creates a table with 400, 800, 1000 splits.  Lower is better.  

Terasort
--------

The 4th Benchmark is Terasort.  Run the benchmarks with speed 'slow' to do a full terasort.

Misc
----

These benchmarks create tables in accumulo named 'test_ingest' and 'CloudIngestTest'.  These tables are deleted
at the end of the benchmarks. The benchmarks will also alter user auths while it runs. It is recommended that
a benchmark user is created.

