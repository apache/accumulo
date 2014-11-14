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

Continuous Query and Ingest
===========================

This directory contains a suite of scripts for placing continuous query and
ingest load on accumulo.  The purpose of these script is two-fold. First,
place continuous load on accumulo to see if breaks.  Second, collect
statistics in order to understand how accumulo behaves.  To run these scripts
copy all of the `.example` files and modify them.  You can put these scripts in
the current directory or define a `CONTINUOUS_CONF_DIR` where the files will be
read from. These scripts rely on `pssh`. Before running any script you may need
to use `pssh` to create the log directory on each machine (if you want it local).
Also, create the table "ci" before running. You can run
`org.apache.accumulo.test.continuous.GenSplits` to generate splits points for a
continuous ingest table.

The following ingest scripts insert data into accumulo that will form a random
graph.

> $ start-ingest.sh  
> $ stop-ingest.sh

The following query scripts randomly walk the graph created by the ingesters.
Each walker produce detailed statistics on query/scan times.

> $ start-walkers.sh  
> $ stop-walker.sh

The following scripts start and stop batch walkers.

> $ start-batchwalkers.sh  
> $ stop-batchwalkers.sh

In addition to placing continuous load, the following scripts start and stop a
service that continually collect statistics about accumulo and HDFS.

> $ start-stats.sh  
> $ stop-stats.sh

Optionally, start the agitator to periodically kill the tabletserver and/or datanode
process(es) on random nodes. You can run this script as root and it will properly start
processes as the user you configured in `continuous-env.sh` (`HDFS_USER` for the Datanode and
`ACCUMULO_USER` for Accumulo processes). If you run it as yourself and the `HDFS_USER` and
`ACCUMULO_USER` values are the same as your user, the agitator will not change users. In
the case where you run the agitator as a non-privileged user which isn't the same as `HDFS_USER`
or `ACCUMULO_USER`, the agitator will attempt to `sudo` to these users, which relies on correct
configuration of sudo. Also, be sure that your `HDFS_USER` has password-less `ssh` configured.

> $ start-agitator.sh  
> $ stop-agitator.sh

Start all three of these services and let them run for a few hours. Then run
`report.pl` to generate a simple HTML report containing plots and histograms
showing what has transpired.

A MapReduce job to verify all data created by continuous ingest can be run
with the following command.  Before running the command modify the `VERIFY_*`
variables in `continuous-env.sh` if needed.  Do not run ingest while running this
command, this will cause erroneous reporting of UNDEFINED nodes. The MapReduce
job will scan a reference after it has scanned the definition.

> $ run-verify.sh

Each entry, except for the first batch of entries, inserted by continuous
ingest references a previously flushed entry.  Since we are referencing flushed
entries, they should always exist.  The MapReduce job checks that all
referenced entries exist.  If it finds any that do not exist it will increment
the UNDEFINED counter and emit the referenced but undefined node.  The MapReduce
job produces two other counts : REFERENCED and UNREFERENCED.  It is
expected that these two counts are non zero.  REFERENCED counts nodes that are
defined and referenced.  UNREFERENCED counts nodes that defined and
unreferenced, these are the latest nodes inserted.

To stress accumulo, run the following script which starts a MapReduce job
that reads and writes to your continuous ingest table.  This MapReduce job
will write out an entry for every entry in the table (except for ones created
by the MapReduce job itself). Stop ingest before running this MapReduce job.
Do not run more than one instance of this MapReduce job concurrently against a
table.

> $ run-moru.sh

