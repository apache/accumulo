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

Upgrading
=========

# From 1.6 to 1.7

Upgrades from 1.6 to 1.7 are be possible with little effort as no changes were made at the data layer and RPC changes
were made in a backwards-compatible way. The recommended way is to stop Accumulo 1.6, perform the Accumulo upgrade to
1.7, and then start 1.7. Like previous versions, after 1.7.0 is started on a 1.6 instance, a one-time upgrade will
happen by the Master which will prevent a downgrade back to 1.6. Upgrades are still one way. Upgrades from versions
prior to 1.6 to 1.7 should follow the below path to 1.6 and then perform the upgrade to 1.7 -- direct upgrades to 1.7
for versions other than 1.6 are untested.

After upgrading to 1.7.0, users will notice the addition of a `replication` table in the `accumulo` namespace. This
table is created and put offline to avoid any additional maintainence if the data-center replication feature is not
in use.

Existing configuration files from 1.6 should be compared against the examples provided in 1.7. The 1.6 configuration
files should all function with 1.7 code, but you will likely want to include a new file (hadoop-metrics2-accumulo.properties)
to enable the new metrics subsystem. Read the section on Hadoop Metrics2 in the Administration chapter of the Accumulo User Manual.

For each of the other new features, new configuration properties exist to support the feature. Refer to the added
sections in the User Manual for the feature for information on how to properly configure and use the new functionality.

# From 1.5 to 1.6

This happens automatically the first time Accumulo 1.6 is started. If your instance previously upgraded from 1.4 to 1.5,
you must verify that your 1.5 instance has no outstanding local write ahead logs. You can do this by ensuring either:

 * All of your tables are online and the Monitor shows all tablets hosted
 * The directory for write ahead logs (logger.dir.walog) from 1.4 has no files remaining on any tablet server / logger
 hosts

To upgrade from 1.5 to 1.6 you must:

 * Verify that there are no outstanding FATE operations
    * Under 1.5 you can list what's in FATE by running `$ACCUMULO_HOME/bin/accumulo
      org.apache.accumulo.server.fate.Admin print`
    * Note that operations in any state will prevent an upgrade. It is safe to delete operations with status SUCCESSFUL.
      For others, you should restart your 1.5 cluster and allow them to finish.
 * Stop the 1.5 instance.
 * Configure 1.6 to use the hdfs directory and zookeepers that 1.5 was using.
 * Copy other 1.5 configuration options as needed.
 * Start Accumulo 1.6.

The upgrade process must make changes to Accumulo's internal state in both
ZooKeeper and the table metadata. This process may take some time if Tablet
Servers have to go through recovery. During this time, the Monitor will claim
that the Master is down and some services may send the Monitor log messages
about failure to communicate with each other.  These messages are safe to
ignore. If you need detail on the upgrade's progress you should view the local
logs on the Tablet Servers and active Master.

### From 1.4 to 1.6

To upgrade from 1.4 to 1.6 you must perform a manual initial step.

Prior to upgrading you must:

 * Verify that there are no outstanding FATE operations
     * Under 1.4 you can list what's in FATE by running
       `$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.fate.Admin print`
     * Note that operations in any state will prevent an upgrade. It is safe
       to delete operations with status SUCCESSFUL. For others, you should restart
       your 1.4 cluster and allow them to finish.
 * Stop the 1.4 instance.
 * Configure 1.6 to use the hdfs directory, walog directories, and zookeepers
   that 1.4 was using.
 * Copy other 1.4 configuration options as needed.

Prior to starting the 1.6 instance you will need to run the LocalWALRecovery tool
on each node that previously ran an instance of the Logger role.

    $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.tserver.log.LocalWALRecovery

The recovery tool will rewrite the 1.4 write ahead logs into a format that 1.6 can read.
After this step has completed on all nodes, start the 1.6 cluster to continue the upgrade.

The upgrade process must make changes to Accumulo's internal state in both ZooKeeper and
the table metadata. This process may take some time if Tablet Servers have to go through
recovery. During this time, the Monitor will claim that the Master is down and some
services may send the Monitor log messages about failure to communicate with each other.
While the upgrade is in progress, the Garbage Collector may complain about invalid paths.
The Master may also complain about failure to create the trace table because it already
exists. These messages are safe to ignore. If other error messages occur, you should seek
out support before continuing to use Accumulo. If you need detail on the upgrade's progress
you should view the local logs on the Tablet Servers and active Master.

Note that the LocalWALRecovery tool does not delete the local files. Once you confirm that
1.6 is successfully running, you should delete these files on the local filesystem.
