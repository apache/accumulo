---
title: "User Manual: Administration"
---

** Next:** [Shell Commands][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Security][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [Hardware][9]
* [Network][10]
* [Installation][11]
* [Dependencies][12]
* [Configuration][13]
* [Initialization][14]
* [Running][15]
* [Monitoring][16]
* [Logging][17]
* [Recovery][18]

* * *

## <a id="Administration"></a> Administration

## <a id="Hardware"></a> Hardware

Because we are running essentially two or three systems simultaneously layered across the cluster: HDFS, Accumulo and MapReduce, it is typical for hardware to consist of 4 to 8 cores, and 8 to 32 GB RAM. This is so each running process can have at least one core and 2 - 4 GB each. 

One core running HDFS can typically keep 2 to 4 disks busy, so each machine may typically have as little as 2 x 300GB disks and as much as 4 x 1TB or 2TB disks. 

It is possible to do with less than this, such as with 1u servers with 2 cores and 4GB each, but in this case it is recommended to only run up to two processes per machine - i.e. DataNode and TabletServer or DataNode and MapReduce worker but not all three. The constraint here is having enough available heap space for all the processes on a machine. 

## <a id="Network"></a> Network

Accumulo communicates via remote procedure calls over TCP/IP for both passing data and control messages. In addition, Accumulo uses HDFS clients to communicate with HDFS. To achieve good ingest and query performance, sufficient network bandwidth must be available between any two machines. 

## <a id="Installation"></a> Installation

Choose a directory for the Accumulo installation. This directory will be referenced by the environment variable $ACCUMULO_HOME. Run the following: 
    
    
    $ tar xzf $ACCUMULO_HOME/accumulo.tar.gz
    

Repeat this step at each machine within the cluster. Usually all machines have the same $ACCUMULO_HOME. 

## <a id="Dependencies"></a> Dependencies

Accumulo requires HDFS and ZooKeeper to be configured and running before starting. Password-less SSH should be configured between at least the Accumulo master and TabletServer machines. It is also a good idea to run Network Time Protocol (NTP) within the cluster to ensure nodes' clocks don't get too out of sync, which can cause problems with automatically timestamped data. Accumulo will remove from the set of TabletServers those machines whose times differ too much from the master's. 

## <a id="Configuration"></a> Configuration

Accumulo is configured by editing several Shell and XML files found in $ACCUMULO_HOME/conf. The structure closely resembles Hadoop's configuration files. 

### <a id="Edit_conf/accumulo-env.sh"></a> Edit conf/accumulo-env.sh

Accumulo needs to know where to find the software it depends on. Edit accumulo-env.sh and specify the following: 

1. Enter the location of the installation directory of Accumulo for $ACCUMULO_HOME
2. Enter your system's Java home for $JAVA_HOME
3. Enter the location of Hadoop for $HADOOP_HOME
4. Choose a location for Accumulo logs and enter it for $ACCUMULO_LOG_DIR
5. Enter the location of ZooKeeper for $ZOOKEEPER_HOME

By default Accumulo TabletServers are set to use 1GB of memory. You may change this by altering the value of $ACCUMULO_TSERVER_OPTS. Note the syntax is that of the Java JVM command line options. This value should be less than the physical memory of the machines running TabletServers. 

There are similar options for the master's memory usage and the garbage collector process. Reduce these if they exceed the physical RAM of your hardware and increase them, within the bounds of the physical RAM, if a process fails because of insufficient memory. 

Note that you will be specifying the Java heap space in accumulo-env.sh. You should make sure that the total heap space used for the Accumulo tserver and the Hadoop DataNode and TaskTracker is less than the available memory on each slave node in the cluster. On large clusters, it is recommended that the Accumulo master, Hadoop NameNode, secondary NameNode, and Hadoop JobTracker all be run on separate machines to allow them to use more heap space. If you are running these on the same machine on a small cluster, likewise make sure their heap space settings fit within the available memory. 

### <a id="Cluster_Specification"></a> Cluster Specification

On the machine that will serve as the Accumulo master: 

1. Write the IP address or domain name of the Accumulo Master to the   
$ACCUMULO_HOME/conf/masters file. 
2. Write the IP addresses or domain name of the machines that will be TabletServers in   
$ACCUMULO_HOME/conf/slaves, one per line. 

Note that if using domain names rather than IP addresses, DNS must be configured properly for all machines participating in the cluster. DNS can be a confusing source of errors. 

### <a id="Accumulo_Settings"></a> Accumulo Settings

Specify appropriate values for the following settings in   
$ACCUMULO_HOME/conf/accumulo-site.xml : 
    
    
    <property>
        <name>zookeeper</name>
        <value>zooserver-one:2181,zooserver-two:2181</value>
        <description>list of zookeeper servers</description>
    </property>
    <property>
        <name>walog</name>
        <value>/var/accumulo/walogs</value>
        <description>local directory for write ahead logs</description>
    </property>
    

This enables Accumulo to find ZooKeeper. Accumulo uses ZooKeeper to coordinate settings between processes and helps finalize TabletServer failure. 

Accumulo records all changes to tables to a write-ahead log before committing them to the table. The `walog' setting specifies the local directory on each machine to which write-ahead logs are written. This directory should exist on all machines acting as TabletServers. 

Some settings can be modified via the Accumulo shell and take effect immediately. However, any settings that should be persisted across system restarts must be recorded in the accumulo-site.xml file. 

### <a id="Deploy_Configuration"></a> Deploy Configuration

Copy the masters, slaves, accumulo-env.sh, and if necessary, accumulo-site.xml from the   
$ACCUMULO_HOME/conf/ directory on the master to all the machines specified in the slaves file. 

## <a id="Initialization"></a> Initialization

Accumulo must be initialized to create the structures it uses internally to locate data across the cluster. HDFS is required to be configured and running before Accumulo can be initialized. 

Once HDFS is started, initialization can be performed by executing   
$ACCUMULO_HOME/bin/accumulo init . This script will prompt for a name for this instance of Accumulo. The instance name is used to identify a set of tables and instance-specific settings. The script will then write some information into HDFS so Accumulo can start properly. 

The initialization script will prompt you to set a root password. Once Accumulo is initialized it can be started. 

## <a id="Running"></a> Running

### <a id="Starting_Accumulo"></a> Starting Accumulo

Make sure Hadoop is configured on all of the machines in the cluster, including access to a shared HDFS instance. Make sure HDFS and ZooKeeper are running. Make sure ZooKeeper is configured and running on at least one machine in the cluster. Start Accumulo using the bin/start-all.sh script. 

To verify that Accumulo is running, check the Status page as described under *Monitoring*. In addition, the Shell can provide some information about the status of tables via reading the !METADATA table. 

### <a id="Stopping_Accumulo"></a> Stopping Accumulo

To shutdown cleanly, run bin/stop-all.sh and the master will orchestrate the shutdown of all the tablet servers. Shutdown waits for all minor compactions to finish, so it may take some time for particular configurations. 

### <a id="Adding_a_Node"></a> Adding a Node

Update your $ACCUMULO_HOME/conf/slaves (or $ACCUMULO_CONF_DIR/slaves) file to account for the addition. 
    
    
    $ACCUMULO_HOME/bin/accumulo admin start <host(s)> {<host> ...}
    

Alternatively, you can ssh to each of the hosts you want to add and run $ACCUMULO_HOME/bin/start-here.sh. 

Make sure the host in question has the new configuration, or else the tablet server won't start; at a minimum this needs to be on the host(s) being added, but in practice it's good to ensure consistent configuration across all nodes. 

### <a id="Decomissioning_a_Node"></a> Decomissioning a Node

If you need to take a node out of operation, you can trigger a graceful shutdown of a tablet server. Accumulo will automatically rebalance the tablets across the available tablet servers. 
    
    
    $ACCUMULO_HOME/bin/accumulo admin stop <host(s)> {<host> ...}
    

Alternatively, you can ssh to each of the hosts you want to remove and run $ACCUMULO_HOME/bin/stop-here.sh. 

Be sure to update your $ACCUMULO_HOME/conf/slaves (or $ACCUMULO_CONF_DIR/slaves) file to account for the removal of these hosts. Bear in mind that the monitor will not re-read the slaves file automatically, so it will report the decomissioned servers as down; it's recommended that you restart the monitor so that the node list is up to date. 

## <a id="Monitoring"></a> Monitoring

The Accumulo Master provides an interface for monitoring the status and health of Accumulo components. This interface can be accessed by pointing a web browser to   
http://accumulomaster:50095/status

## <a id="Logging"></a> Logging

Accumulo processes each write to a set of log files. By default these are found under   
$ACCUMULO/logs/. 

## <a id="Recovery"></a> Recovery

In the event of TabletServer failure or error on shutting Accumulo down, some mutations may not have been minor compacted to HDFS properly. In this case, Accumulo will automatically reapply such mutations from the write-ahead log either when the tablets from the failed server are reassigned by the Master, in the case of a single TabletServer failure or the next time Accumulo starts, in the event of failure during shutdown. 

Recovery is performed by asking the loggers to copy their write-ahead logs into HDFS. As the logs are copied, they are also sorted, so that tablets can easily find their missing updates. The copy/sort status of each file is displayed on Accumulo monitor status page. Once the recovery is complete any tablets involved should return to an ``online" state. Until then those tablets will be unavailable to clients. 

The Accumulo client library is configured to retry failed mutations and in many cases clients will be able to continue processing after the recovery process without throwing an exception. 

Note that because Accumulo uses timestamps to order mutations, any mutations that are applied as part of the recovery process should appear to have been applied when they originally arrived at the TabletServer that failed. This makes the ordering of mutations consistent in the presence of failure. 

* * *

** Next:** [Shell Commands][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [Security][6]   ** [Contents][8]**

[2]: Shell_Commands.html
[4]: accumulo_user_manual.html
[6]: Security.html
[8]: Contents.html
[9]: Administration.html#Hardware
[10]: Administration.html#Network
[11]: Administration.html#Installation
[12]: Administration.html#Dependencies
[13]: Administration.html#Configuration
[14]: Administration.html#Initialization
[15]: Administration.html#Running
[16]: Administration.html#Monitoring
[17]: Administration.html#Logging
[18]: Administration.html#Recovery

