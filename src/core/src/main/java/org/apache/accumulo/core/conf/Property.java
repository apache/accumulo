/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.conf;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;

public enum Property {
  // instance properties (must be the same for every node in an instance)
  INSTANCE_PREFIX("instance.", null, PropertyType.PREFIX,
      "Properties in this category must be consistent throughout a cloud. This is enforced and servers won't be able to communicate if these differ."),
  INSTANCE_ZK_HOST("instance.zookeeper.host", "localhost:2181", PropertyType.HOSTLIST, "Comma separated list of zookeeper servers"),
  INSTANCE_ZK_TIMEOUT("instance.zookeeper.timeout", "30s", PropertyType.TIMEDURATION,
      "Zookeeper session timeout; max value when represented as milliseconds should be no larger than " + Integer.MAX_VALUE),
  INSTANCE_DFS_URI("instance.dfs.uri", "", PropertyType.URI,
      "The url accumulo should use to connect to DFS.  If this is empty, accumulo will obtain this information from the hadoop configuration."),
  INSTANCE_DFS_DIR("instance.dfs.dir", "/accumulo", PropertyType.ABSOLUTEPATH,
      "HDFS directory in which accumulo instance will run.  Do not change after accumulo is initialized."),
  INSTANCE_SECRET("instance.secret", "DEFAULT", PropertyType.STRING,
      "A secret unique to a given instance that all servers must know in order to communicate with one another."
          + " Change it before initialization. To change it later use ./bin/accumulo accumulo.server.util.ChangeSecret [oldpasswd] [newpasswd], "
          + " and then update conf/accumulo-site.xml everywhere."),
  
  // general properties
  GENERAL_PREFIX("general.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of accumulo overall, but do not have to be consistent throughout a cloud."),
  GENERAL_CLASSPATHS(AccumuloClassLoader.CLASSPATH_PROPERTY_NAME, AccumuloClassLoader.DEFAULT_CLASSPATH_VALUE, PropertyType.STRING,
      "A list of all of the places to look for a class. Order does matter, as it will look for the jar "
          + "starting in the first location to the last. Please note, hadoop conf and hadoop lib directories NEED to be here, "
          + "along with accumulo lib and zookeeper directory. Supports full regex on filename alone."), // needs special treatment in accumulo start
                                                                                                        // jar
  GENERAL_DYNAMIC_CLASSPATHS(AccumuloClassLoader.DYNAMIC_CLASSPATH_PROPERTY_NAME, AccumuloClassLoader.DEFAULT_DYNAMIC_CLASSPATH_VALUE, PropertyType.STRING,
      "A list of all of the places where changes in jars or classes will force a reload of the classloader."),
  GENERAL_RPC_TIMEOUT("general.rpc.timeout", "120s", PropertyType.TIMEDURATION, "Time to wait on I/O for simple, short RPC calls"),
  
  // properties that are specific to master server behavior
  MASTER_PREFIX("master.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the master server"),
  MASTER_CLIENTPORT("master.port.client", "9999", PropertyType.PORT, "The port used for handling client connections on the master"),
  MASTER_TABLET_BALANCER("master.tablet.balancer", "org.apache.accumulo.server.master.balancer.DefaultLoadBalancer", PropertyType.CLASSNAME,
      "The balancer class that accumulo will use to make tablet assignment and migration decisions."),
  MASTER_LOGGER_BALANCER("master.logger.balancer", "org.apache.accumulo.server.master.balancer.SimpleLoggerBalancer", PropertyType.CLASSNAME,
      "The balancer class that accumulo will use to make logger assignment decisions."),
  MASTER_RECOVERY_REDUCERS("master.recovery.reducers", "10", PropertyType.COUNT, "Number of reducers to use to sort recovery logs (per log)"),
  MASTER_RECOVERY_MAXAGE("master.recovery.max.age", "60m", PropertyType.TIMEDURATION, "Recovery files older than this age will be removed."),
  MASTER_RECOVERY_MAXTIME("master.recovery.time.max", "30m", PropertyType.TIMEDURATION, "The maximum time to attempt recovery before giving up"),
  MASTER_RECOVERY_QUEUE("master.recovery.queue", "default", PropertyType.STRING, "Priority queue to use for log recovery map/reduce jobs."),
  MASTER_RECOVERY_POOL("master.recovery.pool", "recovery", PropertyType.STRING, "Priority queue to use for log recovery map/reduce jobs."),
  MASTER_RECOVERY_SORT_MAPREDUCE("master.recovery.sort.mapreduce", "false", PropertyType.BOOLEAN,
      "If true, use map/reduce to sort write-ahead logs during recovery"),
  MASTER_BULK_SERVERS("master.bulk.server.max", "4", PropertyType.COUNT, "The number of servers to use during a bulk load"),
  MASTER_BULK_RETRIES("master.bulk.retries", "3", PropertyType.COUNT, "The number of attempts to bulk-load a file before giving up."),
  MASTER_BULK_THREADPOOL_SIZE("master.bulk.threadpool.size", "5", PropertyType.COUNT, "The number of threads to use when coordinating a bulk-import."),
  MASTER_MINTHREADS("master.server.threads.minimum", "2", PropertyType.COUNT, "The minimum number of threads to use to handle incoming requests."),
  MASTER_THREADCHECK("master.server.threadcheck.time", "1s", PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool."),
  
  // properties that are specific to tablet server behavior
  TSERV_PREFIX("tserver.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the tablet servers"),
  TSERV_CLIENT_TIMEOUT("tserver.client.timeout", "3s", PropertyType.TIMEDURATION, "Time to wait for clients to continue scans before closing a session."),
  TSERV_DEFAULT_BLOCKSIZE("tserver.default.blocksize", "1M", PropertyType.MEMORY, "Specifies a default blocksize for the tserver caches"),
  TSERV_DATACACHE_SIZE("tserver.cache.data.size", "100M", PropertyType.MEMORY, "Specifies the size of the cache for file data blocks."),
  TSERV_INDEXCACHE_SIZE("tserver.cache.index.size", "512M", PropertyType.MEMORY, "Specifies the size of the cache for file indices."),
  TSERV_PORTSEARCH("tserver.port.search", "false", PropertyType.BOOLEAN, "if the ports above are in use, search higher ports until one is available"),
  TSERV_CLIENTPORT("tserver.port.client", "9997", PropertyType.PORT, "The port used for handling client connections on the tablet servers"),
  TSERV_MUTATION_QUEUE_MAX("tserver.mutation.queue.max", "256K", PropertyType.MEMORY,
      "The amount of memory to use to store write-ahead-log mutations-per-session before flushing them."),
  TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN("tserver.tablet.split.midpoint.files.max", "30", PropertyType.COUNT,
      "To find a tablets split points, all index files are opened. This setting determines how many index "
          + "files can be opened at once. When there are more index files than this setting multiple passes "
          + "must be made, which is slower. However opening too many files at once can cause problems."),
  TSERV_WALOG_MAX_SIZE("tserver.walog.max.size", "1G", PropertyType.MEMORY, "The maximum size for each write-ahead log"),
  TSERV_MAJC_DELAY("tserver.compaction.major.delay", "30s", PropertyType.TIMEDURATION,
      "Time a tablet server will sleep between checking which tablets need compaction."),
  TSERV_MAJC_THREAD_MAXOPEN("tserver.compaction.major.thread.files.open.max", "10", PropertyType.COUNT,
      "Max number of files a major compaction thread can open at once. "),
  TSERV_SCAN_MAX_OPENFILES("tserver.scan.files.open.max", "100", PropertyType.COUNT,
      "Maximum total map files that all tablets in a tablet server can open for scans. "),
  TSERV_MAX_IDLE("tserver.files.open.idle", "1m", PropertyType.TIMEDURATION, "Tablet servers leave previously used map files open for future queries. "
      + "This setting determines how much time an unused map file should be kept open until it is closed."),
  TSERV_NATIVEMAP_ENABLED("tserver.memory.maps.native.enabled", "true", PropertyType.BOOLEAN,
      "An in-memory data store for accumulo implemented in c++ that increases the amount of data " + "accumulo can hold in memory and avoids Java GC pauses."),
  TSERV_MAXMEM("tserver.memory.maps.max", "1G", PropertyType.MEMORY, "Maximum amount of memory all tablets in memory maps can use."),
  TSERV_MEM_MGMT("tserver.memory.manager", "org.apache.accumulo.server.tabletserver.LargestFirstMemoryManager", PropertyType.CLASSNAME,
      "An implementation of MemoryManger that accumulo will use."),
  TSERV_SESSION_MAXIDLE("tserver.session.idle.max", "1m", PropertyType.TIMEDURATION, "maximum idle time for a session"),
  TSERV_READ_AHEAD_MAXCONCURRENT("tserver.readahead.concurrent.max", "16", PropertyType.COUNT,
      "The maximum number of concurrent read ahead that will execute.  This effectively"
          + " limits the number of long running scans that can run concurrently per tserver."),
  TSERV_METADATA_READ_AHEAD_MAXCONCURRENT("tserver.metadata.readahead.concurrent.max", "8", PropertyType.COUNT,
      "The maximum number of concurrent metadata read ahead that will execute."),
  TSERV_MIGRATE_MAXCONCURRENT("tserver.migrations.concurrent.max", "1", PropertyType.COUNT,
      "The maximum number of concurrent tablet migrations for a tablet server"),
  TSERV_MAJC_MAXCONCURRENT("tserver.compaction.major.concurrent.max", "3", PropertyType.COUNT,
      "The maximum number of concurrent major compactions for a tablet server"),
  TSERV_MINC_MAXCONCURRENT("tserver.compaction.minor.concurrent.max", "4", PropertyType.COUNT,
      "The maximum number of concurrent minor compactions for a tablet server"),
  TSERV_BLOOM_LOAD_MAXCONCURRENT("tserver.bloom.load.concurrent.max", "4", PropertyType.COUNT,
      "The number of concurrent threads that will load bloom filters in the background. "
          + "Setting this to zero will make bloom filters load in the foreground."),
  TSERV_LOGGER_TIMEOUT("tserver.logger.timeout", "30s", PropertyType.TIMEDURATION, "The time to wait for a logger to respond to a write-ahead request"),
  TSERV_LOGGER_COUNT("tserver.logger.count", "2", PropertyType.COUNT, "The number of loggers that each tablet server should use."),
  TSERV_LOGGER_STRATEGY("tserver.logger.strategy", "org.apache.accumulo.server.tabletserver.log.RoundRobinLoggerStrategy", PropertyType.STRING,
      "The classname used to decide which loggers to use."),
  TSERV_MONITOR_FS(
      "tserver.monitor.fs",
      "true",
      PropertyType.BOOLEAN,
      "When enabled the tserver will monitor file systems and kill itself when one switches from rw to ro.  This is usually and indication that Linux has detected a bad disk."),
  TSERV_MEMDUMP_DIR(
      "tserver.dir.memdump",
      "/tmp",
      PropertyType.PATH,
      "A long running scan could possibly hold memory that has been minor compacted.  To prevent this, the in memory map is dumped to a local file and the scan is switched to that local file.  We can not switch to the minor compacted file because it may have been modified by iterators.  The file dumped to the local dir is an exact copy of what was in memory."),
  TSERV_LOCK_MEMORY("tserver.memory.lock", "false", PropertyType.BOOLEAN,
      "The tablet server must communicate with zookeeper frequently to maintain its locks.  If the tablet server's memory is swapped out"
          + " the java garbage collector can stop all processing for long periods.  Change this property to true and the tablet server will "
          + " attempt to lock all of its memory to RAM, which may reduce delays during java garbage collection.  You will have to modify the "
          + " system limit for \"max locked memory\". This feature is only available when running on Linux.  Alternatively you may also "
          + " want to set /proc/sys/vm/swappiness to zero (again, this is Linux-specific)."),
  TSERV_BULK_PROCESS_THREADS("tserver.bulk.process.threads", "1", PropertyType.COUNT,
      "The master will task a tablet server with pre-processing a bulk file prior to assigning it to the appropriate tablet servers.  This configuration"
          + " value controls the number of threads used to process the files."),
  TSERV_BULK_ASSIGNMENT_THREADS("tserver.bulk.assign.threads", "1", PropertyType.COUNT,
      "The master delegates bulk file processing and assignment to tablet servers. After the bulk file has been processed, the tablet server will assign"
          + " the file to the appropriate tablets on all servers.  This property controls the number of threads used to communicate to the other servers."),
  TSERV_BULK_RETRY("tserver.bulk.retry.max", "3", PropertyType.COUNT,
      "The number of times the tablet server will attempt to assign a file to a tablet as it migrates and splits."),
  TSERV_MINTHREADS("tserver.server.threads.minimum", "2", PropertyType.COUNT, "The minimum number of threads to use to handle incoming requests."),
  TSERV_THREADCHECK("tserver.server.threadcheck.time", "1s", PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool."),
  TSERV_HOLD_TIME_SUICIDE("tserver.hold.time.max", "5m", PropertyType.TIMEDURATION,
      "The maximum time for a tablet server to be in the \"memory full\" state.  If the tablet server cannot write out memory"
          + " in this much time, it will assume there is some failure local to its node, and quit.  A value of zero is equivalent to forever."),
  
  // properties that are specific to logger server behavior
  LOGGER_PREFIX("logger.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the write-ahead logger servers"),
  LOGGER_PORT("logger.port.client", "11224", PropertyType.PORT, "The port used for write-ahead logger services"),
  LOGGER_COPY_THREADPOOL_SIZE("logger.copy.threadpool.size", "2", PropertyType.COUNT,
      "size of the thread pool used to copy files from the local log area to HDFS"),
  LOGGER_DIR("logger.dir.walog", "walogs", PropertyType.PATH,
      "The directory used to store write-ahead logs on the local filesystem. It is possible to specify a comma-separated list of directories."),
  LOGGER_PORTSEARCH("logger.port.search", "false", PropertyType.BOOLEAN, "if the port above is in use, search higher ports until one is available"),
  LOGGER_ARCHIVE("logger.archive", "false", PropertyType.BOOLEAN, "determines if logs are archived in hdfs"),
  LOGGER_ARCHIVE_REPLICATION("logger.archive.replication", "0", PropertyType.COUNT,
      "determines the replication factor for walogs archived in hdfs, set to zero to use default"),
  LOGGER_MONITOR_FS(
      "logger.monitor.fs",
      "true",
      PropertyType.BOOLEAN,
      "When enabled the logger will monitor file systems and kill itself when one switches from rw to ro.  This is usually and indication that Linux has detected a bad disk."),
  LOGGER_SORT_BUFFER_SIZE("logger.sort.buffer.size", "200M", PropertyType.MEMORY,
      "The amount of memory to use when sorting logs during recovery. Only used when *not* sorting logs with map/reduce."),
  LOGGER_RECOVERY_FILE_REPLICATION("logger.recovery.file.replication", "1", PropertyType.COUNT,
      "When a logger puts a WALOG into HDFS, it will use this as the replication factor."),
  LOGGER_MINTHREADS("logger.server.threads.minimum", "2", PropertyType.COUNT, "The miniumum number of threads to use to handle incoming requests."),
  LOGGER_THREADCHECK("logger.server.threadcheck.time", "1s", PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool."),
  
  // accumulo garbage collector properties
  GC_PREFIX("gc.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the accumulo garbage collector."),
  GC_CYCLE_START("gc.cycle.start", "30s", PropertyType.TIMEDURATION, "Time to wait before attempting to garbage collect any old files."),
  GC_CYCLE_DELAY("gc.cycle.delay", "5m", PropertyType.TIMEDURATION, "Time between garbage collection cycles. In each cycle, old files "
      + "no longer in use are removed from the filesystem."),
  GC_PORT("gc.port.client", "50091", PropertyType.PORT, "The listening port for the garbage collector's monitor service"),
  GC_DELETE_THREADS("gc.threads.delete", "16", PropertyType.COUNT, "The number of threads used to delete files"),
  
  // properties that are specific to the monitor server behavior
  MONITOR_PREFIX("monitor.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the monitor web server."),
  MONITOR_PORT("monitor.port.client", "50095", PropertyType.PORT, "The listening port for the monitor's http service"),
  MONITOR_LOG4J_PORT("monitor.port.log4j", "4560", PropertyType.PORT, "The listening port for the monitor's log4j logging collection."),
  MONITOR_BANNER_TEXT("monitor.banner.text", "", PropertyType.STRING, "The banner text displayed on the monitor page."),
  MONITOR_BANNER_COLOR("monitor.banner.color", "#c4c4c4", PropertyType.STRING, "The color of the banner text displayed on the monitor page."),
  MONITOR_BANNER_BACKGROUND("monitor.banner.background", "#304065", PropertyType.STRING,
      "The background color of the banner text displayed on the monitor page."),
  
  TRACE_PREFIX("trace.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of distributed tracing."),
  TRACE_PORT("trace.port.client", "12234", PropertyType.PORT, "The listening port for the trace server"),
  TRACE_TABLE("trace.table", "trace", PropertyType.STRING, "The name of the table to store distributed traces"),
  TRACE_USER("trace.user", "root", PropertyType.STRING, "The name of the user to store distributed traces"),
  TRACE_PASSWORD("trace.password", "secret", PropertyType.STRING, "The password for the user used to store distributed traces"),
  
  // per table properties
  TABLE_PREFIX("table.", null, PropertyType.PREFIX, "Properties in this category affect tablet server treatment of tablets, but can be configured "
      + "on a per-table basis. Setting these properties in the site file will override the default globally "
      + "for all tables and not any specific table. However, both the default and the global setting can be "
      + "overridden per table using the table operations API or in the shell, which sets the overridden value "
      + "in zookeeper. Restarting accumulo tablet servers after setting these properties in the site file "
      + "will cause the global setting to take effect. However, you must use the API or the shell to change "
      + "properties in zookeeper that are set on a table."),
  TABLE_MAJC_RATIO(
      "table.compaction.major.ratio",
      "3",
      PropertyType.FRACTION,
      "minimum ratio of total input size to maximum input file size for running a major compaction.   When adjusting this property you may want to also adjust table.file.max.  Want to avoid the situation where only merging minor compactions occur."),
  TABLE_MAJC_COMPACTALL_IDLETIME("table.compaction.major.everything.idle", "1h", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have all "
          + "of its map file compacted into one.  There is no guarantee an idle tablet will be compacted. "
          + "Compactions of idle tablets are only started when regular compactions are not running. Idle "
          + "compactions only take place for tablets that have one or more map files."),
  TABLE_SPLIT_THRESHOLD("table.split.threshold", "1G", PropertyType.MEMORY, "When combined size of files exceeds this amount a tablet is split."),
  TABLE_MINC_LOGS_MAX("table.compaction.minor.logs.threshold", "3", PropertyType.COUNT,
      "When there are more than this many write-ahead logs against a tablet, it will be minor compacted."),
  TABLE_MINC_COMPACT_IDLETIME("table.compaction.minor.idle", "5m", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have its "
          + "in-memory map flushed to disk in a minor compaction.  There is no guarantee an idle " + "tablet will be compacted."),
  TABLE_SCAN_MAXMEM("table.scan.max.memory", "1M", PropertyType.MEMORY,
      "The maximum amount of memory that will be used to cache results of a client query/scan. "
          + "Once this limit is reached, the buffered data is sent to the client."),
  TABLE_FILE_TYPE("table.file.type", RFile.EXTENSION, PropertyType.STRING, "Change the type of file a table writes"),
  TABLE_LOAD_BALANCER("table.balancer", "org.apache.accumulo.server.master.balancer.DefaultLoadBalancer", PropertyType.STRING,
      "This property can be set to allow the LoadBalanceByTable load balancer to change the called Load Balancer for this table"),
  TABLE_FILE_COMPRESSION_TYPE("table.file.compress.type", "gz", PropertyType.STRING, "One of gz,lzo,none"),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE("table.file.compress.blocksize", "100K", PropertyType.MEMORY,
      "Overrides the hadoop io.seqfile.compress.blocksize setting so that map files have better query performance. " + "The maximum value for this is "
          + Integer.MAX_VALUE),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX("table.file.compress.blocksize.index", "128K", PropertyType.MEMORY,
      "Determines how large index blocks can be in files that support multilevel indexes. The maximum value for this is " + Integer.MAX_VALUE),
  TABLE_FILE_BLOCK_SIZE("table.file.blocksize", "0B", PropertyType.MEMORY,
      "Overrides the hadoop dfs.block.size setting so that map files have better query performance. " + "The maximum value for this is " + Integer.MAX_VALUE),
  TABLE_FILE_REPLICATION("table.file.replication", "0", PropertyType.COUNT, "Determines how many replicas to keep of a tables map files in HDFS. "
      + "When this value is LTE 0, HDFS defaults are used."),
  TABLE_FILE_MAX(
      "table.file.max",
      "15",
      PropertyType.COUNT,
      "Determines the max # of files each tablet in a table can have. When adjusting this property you may want to consider adjusting table.compaction.major.ratio also.  Setting this property to 0 will make it default to tserver.scan.files.open.max-1, this will prevent a tablet from having more files than can be opened.  Setting this property low may throttle ingest and increase query performance."),
  TABLE_WALOG_ENABLED("table.walog.enabled", "true", PropertyType.BOOLEAN, "Use the write-ahead log to prevent the loss of data."),
  TABLE_BLOOM_ENABLED("table.bloom.enabled", "false", PropertyType.BOOLEAN, "Use bloom filters on this table."),
  TABLE_BLOOM_LOAD_THRESHOLD("table.bloom.load.threshold", "1", PropertyType.COUNT,
      "This number of seeks that would actually use a bloom filter must occur before a "
          + "map files bloom filter is loaded. Set this to zero to initiate loading of bloom " + "filters when a map file opened."),
  TABLE_BLOOM_SIZE("table.bloom.size", "1048576", PropertyType.COUNT, "Bloom filter size, as number of keys."),
  TABLE_BLOOM_ERRORRATE("table.bloom.error.rate", "0.5%", PropertyType.FRACTION, "Bloom filter error rate."),
  TABLE_BLOOM_KEY_FUNCTOR(
      "table.bloom.key.functor",
      "org.apache.accumulo.core.file.keyfunctor.RowFunctor",
      PropertyType.CLASSNAME,
      "A function that can transform the key prior to insertion and check of bloom filter.  org.apache.accumulo.core.file.keyfunctor.RowFunctor,"
          + ",org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor, and org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor are allowable values."
          + " One can extend any of the above mentioned classes to perform specialized parsing of the key. "),
  TABLE_BLOOM_HASHTYPE("table.bloom.hash.type", "murmur", PropertyType.STRING, "The bloom filter hash type"),
  TABLE_FAILURES_IGNORE("table.failures.ignore", "false", PropertyType.BOOLEAN,
      "If you want queries for your table to hang or fail when data is missing from the system, "
          + "then set this to false. When this set to true missing data will be reported but queries "
          + "will still run possibly returning a subset of the data."),
  TABLE_DEFAULT_SCANTIME_VISIBILITY("table.security.scan.visibility.default", "", PropertyType.STRING,
      "The security label that will be assumed at scan time if an entry does not have a visibility set.<br />"
          + "Note: An empty security label is displayed as []. The scan results will show an empty visibility even if "
          + "the visibility from this setting is applied to the entry.<br />"
          + "CAUTION: If a particular key has an empty security label AND its table's default visibility is also empty, "
          + "access will ALWAYS be granted for users with permission to that table. Additionally, if this field is changed, "
          + "all existing data with an empty visibility label will be interpreted with the new label on the next scan."),
  TABLE_LOCALITY_GROUPS("table.groups.enabled", "", PropertyType.STRING, "A comma separated list of locality group names to enable for this table."),
  TABLE_CONSTRAINT_PREFIX("table.constraint.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that add constraints to a table. "
          + "These properties start with the category prefix, followed by a number, and their values "
          + "correspond to a fully qualified Java class that implements the Constraint interface.<br />"
          + "For example, table.constraint.1 = org.apache.accumulo.core.constraints.MyCustomConstraint "
          + "and table.constraint.2 = my.package.constraints.MySecondConstraint"),
  TABLE_INDEXCACHE_ENABLED("table.cache.index.enable", "true", PropertyType.BOOLEAN, "Determines whether index cache is enabled."),
  TABLE_BLOCKCACHE_ENABLED("table.cache.block.enable", "false", PropertyType.BOOLEAN, "Determines whether file block cache is enabled."),
  TABLE_ITERATOR_PREFIX("table.iterator.", null, PropertyType.PREFIX,
      "Properties in this category specify iterators that are applied at various stages (scopes) of interaction "
          + "with a table. These properties start with the category prefix, followed by a scope (minc, majc, scan, etc.), "
          + "followed by a period, followed by a name, as in table.iterator.scan.vers, or table.iterator.scan.custom. "
          + "The values for these properties are a number indicating the ordering in which it is applied, and a class name "
          + "such as table.iterator.scan.vers = 10,org.apache.accumulo.core.iterators.VersioningIterator<br /> "
          + "These iterators can take options if additional properties are set that look like this property, "
          + "but are suffixed with a period, followed by 'opt' followed by another period, and a property name.<br />"
          + "For example, table.iterator.minc.vers.opt.maxVersions = 3"),
  TABLE_LOCALITY_GROUP_PREFIX("table.group.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that define locality groups in a table. These properties start "
          + "with the category prefix, followed by a name, followed by a period, and followed by a property for that group.<br />"
          + "For example table.group.group1=x,y,z sets the column families for a group called group1. Once configured, "
          + "group1 can be enabled by adding it to the list of groups in the " + TABLE_LOCALITY_GROUPS.getKey() + " property.<br />"
          + "Additional group options may be specified for a named group by setting table.group.&lt;name&gt;.opt.&lt;key&gt;=&lt;value&gt;."),
  TABLE_FORMATTER_CLASS("table.formatter", "org.apache.accumulo.core.util.format.DefaultFormatter", PropertyType.STRING,
      "The Formatter class to apply on results in the shell");
  
  private String key, defaultValue, description;
  private PropertyType type;
  
  private Property(String name, String defaultValue, PropertyType type, String description) {
    this.key = name;
    this.defaultValue = defaultValue;
    this.description = description;
    this.type = type;
  }
  
  public String toString() {
    return this.key;
  }
  
  public String getKey() {
    return this.key;
  }
  
  public String getDefaultValue() {
    return this.defaultValue;
  }
  
  public PropertyType getType() {
    return this.type;
  }
  
  public String getDescription() {
    return this.description;
  }
  
  private static HashSet<String> validTableProperties = null;
  
  public static boolean isValidTablePropertyKey(String key) {
    if (validTableProperties == null) {
      synchronized (Property.class) {
        if (validTableProperties == null) {
          HashSet<String> tmp = new HashSet<String>();
          for (Property p : Property.values())
            if (!p.getType().equals(PropertyType.PREFIX) && p.getKey().startsWith(Property.TABLE_PREFIX.getKey()))
              tmp.add(p.getKey());
          validTableProperties = tmp;
        }
      }
    }
    
    return validTableProperties.contains(key) || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())
        || key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) || key.startsWith(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey());
  }
  
  private static final EnumSet<Property> fixedProperties = EnumSet.of(Property.TSERV_CLIENTPORT, Property.TSERV_NATIVEMAP_ENABLED,
      Property.TSERV_MAJC_THREAD_MAXOPEN, Property.TSERV_SCAN_MAX_OPENFILES, Property.TSERV_MAJC_MAXCONCURRENT, Property.TSERV_LOGGER_COUNT,
      Property.LOGGER_PORT, Property.MASTER_CLIENTPORT, Property.GC_PORT);
  
  public static boolean isFixedZooPropertyKey(Property key) {
    return fixedProperties.contains(key);
  }
  
  public static Set<Property> getFixedProperties() {
    return fixedProperties;
  }
  
  public static boolean isValidZooPropertyKey(String key) {
    // white list prefixes
    return key.startsWith(Property.TABLE_PREFIX.getKey()) || key.startsWith(Property.TSERV_PREFIX.getKey()) || key.startsWith(Property.LOGGER_PREFIX.getKey())
        || key.startsWith(Property.MASTER_PREFIX.getKey()) || key.startsWith(Property.GC_PREFIX.getKey())
        || key.startsWith(Property.MONITOR_PREFIX.getKey() + "banner.");
  }
  
  public static Property getPropertyByKey(String key) {
    for (Property prop : Property.values())
      if (prop.getKey().equals(key))
        return prop;
    return null;
  }
}
