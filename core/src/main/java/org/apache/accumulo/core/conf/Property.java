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

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.interpret.DefaultScanInterpreter;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;

public enum Property {
  // Crypto-related properties
  @Experimental
  CRYPTO_PREFIX("crypto.", null, PropertyType.PREFIX, "Properties in this category related to the configuration of both default and custom crypto modules."),
  @Experimental
  CRYPTO_MODULE_CLASS("crypto.module.class", "NullCryptoModule", PropertyType.STRING,
      "Fully qualified class name of the class that implements the CryptoModule interface, to be used in setting up encryption at rest for the WAL and "
          + "(future) other parts of the code."),
  @Experimental
  CRYPTO_CIPHER_SUITE("crypto.cipher.suite", "NullCipher", PropertyType.STRING, "Describes the cipher suite to use for the write-ahead log"),
  @Experimental
  CRYPTO_CIPHER_ALGORITHM_NAME("crypto.cipher.algorithm.name", "NullCipher", PropertyType.STRING,
      "States the name of the algorithm used in the corresponding cipher suite. Do not make these different, unless you enjoy mysterious exceptions and bugs."),
  @Experimental
  CRYPTO_CIPHER_KEY_LENGTH("crypto.cipher.key.length", "128", PropertyType.STRING,
      "Specifies the key length *in bits* to use for the symmetric key, should probably be 128 or 256 unless you really know what you're doing"),
  @Experimental
  CRYPTO_SECURE_RNG("crypto.secure.rng", "SHA1PRNG", PropertyType.STRING,
      "States the secure random number generator to use, and defaults to the built-in Sun SHA1PRNG"),
  @Experimental
  CRYPTO_SECURE_RNG_PROVIDER("crypto.secure.rng.provider", "SUN", PropertyType.STRING,
      "States the secure random number generator provider to use, and defaults to the built-in SUN provider"),
  @Experimental
  CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS("crypto.secret.key.encryption.strategy.class", "NullSecretKeyEncryptionStrategy", PropertyType.STRING,
      "The class Accumulo should use for its key encryption strategy."),
  @Experimental
  CRYPTO_DEFAULT_KEY_STRATEGY_HDFS_URI("crypto.default.key.strategy.hdfs.uri", "", PropertyType.STRING,
      "The path relative to the top level instance directory (instance.dfs.dir) where to store the key encryption key within HDFS."),
  @Experimental
  CRYPTO_DEFAULT_KEY_STRATEGY_KEY_LOCATION("crypto.default.key.strategy.key.location", "/crypto/secret/keyEncryptionKey", PropertyType.ABSOLUTEPATH,
      "The path relative to the top level instance directory (instance.dfs.dir) where to store the key encryption key within HDFS."),
  @Experimental
  CRYPTO_DEFAULT_KEY_STRATEGY_CIPHER_SUITE("crypto.default.key.strategy.cipher.suite", "NullCipher", PropertyType.STRING,
      "The cipher suite to use when encrypting session keys with a key encryption key.  This should be set to match the overall encryption algorithm  " +
      "but with ECB mode and no padding unless you really know what you're doing and are sure you won't break internal file formats"),
  @Experimental
  CRYPTO_OVERRIDE_KEY_STRATEGY_WITH_CONFIGURED_STRATEGY("crypto.override.key.strategy.with.configured.strategy", "false", PropertyType.BOOLEAN,
          "The default behavior is to record the key encryption strategy with the encrypted file, and continue to use that strategy for the life " +
          "of that file.  Sometimes, you change your strategy and want to use the new strategy, not the old one.  (Most commonly, this will be " +
          "because you have moved key material from one spot to another.)  If you want to override the recorded key strategy with the one in " +
          "the configuration file, set this property to true."),
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
  @Sensitive
  INSTANCE_SECRET("instance.secret", "DEFAULT", PropertyType.STRING,
      "A secret unique to a given instance that all servers must know in order to communicate with one another."
          + " Change it before initialization. To change it later use ./bin/accumulo accumulo.server.util.ChangeSecret [oldpasswd] [newpasswd], "
          + " and then update conf/accumulo-site.xml everywhere."),
  INSTANCE_VOLUMES("instance.volumes", "", PropertyType.STRING, "A list of volumes to use.  By default, this will be the namenode in the hadoop configuration in the accumulo classpath."),
  INSTANCE_SECURITY_AUTHENTICATOR("instance.security.authenticator", "org.apache.accumulo.server.security.handler.ZKAuthenticator", PropertyType.CLASSNAME,
      "The authenticator class that accumulo will use to determine if a user has privilege to perform an action"),
  INSTANCE_SECURITY_AUTHORIZOR("instance.security.authorizor", "org.apache.accumulo.server.security.handler.ZKAuthorizor", PropertyType.CLASSNAME,
      "The authorizor class that accumulo will use to determine what labels a user has privilege to see"),
  INSTANCE_SECURITY_PERMISSION_HANDLER("instance.security.permissionHandler", "org.apache.accumulo.server.security.handler.ZKPermHandler",
      PropertyType.CLASSNAME, "The permission handler class that accumulo will use to determine if a user has privilege to perform an action"),

  // general properties
  GENERAL_PREFIX("general.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of accumulo overall, but do not have to be consistent throughout a cloud."),
  GENERAL_CLASSPATHS(AccumuloClassLoader.CLASSPATH_PROPERTY_NAME, AccumuloClassLoader.ACCUMULO_CLASSPATH_VALUE, PropertyType.STRING,
      "A list of all of the places to look for a class. Order does matter, as it will look for the jar "
          + "starting in the first location to the last. Please note, hadoop conf and hadoop lib directories NEED to be here, "
          + "along with accumulo lib and zookeeper directory. Supports full regex on filename alone."), // needs special treatment in accumulo start jar
  GENERAL_DYNAMIC_CLASSPATHS(AccumuloVFSClassLoader.DYNAMIC_CLASSPATH_PROPERTY_NAME, AccumuloVFSClassLoader.DEFAULT_DYNAMIC_CLASSPATH_VALUE,
      PropertyType.STRING, "A list of all of the places where changes in jars or classes will force a reload of the classloader."),
  GENERAL_RPC_TIMEOUT("general.rpc.timeout", "120s", PropertyType.TIMEDURATION, "Time to wait on I/O for simple, short RPC calls"),
  GENERAL_KERBEROS_KEYTAB("general.kerberos.keytab", "", PropertyType.PATH, "Path to the kerberos keytab to use. Leave blank if not using kerberoized hdfs"),
  GENERAL_KERBEROS_PRINCIPAL("general.kerberos.principal", "", PropertyType.STRING, "Name of the kerberos principal to use. _HOST will automatically be "
      + "replaced by the machines hostname in the hostname portion of the principal. Leave blank if not using kerberoized hdfs"),
  GENERAL_MAX_MESSAGE_SIZE("tserver.server.message.size.max", "1G", PropertyType.MEMORY, "The maximum size of a message that can be sent to a tablet server."),
  GENERAL_VOLUME_CHOOSER("general.volume.chooser", "org.apache.accumulo.server.fs.RandomVolumeChooser", PropertyType.CLASSNAME, "The class that will be used to select which volume will be used to create new files."),

  // properties that are specific to master server behavior
  MASTER_PREFIX("master.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the master server"),
  MASTER_CLIENTPORT("master.port.client", "9999", PropertyType.PORT, "The port used for handling client connections on the master"),
  MASTER_TABLET_BALANCER("master.tablet.balancer", "org.apache.accumulo.server.master.balancer.TableLoadBalancer", PropertyType.CLASSNAME,
      "The balancer class that accumulo will use to make tablet assignment and migration decisions."),
  MASTER_RECOVERY_MAXAGE("master.recovery.max.age", "60m", PropertyType.TIMEDURATION, "Recovery files older than this age will be removed."),
  MASTER_RECOVERY_MAXTIME("master.recovery.time.max", "30m", PropertyType.TIMEDURATION, "The maximum time to attempt recovery before giving up"),
  MASTER_BULK_RETRIES("master.bulk.retries", "3", PropertyType.COUNT, "The number of attempts to bulk-load a file before giving up."),
  MASTER_BULK_THREADPOOL_SIZE("master.bulk.threadpool.size", "5", PropertyType.COUNT, "The number of threads to use when coordinating a bulk-import."),
  MASTER_BULK_TIMEOUT("master.bulk.timeout", "5m", PropertyType.TIMEDURATION, "The time to wait for a tablet server to process a bulk import request"),
  MASTER_MINTHREADS("master.server.threads.minimum", "20", PropertyType.COUNT, "The minimum number of threads to use to handle incoming requests."),
  MASTER_THREADCHECK("master.server.threadcheck.time", "1s", PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool."),
  MASTER_RECOVERY_DELAY("master.recovery.delay", "10s", PropertyType.TIMEDURATION,
      "When a tablet server's lock is deleted, it takes time for it to completely quit. This delay gives it time before log recoveries begin."),
  MASTER_LEASE_RECOVERY_WAITING_PERIOD("master.lease.recovery.interval", "5s", PropertyType.TIMEDURATION,
      "The amount of time to wait after requesting a WAL file to be recovered"),
  MASTER_WALOG_CLOSER_IMPLEMETATION("master.walog.closer.implementation", "org.apache.accumulo.server.master.recovery.HadoopLogCloser", PropertyType.CLASSNAME,
      "A class that implements a mechansim to steal write access to a file"),
  MASTER_FATE_THREADPOOL_SIZE("master.fate.threadpool.size", "4", PropertyType.COUNT,
      "The number of threads used to run FAult-Tolerant Executions.  These are primarily table operations like merge."),

  // properties that are specific to tablet server behavior
  TSERV_PREFIX("tserver.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the tablet servers"),
  TSERV_CLIENT_TIMEOUT("tserver.client.timeout", "3s", PropertyType.TIMEDURATION, "Time to wait for clients to continue scans before closing a session."),
  TSERV_DEFAULT_BLOCKSIZE("tserver.default.blocksize", "1M", PropertyType.MEMORY, "Specifies a default blocksize for the tserver caches"),
  TSERV_DATACACHE_SIZE("tserver.cache.data.size", "128M", PropertyType.MEMORY, "Specifies the size of the cache for file data blocks."),
  TSERV_INDEXCACHE_SIZE("tserver.cache.index.size", "512M", PropertyType.MEMORY, "Specifies the size of the cache for file indices."),
  TSERV_PORTSEARCH("tserver.port.search", "false", PropertyType.BOOLEAN, "if the ports above are in use, search higher ports until one is available"),
  TSERV_CLIENTPORT("tserver.port.client", "9997", PropertyType.PORT, "The port used for handling client connections on the tablet servers"),
  TSERV_MUTATION_QUEUE_MAX("tserver.mutation.queue.max", "256K", PropertyType.MEMORY,
      "The amount of memory to use to store write-ahead-log mutations-per-session before flushing them."),
  TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN("tserver.tablet.split.midpoint.files.max", "30", PropertyType.COUNT,
      "To find a tablets split points, all index files are opened. This setting determines how many index "
          + "files can be opened at once. When there are more index files than this setting multiple passes "
          + "must be made, which is slower. However opening too many files at once can cause problems."),
  TSERV_WALOG_MAX_SIZE("tserver.walog.max.size", "1G", PropertyType.MEMORY,
      "The maximum size for each write-ahead log.  See comment for property tserver.memory.maps.max"),
  TSERV_MAJC_DELAY("tserver.compaction.major.delay", "30s", PropertyType.TIMEDURATION,
      "Time a tablet server will sleep between checking which tablets need compaction."),
  TSERV_MAJC_THREAD_MAXOPEN("tserver.compaction.major.thread.files.open.max", "10", PropertyType.COUNT,
      "Max number of files a major compaction thread can open at once. "),
  TSERV_SCAN_MAX_OPENFILES("tserver.scan.files.open.max", "100", PropertyType.COUNT,
      "Maximum total files that all tablets in a tablet server can open for scans. "),
  TSERV_MAX_IDLE("tserver.files.open.idle", "1m", PropertyType.TIMEDURATION, "Tablet servers leave previously used files open for future queries. "
      + "This setting determines how much time an unused file should be kept open until it is closed."),
  TSERV_NATIVEMAP_ENABLED("tserver.memory.maps.native.enabled", "true", PropertyType.BOOLEAN,
      "An in-memory data store for accumulo implemented in c++ that increases the amount of data accumulo can hold in memory and avoids Java GC pauses."),
  TSERV_MAXMEM("tserver.memory.maps.max", "1G", PropertyType.MEMORY,
      "Maximum amount of memory that can be used to buffer data written to a tablet server. There are two other properties that can effectively limit memory"
          + " usage table.compaction.minor.logs.threshold and tserver.walog.max.size. Ensure that table.compaction.minor.logs.threshold *"
          + " tserver.walog.max.size >= this property."),
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
  TSERV_MONITOR_FS("tserver.monitor.fs", "true", PropertyType.BOOLEAN,
      "When enabled the tserver will monitor file systems and kill itself when one switches from rw to ro.  This is usually and indication that Linux has"
          + " detected a bad disk."),
  TSERV_MEMDUMP_DIR("tserver.dir.memdump", "/tmp", PropertyType.PATH,
      "A long running scan could possibly hold memory that has been minor compacted.  To prevent this, the in memory map is dumped to a local file and the "
          + "scan is switched to that local file.  We can not switch to the minor compacted file because it may have been modified by iterators.  The file "
          + "dumped to the local dir is an exact copy of what was in memory."),
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
  TSERV_BULK_RETRY("tserver.bulk.retry.max", "5", PropertyType.COUNT,
      "The number of times the tablet server will attempt to assign a file to a tablet as it migrates and splits."),
  TSERV_BULK_TIMEOUT("tserver.bulk.timeout", "5m", PropertyType.TIMEDURATION, "The time to wait for a tablet server to process a bulk import request."),
  TSERV_MINTHREADS("tserver.server.threads.minimum", "20", PropertyType.COUNT, "The minimum number of threads to use to handle incoming requests."),
  TSERV_THREADCHECK("tserver.server.threadcheck.time", "1s", PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool."),
  TSERV_HOLD_TIME_SUICIDE("tserver.hold.time.max", "5m", PropertyType.TIMEDURATION,
      "The maximum time for a tablet server to be in the \"memory full\" state.  If the tablet server cannot write out memory"
          + " in this much time, it will assume there is some failure local to its node, and quit.  A value of zero is equivalent to forever."),
  TSERV_WAL_BLOCKSIZE("tserver.wal.blocksize", "0", PropertyType.MEMORY,
      "The size of the HDFS blocks used to write to the Write-Ahead log.  If zero, it will be 110% of tserver.walog.max.size (that is, try to use just one"
          + " block)"),
  TSERV_WAL_REPLICATION("tserver.wal.replication", "0", PropertyType.COUNT,
      "The replication to use when writing the Write-Ahead log to HDFS. If zero, it will use the HDFS default replication setting."),
  TSERV_RECOVERY_MAX_CONCURRENT("tserver.recovery.concurrent.max", "2", PropertyType.COUNT, "The maximum number of threads to use to sort logs during"
      + " recovery"),
  TSERV_SORT_BUFFER_SIZE("tserver.sort.buffer.size", "200M", PropertyType.MEMORY, "The amount of memory to use when sorting logs during recovery."),
  TSERV_ARCHIVE_WALOGS("tserver.archive.walogs", "false", PropertyType.BOOLEAN, "Keep copies of the WALOGs for debugging purposes"),
  TSERV_WORKQ_THREADS("tserver.workq.threads", "2", PropertyType.COUNT,
      "The number of threads for the distributed workq.  These threads are used for copying failed bulk files."),
  TSERV_WAL_SYNC("tserver.wal.sync", "true", PropertyType.BOOLEAN,
      "Use the SYNC_BLOCK create flag to sync WAL writes to disk. Prevents problems recovering from sudden system resets."),

  // properties that are specific to logger server behavior
  LOGGER_PREFIX("logger.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the write-ahead logger servers"),
  LOGGER_DIR("logger.dir.walog", "walogs", PropertyType.PATH,
      "The property only needs to be set if upgrading from 1.4 which used to store write-ahead logs on the local filesystem. In 1.5 write-ahead logs are "
          + "stored in DFS.  When 1.5 is started for the first time it will copy any 1.4 write ahead logs into DFS.  It is possible to specify a "
          + "comma-separated list of directories."),

  // accumulo garbage collector properties
  GC_PREFIX("gc.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the accumulo garbage collector."),
  GC_CYCLE_START("gc.cycle.start", "30s", PropertyType.TIMEDURATION, "Time to wait before attempting to garbage collect any old files."),
  GC_CYCLE_DELAY("gc.cycle.delay", "5m", PropertyType.TIMEDURATION, "Time between garbage collection cycles. In each cycle, old files "
      + "no longer in use are removed from the filesystem."),
  GC_PORT("gc.port.client", "50091", PropertyType.PORT, "The listening port for the garbage collector's monitor service"),
  GC_DELETE_THREADS("gc.threads.delete", "16", PropertyType.COUNT, "The number of threads used to delete files"),
  GC_TRASH_IGNORE("gc.trash.ignore", "false", PropertyType.BOOLEAN, "Do not use the Trash, even if it is configured"),

  // properties that are specific to the monitor server behavior
  MONITOR_PREFIX("monitor.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the monitor web server."),
  MONITOR_PORT("monitor.port.client", "50095", PropertyType.PORT, "The listening port for the monitor's http service"),
  MONITOR_LOG4J_PORT("monitor.port.log4j", "4560", PropertyType.PORT, "The listening port for the monitor's log4j logging collection."),
  MONITOR_BANNER_TEXT("monitor.banner.text", "", PropertyType.STRING, "The banner text displayed on the monitor page."),
  MONITOR_BANNER_COLOR("monitor.banner.color", "#c4c4c4", PropertyType.STRING, "The color of the banner text displayed on the monitor page."),
  MONITOR_BANNER_BACKGROUND("monitor.banner.background", "#304065", PropertyType.STRING,
      "The background color of the banner text displayed on the monitor page."),

  @Experimental
  MONITOR_SSL_KEYSTORE("monitor.ssl.keyStore", "", PropertyType.PATH, "The keystore for enabling monitor SSL."),
  @Experimental
  @Sensitive
  MONITOR_SSL_KEYSTOREPASS("monitor.ssl.keyStorePassword", "", PropertyType.STRING, "The keystore password for enabling monitor SSL."),
  @Experimental
  MONITOR_SSL_TRUSTSTORE("monitor.ssl.trustStore", "", PropertyType.PATH, "The truststore for enabling monitor SSL."),
  @Experimental
  @Sensitive
  MONITOR_SSL_TRUSTSTOREPASS("monitor.ssl.trustStorePassword", "", PropertyType.STRING, "The truststore password for enabling monitor SSL."),
  
  TRACE_PREFIX("trace.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of distributed tracing."),
  TRACE_PORT("trace.port.client", "12234", PropertyType.PORT, "The listening port for the trace server"),
  TRACE_TABLE("trace.table", "trace", PropertyType.STRING, "The name of the table to store distributed traces"),
  TRACE_USER("trace.user", "root", PropertyType.STRING, "The name of the user to store distributed traces"),
  @Sensitive
  TRACE_PASSWORD("trace.password", "secret", PropertyType.STRING, "The password for the user used to store distributed traces"),
  @Sensitive
  TRACE_TOKEN_PROPERTY_PREFIX("trace.token.property", null, PropertyType.PREFIX,
      "The prefix used to create a token for storing distributed traces.  For each propetry required by trace.token.type, place this prefix in front of it."),
  TRACE_TOKEN_TYPE("trace.token.type", PasswordToken.class.getName(), PropertyType.CLASSNAME, "An AuthenticationToken type supported by the authorizer"),

  // per table properties
  TABLE_PREFIX("table.", null, PropertyType.PREFIX, "Properties in this category affect tablet server treatment of tablets, but can be configured "
      + "on a per-table basis. Setting these properties in the site file will override the default globally "
      + "for all tables and not any specific table. However, both the default and the global setting can be "
      + "overridden per table using the table operations API or in the shell, which sets the overridden value "
      + "in zookeeper. Restarting accumulo tablet servers after setting these properties in the site file "
      + "will cause the global setting to take effect. However, you must use the API or the shell to change "
      + "properties in zookeeper that are set on a table."),
  TABLE_MAJC_RATIO("table.compaction.major.ratio", "3", PropertyType.FRACTION,
      "minimum ratio of total input size to maximum input file size for running a major compaction.   When adjusting this property you may want to also "
          + "adjust table.file.max.  Want to avoid the situation where only merging minor compactions occur."),
  TABLE_MAJC_COMPACTALL_IDLETIME("table.compaction.major.everything.idle", "1h", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have all "
          + "of its files compacted into one.  There is no guarantee an idle tablet will be compacted. "
          + "Compactions of idle tablets are only started when regular compactions are not running. Idle "
          + "compactions only take place for tablets that have one or more files."),
  TABLE_SPLIT_THRESHOLD("table.split.threshold", "1G", PropertyType.MEMORY, "When combined size of files exceeds this amount a tablet is split."),
  TABLE_MINC_LOGS_MAX("table.compaction.minor.logs.threshold", "3", PropertyType.COUNT,
      "When there are more than this many write-ahead logs against a tablet, it will be minor compacted.  See comment for property tserver.memory.maps.max"),
  TABLE_MINC_COMPACT_IDLETIME("table.compaction.minor.idle", "5m", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have its "
          + "in-memory map flushed to disk in a minor compaction.  There is no guarantee an idle " + "tablet will be compacted."),
  TABLE_SCAN_MAXMEM("table.scan.max.memory", "512K", PropertyType.MEMORY,
      "The maximum amount of memory that will be used to cache results of a client query/scan. "
          + "Once this limit is reached, the buffered data is sent to the client."),
  TABLE_FILE_TYPE("table.file.type", RFile.EXTENSION, PropertyType.STRING, "Change the type of file a table writes"),
  TABLE_LOAD_BALANCER("table.balancer", "org.apache.accumulo.server.master.balancer.DefaultLoadBalancer", PropertyType.STRING,
      "This property can be set to allow the LoadBalanceByTable load balancer to change the called Load Balancer for this table"),
  TABLE_FILE_COMPRESSION_TYPE("table.file.compress.type", "gz", PropertyType.STRING, "One of gz,lzo,none"),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE("table.file.compress.blocksize", "100K", PropertyType.MEMORY,
      "Similar to the hadoop io.seqfile.compress.blocksize setting, so that files have better query performance. The maximum value for this is "
          + Integer.MAX_VALUE + ". (This setting is the size threshold prior to compression, and applies even compression is disabled.)"),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX("table.file.compress.blocksize.index", "128K", PropertyType.MEMORY,
      "Determines how large index blocks can be in files that support multilevel indexes. The maximum value for this is " + Integer.MAX_VALUE + "."
          + " (This setting is the size threshold prior to compression, and applies even compression is disabled.)"),
  TABLE_FILE_BLOCK_SIZE("table.file.blocksize", "0B", PropertyType.MEMORY,
      "Overrides the hadoop dfs.block.size setting so that files have better query performance. The maximum value for this is " + Integer.MAX_VALUE),
  TABLE_FILE_REPLICATION("table.file.replication", "0", PropertyType.COUNT, "Determines how many replicas to keep of a tables' files in HDFS. "
      + "When this value is LTE 0, HDFS defaults are used."),
  TABLE_FILE_MAX("table.file.max", "15", PropertyType.COUNT,
      "Determines the max # of files each tablet in a table can have. When adjusting this property you may want to consider adjusting"
          + " table.compaction.major.ratio also.  Setting this property to 0 will make it default to tserver.scan.files.open.max-1, this will prevent a"
          + " tablet from having more files than can be opened.  Setting this property low may throttle ingest and increase query performance."),
  TABLE_WALOG_ENABLED("table.walog.enabled", "true", PropertyType.BOOLEAN, "Use the write-ahead log to prevent the loss of data."),
  TABLE_BLOOM_ENABLED("table.bloom.enabled", "false", PropertyType.BOOLEAN, "Use bloom filters on this table."),
  TABLE_BLOOM_LOAD_THRESHOLD("table.bloom.load.threshold", "1", PropertyType.COUNT,
      "This number of seeks that would actually use a bloom filter must occur before a file's bloom filter is loaded."
          + " Set this to zero to initiate loading of bloom filters when a file is opened."),
  TABLE_BLOOM_SIZE("table.bloom.size", "1048576", PropertyType.COUNT, "Bloom filter size, as number of keys."),
  TABLE_BLOOM_ERRORRATE("table.bloom.error.rate", "0.5%", PropertyType.FRACTION, "Bloom filter error rate."),
  TABLE_BLOOM_KEY_FUNCTOR("table.bloom.key.functor", "org.apache.accumulo.core.file.keyfunctor.RowFunctor", PropertyType.CLASSNAME,
      "A function that can transform the key prior to insertion and check of bloom filter.  org.apache.accumulo.core.file.keyfunctor.RowFunctor,"
          + ",org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor, and org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor are"
          + " allowable values. One can extend any of the above mentioned classes to perform specialized parsing of the key. "),
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
  TABLE_FORMATTER_CLASS("table.formatter", DefaultFormatter.class.getName(), PropertyType.STRING, "The Formatter class to apply on results in the shell"),
  TABLE_INTERPRETER_CLASS("table.interepreter", DefaultScanInterpreter.class.getName(), PropertyType.STRING,
      "The ScanInterpreter class to apply on scan arguments in the shell"),
  TABLE_CLASSPATH("table.classpath.context", "", PropertyType.STRING, "Per table classpath context"),

  // VFS ClassLoader properties
  VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY(AccumuloVFSClassLoader.VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY, "", PropertyType.STRING,
      "Configuration for a system level vfs classloader.  Accumulo jar can be configured here and loaded out of HDFS."),
  VFS_CONTEXT_CLASSPATH_PROPERTY(AccumuloVFSClassLoader.VFS_CONTEXT_CLASSPATH_PROPERTY, null, PropertyType.PREFIX,
      "Properties in this category are define a classpath. These properties start  with the category prefix, followed by a context name.  "
          + "The value is a comma seperated list of URIs. Supports full regex on filename alone. For example, "
          + "general.vfs.context.classpath.cx1=hdfs://nn1:9902/mylibdir/*.jar.  "
          + "You can enable post delegation for a context, which will load classes from the context first instead of the parent first.  "
          + "Do this by setting general.vfs.context.classpath.&lt;name&gt;.delegation=post, where &lt;name&gt; is your context name.  "
          + "If delegation is not specified, it defaults to loading from parent classloader first."),
  @Interpolated
  VFS_CLASSLOADER_CACHE_DIR(AccumuloVFSClassLoader.VFS_CACHE_DIR, "${java.io.tmpdir}" + File.separator + "accumulo-vfs-cache-${user.name}",
      PropertyType.ABSOLUTEPATH, "Directory to use for the vfs cache. The cache will keep a soft reference to all of the classes loaded in the VM."
          + " This should be on local disk on each node with sufficient space. It defaults to ${java.io.tmpdir}/accumulo-vfs-cache-${user.name}"),
  
  @Interpolated
  @Experimental
  GENERAL_MAVEN_PROJECT_BASEDIR(AccumuloClassLoader.MAVEN_PROJECT_BASEDIR_PROPERTY_NAME, AccumuloClassLoader.DEFAULT_MAVEN_PROJECT_BASEDIR_VALUE,
      PropertyType.ABSOLUTEPATH, "Set this to automatically add maven target/classes directories to your dynamic classpath"),
  
  ;
  

  private String key, defaultValue, description;
  private PropertyType type;
  static Logger log = Logger.getLogger(Property.class);
  
  private Property(String name, String defaultValue, PropertyType type, String description) {
    this.key = name;
    this.defaultValue = defaultValue;
    this.description = description;
    this.type = type;
  }

  @Override
  public String toString() {
    return this.key;
  }

  public String getKey() {
    return this.key;
  }

  public String getRawDefaultValue() {
    return this.defaultValue;
  }

  public String getDefaultValue() {
    if (isInterpolated()) {
      PropertiesConfiguration pconf = new PropertiesConfiguration();
      pconf.append(new SystemConfiguration());
      pconf.addProperty("hack_default_value", this.defaultValue);
      String v = pconf.getString("hack_default_value");
      if (this.type == PropertyType.ABSOLUTEPATH)
        return new File(v).getAbsolutePath();
      else
        return v;
    } else {
      return getRawDefaultValue();
    }
  }

  public PropertyType getType() {
    return this.type;
  }

  public String getDescription() {
    return this.description;
  }

  private boolean isInterpolated() {
    return hasAnnotation(Interpolated.class) || hasPrefixWithAnnotation(getKey(), Interpolated.class);
  }

  public boolean isExperimental() {
    return hasAnnotation(Experimental.class) || hasPrefixWithAnnotation(getKey(), Experimental.class);
  }
  
  public boolean isDeprecated() {
    return hasAnnotation(Deprecated.class) || hasPrefixWithAnnotation(getKey(), Deprecated.class);
  }
  
  public boolean isSensitive() {
    return hasAnnotation(Sensitive.class) || hasPrefixWithAnnotation(getKey(), Sensitive.class);
  }
  
  public static boolean isSensitive(String key) {
    return hasPrefixWithAnnotation(key, Sensitive.class);
  }
  
  private <T extends Annotation> boolean hasAnnotation(Class<T> annotationType) {
    Logger log = Logger.getLogger(getClass());
    try {
      for (Annotation a : getClass().getField(name()).getAnnotations())
        if (annotationType.isInstance(a))
          return true;
    } catch (SecurityException e) {
      log.error(e, e);
    } catch (NoSuchFieldException e) {
      log.error(e, e);
    }
    return false;
  }
  
  private static <T extends Annotation> boolean hasPrefixWithAnnotation(String key, Class<T> annotationType) {
    // relies on side-effects of isValidPropertyKey to populate validPrefixes
    if (isValidPropertyKey(key)) {
      // check if property exists on its own and has the annotation
      if (Property.getPropertyByKey(key) != null)
        return getPropertyByKey(key).hasAnnotation(annotationType);
      // can't find the property, so check the prefixes
      boolean prefixHasAnnotation = false;
      for (String prefix : validPrefixes)
        if (key.startsWith(prefix))
          prefixHasAnnotation = prefixHasAnnotation || getPropertyByKey(prefix).hasAnnotation(annotationType);
      return prefixHasAnnotation;
    }
    return false;
  }

  private static HashSet<String> validTableProperties = null;
  private static HashSet<String> validProperties = null;
  private static HashSet<String> validPrefixes = null;

  private static boolean isKeyValidlyPrefixed(String key) {
    for (String prefix : validPrefixes) {
      if (key.startsWith(prefix))
        return true;
    }

    return false;
  }

  public synchronized static boolean isValidPropertyKey(String key) {
    if (validProperties == null) {
      validProperties = new HashSet<String>();
      validPrefixes = new HashSet<String>();

      for (Property p : Property.values()) {
        if (p.getType().equals(PropertyType.PREFIX)) {
          validPrefixes.add(p.getKey());
        } else {
          validProperties.add(p.getKey());
        }
      }
    }

    return validProperties.contains(key) || isKeyValidlyPrefixed(key);
  }

  public synchronized static boolean isValidTablePropertyKey(String key) {
    if (validTableProperties == null) {
      validTableProperties = new HashSet<String>();
      for (Property p : Property.values()) {
        if (!p.getType().equals(PropertyType.PREFIX) && p.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
          validTableProperties.add(p.getKey());
        }
      }
    }

    return validTableProperties.contains(key) || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())
        || key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) || key.startsWith(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey());
  }

  private static final EnumSet<Property> fixedProperties = EnumSet.of(Property.TSERV_CLIENTPORT, Property.TSERV_NATIVEMAP_ENABLED,
      Property.TSERV_SCAN_MAX_OPENFILES, Property.MASTER_CLIENTPORT, Property.GC_PORT);

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
        || key.startsWith(Property.MONITOR_PREFIX.getKey() + "banner.") || key.startsWith(VFS_CONTEXT_CLASSPATH_PROPERTY.getKey());
  }

  public static Property getPropertyByKey(String key) {
    for (Property prop : Property.values())
      if (prop.getKey().equals(key))
        return prop;
    return null;
  }

  /**
   * @return true if this is a property whose value is expected to be a java class
   */
  public static boolean isClassProperty(String key) {
    return (key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey()) && key.substring(Property.TABLE_CONSTRAINT_PREFIX.getKey().length()).split("\\.").length == 1)
        || (key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) && key.substring(Property.TABLE_ITERATOR_PREFIX.getKey().length()).split("\\.").length == 2)
        || key.equals(Property.TABLE_LOAD_BALANCER.getKey());
  }

  public static <T> T createInstanceFromPropertyName(AccumuloConfiguration conf, Property property, Class<T> base, T defaultInstance) {
    String clazzName = conf.get(property);
    T instance = null;

    try {
      Class<? extends T> clazz = AccumuloVFSClassLoader.loadClass(clazzName, base);
      instance = clazz.newInstance();
      log.info("Loaded class : " + clazzName);
    } catch (Exception e) {
      log.warn("Failed to load class ", e);
    }

    if (instance == null) {
      log.info("Using " + defaultInstance.getClass().getName());
      instance = defaultInstance;
    }
    return instance;
  }
}
