/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.conf;

import java.lang.annotation.Annotation;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.constraints.NoDeleteConstraint;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.spi.scan.ScanPrioritizer;
import org.apache.accumulo.core.spi.scan.SimpleScanDispatcher;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.interpret.DefaultScanInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public enum Property {
  // SSL properties local to each node (see also instance.ssl.enabled which must be consistent
  // across all nodes in an instance)
  RPC_PREFIX("rpc.", null, PropertyType.PREFIX,
      "Properties in this category related to the configuration of SSL keys for"
          + " RPC. See also instance.ssl.enabled"),
  RPC_SSL_KEYSTORE_PATH("rpc.javax.net.ssl.keyStore", "", PropertyType.PATH,
      "Path of the keystore file for the server's private SSL key"),
  @Sensitive
  RPC_SSL_KEYSTORE_PASSWORD("rpc.javax.net.ssl.keyStorePassword", "", PropertyType.STRING,
      "Password used to encrypt the SSL private keystore. "
          + "Leave blank to use the Accumulo instance secret"),
  RPC_SSL_KEYSTORE_TYPE("rpc.javax.net.ssl.keyStoreType", "jks", PropertyType.STRING,
      "Type of SSL keystore"),
  RPC_SSL_TRUSTSTORE_PATH("rpc.javax.net.ssl.trustStore", "", PropertyType.PATH,
      "Path of the truststore file for the root cert"),
  @Sensitive
  RPC_SSL_TRUSTSTORE_PASSWORD("rpc.javax.net.ssl.trustStorePassword", "", PropertyType.STRING,
      "Password used to encrypt the SSL truststore. Leave blank to use no password"),
  RPC_SSL_TRUSTSTORE_TYPE("rpc.javax.net.ssl.trustStoreType", "jks", PropertyType.STRING,
      "Type of SSL truststore"),
  RPC_USE_JSSE("rpc.useJsse", "false", PropertyType.BOOLEAN,
      "Use JSSE system properties to configure SSL rather than the " + RPC_PREFIX.getKey()
          + "javax.net.ssl.* Accumulo properties"),
  RPC_SSL_CIPHER_SUITES("rpc.ssl.cipher.suites", "", PropertyType.STRING,
      "Comma separated list of cipher suites that can be used by accepted connections"),
  RPC_SSL_ENABLED_PROTOCOLS("rpc.ssl.server.enabled.protocols", "TLSv1.2", PropertyType.STRING,
      "Comma separated list of protocols that can be used to accept connections"),
  RPC_SSL_CLIENT_PROTOCOL("rpc.ssl.client.protocol", "TLSv1.2", PropertyType.STRING,
      "The protocol used to connect to a secure server, must be in the list of enabled protocols "
          + "on the server side (rpc.ssl.server.enabled.protocols)"),
  /**
   * @since 1.7.0
   */
  RPC_SASL_QOP("rpc.sasl.qop", "auth", PropertyType.STRING,
      "The quality of protection to be used with SASL. Valid values are 'auth', 'auth-int',"
          + " and 'auth-conf'"),

  // instance properties (must be the same for every node in an instance)
  INSTANCE_PREFIX("instance.", null, PropertyType.PREFIX,
      "Properties in this category must be consistent throughout a cloud. "
          + "This is enforced and servers won't be able to communicate if these differ."),
  INSTANCE_ZK_HOST("instance.zookeeper.host", "localhost:2181", PropertyType.HOSTLIST,
      "Comma separated list of zookeeper servers"),
  INSTANCE_ZK_TIMEOUT("instance.zookeeper.timeout", "30s", PropertyType.TIMEDURATION,
      "Zookeeper session timeout; "
          + "max value when represented as milliseconds should be no larger than "
          + Integer.MAX_VALUE),
  @Sensitive
  INSTANCE_SECRET("instance.secret", "DEFAULT", PropertyType.STRING,
      "A secret unique to a given instance that all servers must know in order"
          + " to communicate with one another. It should be changed prior to the"
          + " initialization of Accumulo. To change it after Accumulo has been"
          + " initialized, use the ChangeSecret tool and then update accumulo.properties"
          + " everywhere. Before using the ChangeSecret tool, make sure Accumulo is not"
          + " running and you are logged in as the user that controls Accumulo files in"
          + " HDFS. To use the ChangeSecret tool, run the command: ./bin/accumulo"
          + " org.apache.accumulo.server.util.ChangeSecret"),
  INSTANCE_VOLUMES("instance.volumes", "", PropertyType.STRING,
      "A comma separated list of dfs uris to use. Files will be stored across"
          + " these filesystems. In some situations, the first volume in this list"
          + " may be treated differently, such as being preferred for writing out"
          + " temporary files (for example, when creating a pre-split table)."
          + " After adding uris to this list, run 'accumulo init --add-volume' and then"
          + " restart tservers. If entries are removed from this list then tservers"
          + " will need to be restarted. After a uri is removed from the list Accumulo"
          + " will not create new files in that location, however Accumulo can still"
          + " reference files created at that location before the config change. To use"
          + " a comma or other reserved characters in a URI use standard URI hex"
          + " encoding. For example replace commas with %2C."),
  INSTANCE_VOLUMES_REPLACEMENTS("instance.volumes.replacements", "", PropertyType.STRING,
      "Since accumulo stores absolute URIs changing the location of a namenode "
          + "could prevent Accumulo from starting. The property helps deal with "
          + "that situation. Provide a comma separated list of uri replacement "
          + "pairs here if a namenode location changes. Each pair should be separated "
          + "with a space. For example, if hdfs://nn1 was replaced with "
          + "hdfs://nnA and hdfs://nn2 was replaced with hdfs://nnB, then set this "
          + "property to 'hdfs://nn1 hdfs://nnA,hdfs://nn2 hdfs://nnB' "
          + "Replacements must be configured for use. To see which volumes are "
          + "currently in use, run 'accumulo admin volumes -l'. To use a comma or "
          + "other reserved characters in a URI use standard URI hex encoding. For "
          + "example replace commas with %2C."),
  INSTANCE_VOLUMES_UPGRADE_RELATIVE("instance.volumes.upgrade.relative", "", PropertyType.STRING,
      "The volume dfs uri containing relative tablet file paths. Relative paths may exist in the metadata from "
          + "versions prior to 1.6. This property is only required if a relative path is detected "
          + "during the upgrade process and will only be used once."),
  @Experimental // interface uses unstable internal types, use with caution
  INSTANCE_SECURITY_AUTHENTICATOR("instance.security.authenticator",
      "org.apache.accumulo.server.security.handler.ZKAuthenticator", PropertyType.CLASSNAME,
      "The authenticator class that accumulo will use to determine if a user "
          + "has privilege to perform an action"),
  @Experimental // interface uses unstable internal types, use with caution
  INSTANCE_SECURITY_AUTHORIZOR("instance.security.authorizor",
      "org.apache.accumulo.server.security.handler.ZKAuthorizor", PropertyType.CLASSNAME,
      "The authorizor class that accumulo will use to determine what labels a "
          + "user has privilege to see"),
  @Experimental // interface uses unstable internal types, use with caution
  INSTANCE_SECURITY_PERMISSION_HANDLER("instance.security.permissionHandler",
      "org.apache.accumulo.server.security.handler.ZKPermHandler", PropertyType.CLASSNAME,
      "The permission handler class that accumulo will use to determine if a "
          + "user has privilege to perform an action"),
  INSTANCE_RPC_SSL_ENABLED("instance.rpc.ssl.enabled", "false", PropertyType.BOOLEAN,
      "Use SSL for socket connections from clients and among accumulo services. "
          + "Mutually exclusive with SASL RPC configuration."),
  INSTANCE_RPC_SSL_CLIENT_AUTH("instance.rpc.ssl.clientAuth", "false", PropertyType.BOOLEAN,
      "Require clients to present certs signed by a trusted root"),
  /**
   * @since 1.7.0
   */
  INSTANCE_RPC_SASL_ENABLED("instance.rpc.sasl.enabled", "false", PropertyType.BOOLEAN,
      "Configures Thrift RPCs to require SASL with GSSAPI which supports "
          + "Kerberos authentication. Mutually exclusive with SSL RPC configuration."),
  INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION("instance.rpc.sasl.allowed.user.impersonation", "",
      PropertyType.STRING,
      "One-line configuration property controlling what users are allowed to "
          + "impersonate other users"),
  INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION("instance.rpc.sasl.allowed.host.impersonation", "",
      PropertyType.STRING,
      "One-line configuration property controlling the network locations "
          + "(hostnames) that are allowed to impersonate other users"),
  // Crypto-related properties
  @Experimental
  INSTANCE_CRYPTO_PREFIX("instance.crypto.opts.", null, PropertyType.PREFIX,
      "Properties related to on-disk file encryption."),
  @Experimental
  @Sensitive
  INSTANCE_CRYPTO_SENSITIVE_PREFIX("instance.crypto.opts.sensitive.", null, PropertyType.PREFIX,
      "Sensitive properties related to on-disk file encryption."),
  @Experimental
  INSTANCE_CRYPTO_SERVICE("instance.crypto.service",
      "org.apache.accumulo.core.spi.crypto.NoCryptoService", PropertyType.CLASSNAME,
      "The class which executes on-disk file encryption. The default does nothing. To enable "
          + "encryption, replace this classname with an implementation of the"
          + "org.apache.accumulo.core.spi.crypto.CryptoService interface."),

  // general properties
  GENERAL_PREFIX("general.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of accumulo overall, but"
          + " do not have to be consistent throughout a cloud."),
  @Deprecated(since = "2.0.0")
  GENERAL_DYNAMIC_CLASSPATHS(
      org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.DYNAMIC_CLASSPATH_PROPERTY_NAME,
      org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.DEFAULT_DYNAMIC_CLASSPATH_VALUE,
      PropertyType.STRING,
      "This property is deprecated since 2.0.0. A list of all of the places where changes "
          + "in jars or classes will force a reload of the classloader. Built-in dynamic class "
          + "loading will be removed in a future version. If this is needed, consider overriding "
          + "the Java system class loader with one that has this feature "
          + "(https://docs.oracle.com/javase/8/docs/api/java/lang/ClassLoader.html#getSystemClassLoader--). "
          + "Additionally, this property no longer does property interpolation of environment "
          + "variables, such as '$ACCUMULO_HOME'. Use commons-configuration syntax,"
          + "'${env:ACCUMULO_HOME}' instead."),
  GENERAL_CONTEXT_CLASSLOADER_FACTORY("general.context.class.loader.factory", "",
      PropertyType.CLASSNAME,
      "Name of classloader factory to be used to create classloaders for named contexts,"
          + " such as per-table contexts set by `table.class.loader.context`."),
  GENERAL_RPC_TIMEOUT("general.rpc.timeout", "120s", PropertyType.TIMEDURATION,
      "Time to wait on I/O for simple, short RPC calls"),
  @Experimental
  GENERAL_RPC_SERVER_TYPE("general.rpc.server.type", "", PropertyType.STRING,
      "Type of Thrift server to instantiate, see "
          + "org.apache.accumulo.server.rpc.ThriftServerType for more information. "
          + "Only useful for benchmarking thrift servers"),
  GENERAL_KERBEROS_KEYTAB("general.kerberos.keytab", "", PropertyType.PATH,
      "Path to the kerberos keytab to use. Leave blank if not using kerberoized hdfs"),
  GENERAL_KERBEROS_PRINCIPAL("general.kerberos.principal", "", PropertyType.STRING,
      "Name of the kerberos principal to use. _HOST will automatically be "
          + "replaced by the machines hostname in the hostname portion of the "
          + "principal. Leave blank if not using kerberoized hdfs"),
  GENERAL_KERBEROS_RENEWAL_PERIOD("general.kerberos.renewal.period", "30s",
      PropertyType.TIMEDURATION,
      "The amount of time between attempts to perform Kerberos ticket renewals."
          + " This does not equate to how often tickets are actually renewed (which is"
          + " performed at 80% of the ticket lifetime)."),
  GENERAL_MAX_MESSAGE_SIZE("general.server.message.size.max", "1G", PropertyType.BYTES,
      "The maximum size of a message that can be sent to a server."),
  GENERAL_SIMPLETIMER_THREADPOOL_SIZE("general.server.simpletimer.threadpool.size", "1",
      PropertyType.COUNT, "The number of threads to use for " + "server-internal scheduled tasks"),
  // If you update the default type, be sure to update the default used for initialization failures
  // in VolumeManagerImpl
  @Experimental
  GENERAL_VOLUME_CHOOSER("general.volume.chooser", RandomVolumeChooser.class.getName(),
      PropertyType.CLASSNAME,
      "The class that will be used to select which volume will be used to create new files."),
  GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS("general.security.credential.provider.paths", "",
      PropertyType.STRING, "Comma-separated list of paths to CredentialProviders"),
  GENERAL_ARBITRARY_PROP_PREFIX("general.custom.", null, PropertyType.PREFIX,
      "Prefix to be used for user defined system-wide properties. This may be"
          + " particularly useful for system-wide configuration for various"
          + " user-implementations of pluggable Accumulo features, such as the balancer"
          + " or volume chooser."),
  GENERAL_DELEGATION_TOKEN_LIFETIME("general.delegation.token.lifetime", "7d",
      PropertyType.TIMEDURATION,
      "The length of time that delegation tokens and secret keys are valid"),
  GENERAL_DELEGATION_TOKEN_UPDATE_INTERVAL("general.delegation.token.update.interval", "1d",
      PropertyType.TIMEDURATION, "The length of time between generation of new secret keys"),
  GENERAL_MAX_SCANNER_RETRY_PERIOD("general.max.scanner.retry.period", "5s",
      PropertyType.TIMEDURATION,
      "The maximum amount of time that a Scanner should wait before retrying a failed RPC"),
  // experimental - micrometer properties
  @Experimental
  GENERAL_METRICS_CONFIGURATION_PROPERTIES_FILE("general.metrics.configuration.properties",
      "accumulo.metrics.configuration.properties", PropertyType.STRING,
      "The Accumulo metrics configuration properties filename. The file is used to"
          + " configure micrometer MeterRegistries that loaded at runtime using classes that"
          + " extend MetricsServiceLoader.class"),
  // properties that are specific to manager server behavior
  MANAGER_PREFIX("manager.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the manager server."),
  @Deprecated(since = "2.1.0")
  @ReplacedBy(property = Property.MANAGER_PREFIX)
  MASTER_PREFIX("master.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the manager (formerly named master) server. "
          + "Since 2.1.0, all properties in this category are deprecated and replaced with corresponding "
          + "`manager.*` properties. The old `master.*` names can still be used until at release 3.0, but a warning "
          + "will be emitted. Configuration files should be updated to use the new property names."),
  MANAGER_CLIENTPORT("manager.port.client", "9999", PropertyType.PORT,
      "The port used for handling client connections on the manager"),
  MANAGER_TABLET_BALANCER("manager.tablet.balancer",
      "org.apache.accumulo.core.spi.balancer.TableLoadBalancer", PropertyType.CLASSNAME,
      "The balancer class that accumulo will use to make tablet assignment and "
          + "migration decisions."),
  MANAGER_BULK_RETRIES("manager.bulk.retries", "3", PropertyType.COUNT,
      "The number of attempts to bulk import a RFile before giving up."),
  MANAGER_BULK_THREADPOOL_SIZE("manager.bulk.threadpool.size", "5", PropertyType.COUNT,
      "The number of threads to use when coordinating a bulk import."),
  MANAGER_BULK_THREADPOOL_TIMEOUT("manager.bulk.threadpool.timeout", "0s",
      PropertyType.TIMEDURATION,
      "The time after which bulk import threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely."),
  MANAGER_BULK_TIMEOUT("manager.bulk.timeout", "5m", PropertyType.TIMEDURATION,
      "The time to wait for a tablet server to process a bulk import request"),
  MANAGER_RENAME_THREADS("manager.rename.threadpool.size", "20", PropertyType.COUNT,
      "The number of threads to use when renaming user files during table import or bulk ingest."),
  @Deprecated(since = "2.1.0")
  @ReplacedBy(property = MANAGER_RENAME_THREADS)
  MANAGER_BULK_RENAME_THREADS("manager.bulk.rename.threadpool.size", "20", PropertyType.COUNT,
      "This property is deprecated since 2.1.0. The number of threads to use when moving user files to bulk ingest "
          + "directories under accumulo control"),
  MANAGER_BULK_TSERVER_REGEX("manager.bulk.tserver.regex", "", PropertyType.STRING,
      "Regular expression that defines the set of Tablet Servers that will perform bulk imports"),
  MANAGER_MINTHREADS("manager.server.threads.minimum", "20", PropertyType.COUNT,
      "The minimum number of threads to use to handle incoming requests."),
  MANAGER_MINTHREADS_TIMEOUT("manager.server.threads.timeout", "0s", PropertyType.TIMEDURATION,
      "The time after which incoming request threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely."),
  MANAGER_THREADCHECK("manager.server.threadcheck.time", "1s", PropertyType.TIMEDURATION,
      "The time between adjustments of the server thread pool."),
  MANAGER_RECOVERY_DELAY("manager.recovery.delay", "10s", PropertyType.TIMEDURATION,
      "When a tablet server's lock is deleted, it takes time for it to "
          + "completely quit. This delay gives it time before log recoveries begin."),
  MANAGER_LEASE_RECOVERY_WAITING_PERIOD("manager.lease.recovery.interval", "5s",
      PropertyType.TIMEDURATION,
      "The amount of time to wait after requesting a write-ahead log to be recovered"),
  MANAGER_WALOG_CLOSER_IMPLEMETATION("manager.walog.closer.implementation",
      "org.apache.accumulo.server.manager.recovery.HadoopLogCloser", PropertyType.CLASSNAME,
      "A class that implements a mechanism to steal write access to a write-ahead log"),
  MANAGER_FATE_METRICS_ENABLED("manager.fate.metrics.enabled", "true", PropertyType.BOOLEAN,
      "Enable reporting of FATE metrics in JMX (and logging with Hadoop Metrics2"),
  MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL("manager.fate.metrics.min.update.interval", "60s",
      PropertyType.TIMEDURATION, "Limit calls from metric sinks to zookeeper to update interval"),
  MANAGER_FATE_THREADPOOL_SIZE("manager.fate.threadpool.size", "4", PropertyType.COUNT,
      "The number of threads used to run fault-tolerant executions (FATE)."
          + " These are primarily table operations like merge."),
  MANAGER_REPLICATION_SCAN_INTERVAL("manager.replication.status.scan.interval", "30s",
      PropertyType.TIMEDURATION,
      "Amount of time to sleep before scanning the status section of the "
          + "replication table for new data"),
  MANAGER_REPLICATION_COORDINATOR_PORT("manager.replication.coordinator.port", "10001",
      PropertyType.PORT, "Port for the replication coordinator service"),
  MANAGER_REPLICATION_COORDINATOR_MINTHREADS("manager.replication.coordinator.minthreads", "4",
      PropertyType.COUNT, "Minimum number of threads dedicated to answering coordinator requests"),
  MANAGER_REPLICATION_COORDINATOR_THREADCHECK("manager.replication.coordinator.threadcheck.time",
      "5s", PropertyType.TIMEDURATION,
      "The time between adjustments of the coordinator thread pool"),
  MANAGER_STATUS_THREAD_POOL_SIZE("manager.status.threadpool.size", "0", PropertyType.COUNT,
      "The number of threads to use when fetching the tablet server status for balancing.  Zero "
          + "indicates an unlimited number of threads will be used."),
  MANAGER_METADATA_SUSPENDABLE("manager.metadata.suspendable", "false", PropertyType.BOOLEAN,
      "Allow tablets for the " + MetadataTable.NAME
          + " table to be suspended via table.suspend.duration."),
  MANAGER_STARTUP_TSERVER_AVAIL_MIN_COUNT("manager.startup.tserver.avail.min.count", "0",
      PropertyType.COUNT,
      "Minimum number of tservers that need to be registered before manager will "
          + "start tablet assignment - checked at manager initialization, when manager gets lock. "
          + " When set to 0 or less, no blocking occurs. Default is 0 (disabled) to keep original "
          + " behaviour. Added with version 1.10"),
  MANAGER_STARTUP_TSERVER_AVAIL_MAX_WAIT("manager.startup.tserver.avail.max.wait", "0",
      PropertyType.TIMEDURATION,
      "Maximum time manager will wait for tserver available threshold "
          + "to be reached before continuing. When set to 0 or less, will block "
          + "indefinitely. Default is 0 to block indefinitely. Only valid when tserver available "
          + "threshold is set greater than 0. Added with version 1.10"),
  // properties that are specific to tablet server behavior
  TSERV_PREFIX("tserver.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the tablet servers"),
  TSERV_CLIENT_TIMEOUT("tserver.client.timeout", "3s", PropertyType.TIMEDURATION,
      "Time to wait for clients to continue scans before closing a session."),
  TSERV_DEFAULT_BLOCKSIZE("tserver.default.blocksize", "1M", PropertyType.BYTES,
      "Specifies a default blocksize for the tserver caches"),
  TSERV_CACHE_MANAGER_IMPL("tserver.cache.manager.class",
      "org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager", PropertyType.STRING,
      "Specifies the class name of the block cache factory implementation."
          + " Alternative implementation is"
          + " org.apache.accumulo.core.file.blockfile.cache.tinylfu.TinyLfuBlockCacheManager"),
  TSERV_DATACACHE_SIZE("tserver.cache.data.size", "10%", PropertyType.MEMORY,
      "Specifies the size of the cache for RFile data blocks."),
  TSERV_INDEXCACHE_SIZE("tserver.cache.index.size", "25%", PropertyType.MEMORY,
      "Specifies the size of the cache for RFile index blocks."),
  TSERV_SUMMARYCACHE_SIZE("tserver.cache.summary.size", "10%", PropertyType.MEMORY,
      "Specifies the size of the cache for summary data on each tablet server."),
  TSERV_PORTSEARCH("tserver.port.search", "false", PropertyType.BOOLEAN,
      "if the ports above are in use, search higher ports until one is available"),
  TSERV_CLIENTPORT("tserver.port.client", "9997", PropertyType.PORT,
      "The port used for handling client connections on the tablet servers"),
  TSERV_TOTAL_MUTATION_QUEUE_MAX("tserver.total.mutation.queue.max", "5%", PropertyType.MEMORY,
      "The amount of memory used to store write-ahead-log mutations before flushing them."),
  TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN("tserver.tablet.split.midpoint.files.max", "300",
      PropertyType.COUNT,
      "To find a tablets split points, all RFiles are opened and their indexes"
          + " are read. This setting determines how many RFiles can be opened at once."
          + " When there are more RFiles than this setting multiple passes must be"
          + " made, which is slower. However opening too many RFiles at once can cause"
          + " problems."),
  TSERV_WALOG_MAX_REFERENCED("tserver.walog.max.referenced", "3", PropertyType.COUNT,
      "When a tablet server has more than this many write ahead logs, any tablet referencing older "
          + "logs over this threshold is minor compacted.  Also any tablet referencing this many "
          + "logs or more will be compacted."),
  TSERV_WALOG_MAX_SIZE("tserver.walog.max.size", "1G", PropertyType.BYTES,
      "The maximum size for each write-ahead log. See comment for property"
          + " tserver.memory.maps.max"),
  TSERV_WALOG_MAX_AGE("tserver.walog.max.age", "24h", PropertyType.TIMEDURATION,
      "The maximum age for each write-ahead log."),
  TSERV_WALOG_TOLERATED_CREATION_FAILURES("tserver.walog.tolerated.creation.failures", "50",
      PropertyType.COUNT,
      "The maximum number of failures tolerated when creating a new write-ahead"
          + " log. Negative values will allow unlimited creation failures. Exceeding this"
          + " number of failures consecutively trying to create a new write-ahead log"
          + " causes the TabletServer to exit."),
  TSERV_WALOG_TOLERATED_WAIT_INCREMENT("tserver.walog.tolerated.wait.increment", "1000ms",
      PropertyType.TIMEDURATION,
      "The amount of time to wait between failures to create or write a write-ahead log."),
  // Never wait longer than 5 mins for a retry
  TSERV_WALOG_TOLERATED_MAXIMUM_WAIT_DURATION("tserver.walog.maximum.wait.duration", "5m",
      PropertyType.TIMEDURATION,
      "The maximum amount of time to wait after a failure to create or write a write-ahead log."),
  TSERV_SCAN_MAX_OPENFILES("tserver.scan.files.open.max", "100", PropertyType.COUNT,
      "Maximum total RFiles that all tablets in a tablet server can open for scans. "),
  TSERV_MAX_IDLE("tserver.files.open.idle", "1m", PropertyType.TIMEDURATION,
      "Tablet servers leave previously used RFiles open for future queries."
          + " This setting determines how much time an unused RFile should be kept open"
          + " until it is closed."),
  TSERV_NATIVEMAP_ENABLED("tserver.memory.maps.native.enabled", "true", PropertyType.BOOLEAN,
      "An in-memory data store for accumulo implemented in c++ that increases"
          + " the amount of data accumulo can hold in memory and avoids Java GC" + " pauses."),
  TSERV_MAXMEM("tserver.memory.maps.max", "33%", PropertyType.MEMORY,
      "Maximum amount of memory that can be used to buffer data written to a"
          + " tablet server. There are two other properties that can effectively limit"
          + " memory usage table.compaction.minor.logs.threshold and"
          + " tserver.walog.max.size. Ensure that table.compaction.minor.logs.threshold"
          + " * tserver.walog.max.size >= this property."),
  TSERV_SESSION_MAXIDLE("tserver.session.idle.max", "1m", PropertyType.TIMEDURATION,
      "When a tablet server's SimpleTimer thread triggers to check idle"
          + " sessions, this configurable option will be used to evaluate scan sessions"
          + " to determine if they can be closed due to inactivity"),
  TSERV_UPDATE_SESSION_MAXIDLE("tserver.session.update.idle.max", "1m", PropertyType.TIMEDURATION,
      "When a tablet server's SimpleTimer thread triggers to check idle"
          + " sessions, this configurable option will be used to evaluate update"
          + " sessions to determine if they can be closed due to inactivity"),
  TSERV_SCAN_EXECUTORS_PREFIX("tserver.scan.executors.", null, PropertyType.PREFIX,
      "Prefix for defining executors to service scans. See "
          + "[scan executors]({% durl administration/scan-executors %}) for an overview of why and"
          + " how to use this property. For each executor the number of threads, thread priority, "
          + "and an optional prioritizer can be configured. To configure a new executor, set "
          + "`tserver.scan.executors.<name>.threads=<number>`.  Optionally, can also set "
          + "`tserver.scan.executors.<name>.priority=<number 1 to 10>`, "
          + "`tserver.scan.executors.<name>.prioritizer=<class name>`, and "
          + "`tserver.scan.executors.<name>.prioritizer.opts.<key>=<value>`"),
  TSERV_SCAN_EXECUTORS_DEFAULT_THREADS("tserver.scan.executors.default.threads", "16",
      PropertyType.COUNT,
      "The number of threads for the scan executor that tables use by default."),
  TSERV_SCAN_EXECUTORS_DEFAULT_PRIORITIZER("tserver.scan.executors.default.prioritizer", "",
      PropertyType.STRING,
      "Prioritizer for the default scan executor.  Defaults to none which "
          + "results in FIFO priority.  Set to a class that implements "
          + ScanPrioritizer.class.getName() + " to configure one."),
  TSERV_SCAN_EXECUTORS_META_THREADS("tserver.scan.executors.meta.threads", "8", PropertyType.COUNT,
      "The number of threads for the metadata table scan executor."),
  TSERV_MIGRATE_MAXCONCURRENT("tserver.migrations.concurrent.max", "1", PropertyType.COUNT,
      "The maximum number of concurrent tablet migrations for a tablet server"),
  TSERV_MAJC_DELAY("tserver.compaction.major.delay", "30s", PropertyType.TIMEDURATION,
      "Time a tablet server will sleep between checking which tablets need compaction."),
  TSERV_COMPACTION_SERVICE_PREFIX("tserver.compaction.major.service.", null, PropertyType.PREFIX,
      "Prefix for compaction services."),
  TSERV_COMPACTION_SERVICE_ROOT_PLANNER("tserver.compaction.major.service.root.planner",
      DefaultCompactionPlanner.class.getName(), PropertyType.CLASSNAME,
      "Compaction planner for root tablet service"),
  TSERV_COMPACTION_SERVICE_ROOT_RATE_LIMIT("tserver.compaction.major.service.root.rate.limit", "0B",
      PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major"
          + " compactions in this compaction service, or 0B for unlimited."),
  TSERV_COMPACTION_SERVICE_ROOT_MAX_OPEN(
      "tserver.compaction.major.service.root.planner.opts.maxOpen", "30", PropertyType.COUNT,
      "The maximum number of files a compaction will open"),
  TSERV_COMPACTION_SERVICE_ROOT_EXECUTORS(
      "tserver.compaction.major.service.root.planner.opts.executors",
      "[{'name':'small','maxSize':'32M','numThreads':1},"
          + "{'name':'huge','numThreads':1}]".replaceAll("'", "\""),
      PropertyType.STRING,
      "See {% jlink -f org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner %} "),
  TSERV_COMPACTION_SERVICE_META_PLANNER("tserver.compaction.major.service.meta.planner",
      DefaultCompactionPlanner.class.getName(), PropertyType.CLASSNAME,
      "Compaction planner for metadata table"),
  TSERV_COMPACTION_SERVICE_META_RATE_LIMIT("tserver.compaction.major.service.meta.rate.limit", "0B",
      PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major"
          + " compactions in this compaction service, or 0B for unlimited."),
  TSERV_COMPACTION_SERVICE_META_MAX_OPEN(
      "tserver.compaction.major.service.meta.planner.opts.maxOpen", "30", PropertyType.COUNT,
      "The maximum number of files a compaction will open"),
  TSERV_COMPACTION_SERVICE_META_EXECUTORS(
      "tserver.compaction.major.service.meta.planner.opts.executors",
      "[{'name':'small','maxSize':'32M','numThreads':2},"
          + "{'name':'huge','numThreads':2}]".replaceAll("'", "\""),
      PropertyType.STRING,
      "See {% jlink -f org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner %} "),
  TSERV_COMPACTION_SERVICE_DEFAULT_PLANNER("tserver.compaction.major.service.default.planner",
      DefaultCompactionPlanner.class.getName(), PropertyType.CLASSNAME,
      "Planner for default compaction service."),
  TSERV_COMPACTION_SERVICE_DEFAULT_RATE_LIMIT("tserver.compaction.major.service.default.rate.limit",
      "0B", PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major"
          + " compactions in this compaction service, or 0B for unlimited."),
  TSERV_COMPACTION_SERVICE_DEFAULT_MAX_OPEN(
      "tserver.compaction.major.service.default.planner.opts.maxOpen", "10", PropertyType.COUNT,
      "The maximum number of files a compaction will open"),
  TSERV_COMPACTION_SERVICE_DEFAULT_EXECUTORS(
      "tserver.compaction.major.service.default.planner.opts.executors",
      "[{'name':'small','maxSize':'32M','numThreads':2},"
          + "{'name':'medium','maxSize':'128M','numThreads':2},"
          + "{'name':'large','numThreads':2}]".replaceAll("'", "\""),
      PropertyType.STRING,
      "See {% jlink -f org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner %} "),
  @Deprecated(since = "2.1.0", forRemoval = true)
  @ReplacedBy(property = Property.TSERV_COMPACTION_SERVICE_DEFAULT_MAX_OPEN)
  TSERV_MAJC_THREAD_MAXOPEN("tserver.compaction.major.thread.files.open.max", "10",
      PropertyType.COUNT, "Max number of RFiles a major compaction thread can open at once. "),
  @Deprecated(since = "2.1.0", forRemoval = true)
  @ReplacedBy(property = Property.TSERV_COMPACTION_SERVICE_DEFAULT_EXECUTORS)
  TSERV_MAJC_MAXCONCURRENT("tserver.compaction.major.concurrent.max", "3", PropertyType.COUNT,
      "The maximum number of concurrent major compactions for a tablet server"),
  @Deprecated(since = "2.1.0", forRemoval = true)
  @ReplacedBy(property = Property.TSERV_COMPACTION_SERVICE_DEFAULT_RATE_LIMIT)
  TSERV_MAJC_THROUGHPUT("tserver.compaction.major.throughput", "0B", PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major"
          + " compactions within each compaction service, or 0B for unlimited."),
  TSERV_MINC_MAXCONCURRENT("tserver.compaction.minor.concurrent.max", "4", PropertyType.COUNT,
      "The maximum number of concurrent minor compactions for a tablet server"),
  TSERV_MAJC_TRACE_PERCENT("tserver.compaction.major.trace.percent", "0.1", PropertyType.FRACTION,
      "The percent of major compactions to trace"),
  TSERV_MINC_TRACE_PERCENT("tserver.compaction.minor.trace.percent", "0.1", PropertyType.FRACTION,
      "The percent of minor compactions to trace"),
  TSERV_COMPACTION_WARN_TIME("tserver.compaction.warn.time", "10m", PropertyType.TIMEDURATION,
      "When a compaction has not made progress for this time period, a warning will be logged"),
  TSERV_BLOOM_LOAD_MAXCONCURRENT("tserver.bloom.load.concurrent.max", "4", PropertyType.COUNT,
      "The number of concurrent threads that will load bloom filters in the background. "
          + "Setting this to zero will make bloom filters load in the foreground."),
  TSERV_MONITOR_FS("tserver.monitor.fs", "false", PropertyType.BOOLEAN,
      "When enabled the tserver will monitor file systems and kill itself when"
          + " one switches from rw to ro. This is usually and indication that Linux has"
          + " detected a bad disk."),
  TSERV_MEMDUMP_DIR("tserver.dir.memdump", "/tmp", PropertyType.PATH,
      "A long running scan could possibly hold memory that has been minor"
          + " compacted. To prevent this, the in memory map is dumped to a local file"
          + " and the scan is switched to that local file. We can not switch to the"
          + " minor compacted file because it may have been modified by iterators. The"
          + " file dumped to the local dir is an exact copy of what was in memory."),
  TSERV_BULK_PROCESS_THREADS("tserver.bulk.process.threads", "1", PropertyType.COUNT,
      "The manager will task a tablet server with pre-processing a bulk import"
          + " RFile prior to assigning it to the appropriate tablet servers. This"
          + " configuration value controls the number of threads used to process the" + " files."),
  TSERV_BULK_ASSIGNMENT_THREADS("tserver.bulk.assign.threads", "1", PropertyType.COUNT,
      "The manager delegates bulk import RFile processing and assignment to"
          + " tablet servers. After file has been processed, the tablet server will"
          + " assign the file to the appropriate tablets on all servers. This property"
          + " controls the number of threads used to communicate to the other" + " servers."),
  TSERV_BULK_RETRY("tserver.bulk.retry.max", "5", PropertyType.COUNT,
      "The number of times the tablet server will attempt to assign a RFile to"
          + " a tablet as it migrates and splits."),
  TSERV_BULK_TIMEOUT("tserver.bulk.timeout", "5m", PropertyType.TIMEDURATION,
      "The time to wait for a tablet server to process a bulk import request."),
  TSERV_MINTHREADS("tserver.server.threads.minimum", "20", PropertyType.COUNT,
      "The minimum number of threads to use to handle incoming requests."),
  TSERV_MINTHREADS_TIMEOUT("tserver.server.threads.timeout", "0s", PropertyType.TIMEDURATION,
      "The time after which incoming request threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely."),
  TSERV_THREADCHECK("tserver.server.threadcheck.time", "1s", PropertyType.TIMEDURATION,
      "The time between adjustments of the server thread pool."),
  TSERV_MAX_MESSAGE_SIZE("tserver.server.message.size.max", "1G", PropertyType.BYTES,
      "The maximum size of a message that can be sent to a tablet server."),
  TSERV_LOG_BUSY_TABLETS_COUNT("tserver.log.busy.tablets.count", "0", PropertyType.COUNT,
      "Number of busiest tablets to log. Logged at interval controlled by "
          + "tserver.log.busy.tablets.interval. If <= 0, logging of busy tablets is disabled"),
  TSERV_LOG_BUSY_TABLETS_INTERVAL("tserver.log.busy.tablets.interval", "1h",
      PropertyType.TIMEDURATION, "Time interval between logging out busy tablets information."),
  TSERV_HOLD_TIME_SUICIDE("tserver.hold.time.max", "5m", PropertyType.TIMEDURATION,
      "The maximum time for a tablet server to be in the \"memory full\" state."
          + " If the tablet server cannot write out memory in this much time, it will"
          + " assume there is some failure local to its node, and quit. A value of zero"
          + " is equivalent to forever."),
  TSERV_WAL_BLOCKSIZE("tserver.wal.blocksize", "0", PropertyType.BYTES,
      "The size of the HDFS blocks used to write to the Write-Ahead log. If"
          + " zero, it will be 110% of tserver.walog.max.size (that is, try to use just"
          + " one block)"),
  TSERV_WAL_REPLICATION("tserver.wal.replication", "0", PropertyType.COUNT,
      "The replication to use when writing the Write-Ahead log to HDFS. If"
          + " zero, it will use the HDFS default replication setting."),
  TSERV_RECOVERY_MAX_CONCURRENT("tserver.recovery.concurrent.max", "2", PropertyType.COUNT,
      "The maximum number of threads to use to sort logs during" + " recovery"),
  TSERV_SORT_BUFFER_SIZE("tserver.sort.buffer.size", "10%", PropertyType.MEMORY,
      "The amount of memory to use when sorting logs during recovery."),
  TSERV_WORKQ_THREADS("tserver.workq.threads", "2", PropertyType.COUNT,
      "The number of threads for the distributed work queue. These threads are"
          + " used for copying failed bulk import RFiles."),
  TSERV_WAL_SYNC("tserver.wal.sync", "true", PropertyType.BOOLEAN,
      "Use the SYNC_BLOCK create flag to sync WAL writes to disk. Prevents"
          + " problems recovering from sudden system resets."),
  TSERV_ASSIGNMENT_DURATION_WARNING("tserver.assignment.duration.warning", "10m",
      PropertyType.TIMEDURATION,
      "The amount of time an assignment can run before the server will print a"
          + " warning along with the current stack trace. Meant to help debug stuck"
          + " assignments"),
  TSERV_REPLICATION_REPLAYERS("tserver.replication.replayer.", null, PropertyType.PREFIX,
      "Allows configuration of implementation used to apply replicated data"),
  TSERV_REPLICATION_DEFAULT_HANDLER("tserver.replication.default.replayer",
      "org.apache.accumulo.tserver.replication.BatchWriterReplicationReplayer",
      PropertyType.CLASSNAME, "Default AccumuloReplicationReplayer implementation"),
  TSERV_REPLICATION_BW_REPLAYER_MEMORY("tserver.replication.batchwriter.replayer.memory", "50M",
      PropertyType.BYTES, "Memory to provide to batchwriter to replay mutations for replication"),
  TSERV_ASSIGNMENT_MAXCONCURRENT("tserver.assignment.concurrent.max", "2", PropertyType.COUNT,
      "The number of threads available to load tablets. Recoveries are still performed serially."),
  TSERV_SLOW_FLUSH_MILLIS("tserver.slow.flush.time", "100ms", PropertyType.TIMEDURATION,
      "If a flush to the write-ahead log takes longer than this period of time,"
          + " debugging information will written, and may result in a log rollover."),
  TSERV_SLOW_FILEPERMIT_MILLIS("tserver.slow.filepermit.time", "100ms", PropertyType.TIMEDURATION,
      "If a thread blocks more than this period of time waiting to get file permits,"
          + " debugging information will be written."),
  TSERV_SUMMARY_PARTITION_THREADS("tserver.summary.partition.threads", "10", PropertyType.COUNT,
      "Summary data must be retrieved from RFiles. For a large number of"
          + " RFiles, the files are broken into partitions of 100k files. This setting"
          + " determines how many of these groups of 100k RFiles will be processed"
          + " concurrently."),
  TSERV_SUMMARY_REMOTE_THREADS("tserver.summary.remote.threads", "128", PropertyType.COUNT,
      "For a partitioned group of 100k RFiles, those files are grouped by"
          + " tablet server. Then a remote tablet server is asked to gather summary"
          + " data. This setting determines how many concurrent request are made per"
          + " partition."),
  TSERV_SUMMARY_RETRIEVAL_THREADS("tserver.summary.retrieval.threads", "10", PropertyType.COUNT,
      "The number of threads on each tablet server available to retrieve"
          + " summary data, that is not currently in cache, from RFiles."),

  // accumulo garbage collector properties
  GC_PREFIX("gc.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the accumulo garbage collector."),
  GC_CANDIDATE_BATCH_SIZE("gc.candidate.batch.size", "8M", PropertyType.BYTES,
      "The batch size used for garbage collection."),
  GC_CYCLE_START("gc.cycle.start", "30s", PropertyType.TIMEDURATION,
      "Time to wait before attempting to garbage collect any old RFiles or write-ahead logs."),
  GC_CYCLE_DELAY("gc.cycle.delay", "5m", PropertyType.TIMEDURATION,
      "Time between garbage collection cycles. In each cycle, old RFiles or write-ahead logs "
          + "no longer in use are removed from the filesystem."),
  GC_PORT("gc.port.client", "9998", PropertyType.PORT,
      "The listening port for the garbage collector's monitor service"),
  GC_DELETE_THREADS("gc.threads.delete", "16", PropertyType.COUNT,
      "The number of threads used to delete RFiles and write-ahead logs"),
  GC_TRASH_IGNORE("gc.trash.ignore", "false", PropertyType.BOOLEAN,
      "Do not use the Trash, even if it is configured."),
  GC_TRACE_PERCENT("gc.trace.percent", "0.01", PropertyType.FRACTION,
      "Percent of gc cycles to trace"),
  GC_SAFEMODE("gc.safemode", "false", PropertyType.BOOLEAN,
      "Provides listing of files to be deleted but does not delete any files"),
  GC_USE_FULL_COMPACTION("gc.post.metadata.action", "flush", PropertyType.GC_POST_ACTION,
      "When the gc runs it can make a lot of changes to the metadata, on completion, "
          + " to force the changes to be written to disk, the metadata and root tables can be flushed"
          + " and possibly compacted. Legal values are: compact - which both flushes and compacts the"
          + " metadata; flush - which flushes only (compactions may be triggered if required); or none"),
  GC_METRICS_ENABLED("gc.metrics.enabled", "true", PropertyType.BOOLEAN,
      "Enable detailed gc metrics reporting with hadoop metrics."),

  // properties that are specific to the monitor server behavior
  MONITOR_PREFIX("monitor.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the monitor web server."),
  MONITOR_PORT("monitor.port.client", "9995", PropertyType.PORT,
      "The listening port for the monitor's http service"),
  MONITOR_SSL_KEYSTORE("monitor.ssl.keyStore", "", PropertyType.PATH,
      "The keystore for enabling monitor SSL."),
  @Sensitive
  MONITOR_SSL_KEYSTOREPASS("monitor.ssl.keyStorePassword", "", PropertyType.STRING,
      "The keystore password for enabling monitor SSL."),
  MONITOR_SSL_KEYSTORETYPE("monitor.ssl.keyStoreType", "jks", PropertyType.STRING,
      "Type of SSL keystore"),
  @Sensitive
  MONITOR_SSL_KEYPASS("monitor.ssl.keyPassword", "", PropertyType.STRING,
      "Optional: the password for the private key in the keyStore. When not provided, this "
          + "defaults to the keystore password."),
  MONITOR_SSL_TRUSTSTORE("monitor.ssl.trustStore", "", PropertyType.PATH,
      "The truststore for enabling monitor SSL."),
  @Sensitive
  MONITOR_SSL_TRUSTSTOREPASS("monitor.ssl.trustStorePassword", "", PropertyType.STRING,
      "The truststore password for enabling monitor SSL."),
  MONITOR_SSL_TRUSTSTORETYPE("monitor.ssl.trustStoreType", "jks", PropertyType.STRING,
      "Type of SSL truststore"),
  MONITOR_SSL_INCLUDE_CIPHERS("monitor.ssl.include.ciphers", "", PropertyType.STRING,
      "A comma-separated list of allows SSL Ciphers, see"
          + " monitor.ssl.exclude.ciphers to disallow ciphers"),
  MONITOR_SSL_EXCLUDE_CIPHERS("monitor.ssl.exclude.ciphers", "", PropertyType.STRING,
      "A comma-separated list of disallowed SSL Ciphers, see"
          + " monitor.ssl.include.ciphers to allow ciphers"),
  MONITOR_SSL_INCLUDE_PROTOCOLS("monitor.ssl.include.protocols", "TLSv1.2", PropertyType.STRING,
      "A comma-separate list of allowed SSL protocols"),
  MONITOR_LOCK_CHECK_INTERVAL("monitor.lock.check.interval", "5s", PropertyType.TIMEDURATION,
      "The amount of time to sleep between checking for the Montior ZooKeeper lock"),
  MONITOR_RESOURCES_EXTERNAL("monitor.resources.external", "", PropertyType.STRING,
      "A JSON Map of Strings. Each String should be an HTML tag of an external"
          + " resource (JS or CSS) to be imported by the Monitor. Be sure to wrap"
          + " with CDATA tags. If this value is set, all of the external resources"
          + " in the `<head>` tag of the Monitor will be replaced with the tags set here."
          + " Be sure the jquery tag is first since other scripts will depend on it."
          + " The resources that are used by default can be seen in"
          + " accumulo/server/monitor/src/main/resources/templates/default.ftl"),

  TRACE_PREFIX("trace.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of distributed tracing."),
  TRACE_SPAN_RECEIVERS("trace.span.receivers", "org.apache.accumulo.tracer.ZooTraceClient",
      PropertyType.CLASSNAMELIST, "A list of span receiver classes to send trace spans"),
  TRACE_SPAN_RECEIVER_PREFIX("trace.span.receiver.", null, PropertyType.PREFIX,
      "Prefix for span receiver configuration properties"),
  TRACE_ZK_PATH("trace.zookeeper.path", Constants.ZTRACERS, PropertyType.STRING,
      "The zookeeper node where tracers are registered"),
  TRACE_PORT("trace.port.client", "12234", PropertyType.PORT,
      "The listening port for the trace server"),
  TRACE_TABLE("trace.table", "trace", PropertyType.STRING,
      "The name of the table to store distributed traces"),
  TRACE_USER("trace.user", "root", PropertyType.STRING,
      "The name of the user to store distributed traces"),
  @Sensitive
  TRACE_PASSWORD("trace.password", "secret", PropertyType.STRING,
      "The password for the user used to store distributed traces"),
  @Sensitive
  TRACE_TOKEN_PROPERTY_PREFIX("trace.token.property.", null, PropertyType.PREFIX,
      "The prefix used to create a token for storing distributed traces. For"
          + " each property required by trace.token.type, place this prefix in front of it."),
  TRACE_TOKEN_TYPE("trace.token.type", PasswordToken.class.getName(), PropertyType.CLASSNAME,
      "An AuthenticationToken type supported by the authorizer"),

  // per table properties
  TABLE_PREFIX("table.", null, PropertyType.PREFIX,
      "Properties in this category affect tablet server treatment of tablets,"
          + " but can be configured on a per-table basis. Setting these properties in"
          + " accumulo.properties will override the default globally for all tables and not"
          + " any specific table. However, both the default and the global setting can"
          + " be overridden per table using the table operations API or in the shell,"
          + " which sets the overridden value in zookeeper. Restarting accumulo tablet"
          + " servers after setting these properties in accumulo.properties will cause the"
          + " global setting to take effect. However, you must use the API or the shell"
          + " to change properties in zookeeper that are set on a table."),
  TABLE_ARBITRARY_PROP_PREFIX("table.custom.", null, PropertyType.PREFIX,
      "Prefix to be used for user defined arbitrary properties."),
  TABLE_MAJC_RATIO("table.compaction.major.ratio", "3", PropertyType.FRACTION,
      "Minimum ratio of total input size to maximum input RFile size for"
          + " running a major compaction. "),
  @Deprecated(since = "2.1.0", forRemoval = true)
  TABLE_MAJC_COMPACTALL_IDLETIME("table.compaction.major.everything.idle", "1h",
      PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may"
          + " have all of its RFiles compacted into one. There is no guarantee an idle"
          + " tablet will be compacted. Compactions of idle tablets are only started"
          + " when regular compactions are not running. Idle compactions only take"
          + " place for tablets that have one or more RFiles."),
  TABLE_SPLIT_THRESHOLD("table.split.threshold", "1G", PropertyType.BYTES,
      "A tablet is split when the combined size of RFiles exceeds this amount."),
  TABLE_MAX_END_ROW_SIZE("table.split.endrow.size.max", "10k", PropertyType.BYTES,
      "Maximum size of end row"),
  @Deprecated(since = "2.0.0")
  @ReplacedBy(property = Property.TSERV_WALOG_MAX_REFERENCED)
  TABLE_MINC_LOGS_MAX("table.compaction.minor.logs.threshold", "3", PropertyType.COUNT,
      "This property is deprecated since 2.0.0."),
  TABLE_MINC_COMPACT_IDLETIME("table.compaction.minor.idle", "5m", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have its "
          + "in-memory map flushed to disk in a minor compaction. There is no guarantee an idle "
          + "tablet will be compacted."),
  TABLE_COMPACTION_DISPATCHER("table.compaction.dispatcher",
      SimpleCompactionDispatcher.class.getName(), PropertyType.CLASSNAME,
      "A configurable dispatcher that decides what comaction service a table should use."),
  TABLE_COMPACTION_DISPATCHER_OPTS("table.compaction.dispatcher.opts.", null, PropertyType.PREFIX,
      "Options for the table compaction dispatcher"),
  TABLE_COMPACTION_SELECTOR("table.compaction.selector", "", PropertyType.CLASSNAME,
      "A configurable selector for a table that can periodically select file for mandatory "
          + "compaction, even if the files do not meet the compaction ratio."),
  TABLE_COMPACTION_SELECTOR_OPTS("table.compaction.selector.opts.", null, PropertyType.PREFIX,
      "Options for the table compaction dispatcher"),
  TABLE_COMPACTION_CONFIGURER("table.compaction.configurer", "", PropertyType.CLASSNAME,
      "A plugin that can dynamically configure compaction output files based on input files."),
  TABLE_COMPACTION_CONFIGURER_OPTS("table.compaction.configurer.opts.", null, PropertyType.PREFIX,
      "Options for the table compaction configuror"),
  @Deprecated(since = "2.1.0", forRemoval = true)
  @ReplacedBy(property = TABLE_COMPACTION_SELECTOR)
  TABLE_COMPACTION_STRATEGY("table.majc.compaction.strategy",
      "org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy", PropertyType.CLASSNAME,
      "Deprecated since 2.1.0 See {% jlink -f org.apache.accumulo.core.spi.compaction}"),
  @Deprecated(since = "2.1.0", forRemoval = true)
  @ReplacedBy(property = TABLE_COMPACTION_SELECTOR_OPTS)
  TABLE_COMPACTION_STRATEGY_PREFIX("table.majc.compaction.strategy.opts.", null,
      PropertyType.PREFIX,
      "Properties in this category are used to configure the compaction strategy."),
  TABLE_SCAN_DISPATCHER("table.scan.dispatcher", SimpleScanDispatcher.class.getName(),
      PropertyType.CLASSNAME,
      "This class is used to dynamically dispatch scans to configured scan executors.  Configured "
          + "classes must implement {% jlink " + ScanDispatcher.class.getName() + " %} See "
          + "[scan executors]({% durl administration/scan-executors %}) for an overview of why"
          + " and how to use this property. This property is ignored for the root and metadata"
          + " table.  The metadata table always dispatches to a scan executor named `meta`."),
  TABLE_SCAN_DISPATCHER_OPTS("table.scan.dispatcher.opts.", null, PropertyType.PREFIX,
      "Options for the table scan dispatcher"),
  TABLE_SCAN_MAXMEM("table.scan.max.memory", "512k", PropertyType.BYTES,
      "The maximum amount of memory that will be used to cache results of a client query/scan. "
          + "Once this limit is reached, the buffered data is sent to the client."),
  TABLE_FILE_TYPE("table.file.type", RFile.EXTENSION, PropertyType.STRING,
      "Change the type of file a table writes"),
  TABLE_LOAD_BALANCER("table.balancer", "org.apache.accumulo.core.spi.balancer.SimpleLoadBalancer",
      PropertyType.STRING,
      "This property can be set to allow the LoadBalanceByTable load balancer"
          + " to change the called Load Balancer for this table"),
  TABLE_FILE_COMPRESSION_TYPE("table.file.compress.type", "gz", PropertyType.STRING,
      "Compression algorithm used on index and data blocks before they are"
          + " written. Possible values: zstd, gz, snappy, lzo, none"),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE("table.file.compress.blocksize", "100k", PropertyType.BYTES,
      "The maximum size of data blocks in RFiles before they are compressed and written."),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX("table.file.compress.blocksize.index", "128k",
      PropertyType.BYTES,
      "The maximum size of index blocks in RFiles before they are compressed and written."),
  TABLE_FILE_BLOCK_SIZE("table.file.blocksize", "0B", PropertyType.BYTES,
      "The HDFS block size used when writing RFiles. When set to 0B, the"
          + " value/defaults of HDFS property 'dfs.block.size' will be used."),
  TABLE_FILE_REPLICATION("table.file.replication", "0", PropertyType.COUNT,
      "The number of replicas for a table's RFiles in HDFS. When set to 0, HDFS"
          + " defaults are used."),
  TABLE_FILE_MAX("table.file.max", "15", PropertyType.COUNT,
      "The maximum number of RFiles each tablet in a table can have. When"
          + " adjusting this property you may want to consider adjusting"
          + " table.compaction.major.ratio also. Setting this property to 0 will make"
          + " it default to tserver.scan.files.open.max-1, this will prevent a tablet"
          + " from having more RFiles than can be opened. Setting this property low may"
          + " throttle ingest and increase query performance."),
  TABLE_FILE_SUMMARY_MAX_SIZE("table.file.summary.maxSize", "256k", PropertyType.BYTES,
      "The maximum size summary that will be stored. The number of RFiles that"
          + " had summary data exceeding this threshold is reported by"
          + " Summary.getFileStatistics().getLarge(). When adjusting this consider the"
          + " expected number RFiles with summaries on each tablet server and the"
          + " summary cache size."),
  TABLE_BLOOM_ENABLED("table.bloom.enabled", "false", PropertyType.BOOLEAN,
      "Use bloom filters on this table."),
  TABLE_BLOOM_LOAD_THRESHOLD("table.bloom.load.threshold", "1", PropertyType.COUNT,
      "This number of seeks that would actually use a bloom filter must occur"
          + " before a RFile's bloom filter is loaded. Set this to zero to initiate"
          + " loading of bloom filters when a RFile is opened."),
  TABLE_BLOOM_SIZE("table.bloom.size", "1048576", PropertyType.COUNT,
      "Bloom filter size, as number of keys."),
  TABLE_BLOOM_ERRORRATE("table.bloom.error.rate", "0.5%", PropertyType.FRACTION,
      "Bloom filter error rate."),
  TABLE_BLOOM_KEY_FUNCTOR("table.bloom.key.functor",
      "org.apache.accumulo.core.file.keyfunctor.RowFunctor", PropertyType.CLASSNAME,
      "A function that can transform the key prior to insertion and check of"
          + " bloom filter. org.apache.accumulo.core.file.keyfunctor.RowFunctor,"
          + " org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor, and"
          + " org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor are"
          + " allowable values. One can extend any of the above mentioned classes to"
          + " perform specialized parsing of the key. "),
  TABLE_BLOOM_HASHTYPE("table.bloom.hash.type", "murmur", PropertyType.STRING,
      "The bloom filter hash type"),
  TABLE_BULK_MAX_TABLETS("table.bulk.max.tablets", "0", PropertyType.COUNT,
      "The maximum number of tablets allowed for one bulk import file. Value of 0 is Unlimited. "
          + "This property is only enforced in the new bulk import API"),
  TABLE_DURABILITY("table.durability", "sync", PropertyType.DURABILITY,
      "The durability used to write to the write-ahead log. Legal values are:"
          + " none, which skips the write-ahead log; log, which sends the data to the"
          + " write-ahead log, but does nothing to make it durable; flush, which pushes"
          + " data to the file system; and sync, which ensures the data is written to disk."),

  TABLE_FAILURES_IGNORE("table.failures.ignore", "false", PropertyType.BOOLEAN,
      "If you want queries for your table to hang or fail when data is missing"
          + " from the system, then set this to false. When this set to true missing"
          + " data will be reported but queries will still run possibly returning a"
          + " subset of the data."),
  TABLE_DEFAULT_SCANTIME_VISIBILITY("table.security.scan.visibility.default", "",
      PropertyType.STRING,
      "The security label that will be assumed at scan time if an entry does"
          + " not have a visibility expression.\n"
          + "Note: An empty security label is displayed as []. The scan results"
          + " will show an empty visibility even if the visibility from this"
          + " setting is applied to the entry.\n"
          + "CAUTION: If a particular key has an empty security label AND its"
          + " table's default visibility is also empty, access will ALWAYS be"
          + " granted for users with permission to that table. Additionally, if this"
          + " field is changed, all existing data with an empty visibility label"
          + " will be interpreted with the new label on the next scan."),
  TABLE_LOCALITY_GROUPS("table.groups.enabled", "", PropertyType.STRING,
      "A comma separated list of locality group names to enable for this table."),
  TABLE_CONSTRAINT_PREFIX("table.constraint.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that add"
          + " constraints to a table. These properties start with the category"
          + " prefix, followed by a number, and their values correspond to a fully"
          + " qualified Java class that implements the Constraint interface.\n" + "For example:\n"
          + "table.constraint.1 = org.apache.accumulo.core.constraints.MyCustomConstraint\n"
          + "and:\n" + " table.constraint.2 = my.package.constraints.MySecondConstraint"),
  TABLE_INDEXCACHE_ENABLED("table.cache.index.enable", "true", PropertyType.BOOLEAN,
      "Determines whether index block cache is enabled for a table."),
  TABLE_BLOCKCACHE_ENABLED("table.cache.block.enable", "false", PropertyType.BOOLEAN,
      "Determines whether data block cache is enabled for a table."),
  TABLE_ITERATOR_PREFIX("table.iterator.", null, PropertyType.PREFIX,
      "Properties in this category specify iterators that are applied at"
          + " various stages (scopes) of interaction with a table. These properties"
          + " start with the category prefix, followed by a scope (minc, majc, scan,"
          + " etc.), followed by a period, followed by a name, as in"
          + " table.iterator.scan.vers, or table.iterator.scan.custom. The values for"
          + " these properties are a number indicating the ordering in which it is"
          + " applied, and a class name such as:\n"
          + "table.iterator.scan.vers = 10,org.apache.accumulo.core.iterators.VersioningIterator\n"
          + "These iterators can take options if additional properties are set that"
          + " look like this property, but are suffixed with a period, followed by 'opt'"
          + " followed by another period, and a property name.\n"
          + "For example, table.iterator.minc.vers.opt.maxVersions = 3"),
  TABLE_ITERATOR_SCAN_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.scan.name() + ".", null,
      PropertyType.PREFIX, "Convenience prefix to find options for the scan iterator scope"),
  TABLE_ITERATOR_MINC_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.minc.name() + ".", null,
      PropertyType.PREFIX, "Convenience prefix to find options for the minc iterator scope"),
  TABLE_ITERATOR_MAJC_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.majc.name() + ".", null,
      PropertyType.PREFIX, "Convenience prefix to find options for the majc iterator scope"),
  TABLE_LOCALITY_GROUP_PREFIX("table.group.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that define"
          + " locality groups in a table. These properties start with the category"
          + " prefix, followed by a name, followed by a period, and followed by a"
          + " property for that group.\n"
          + "For example table.group.group1=x,y,z sets the column families for a"
          + " group called group1. Once configured, group1 can be enabled by adding"
          + " it to the list of groups in the " + TABLE_LOCALITY_GROUPS.getKey() + " property.\n"
          + "Additional group options may be specified for a named group by setting"
          + " `table.group.<name>.opt.<key>=<value>`."),
  TABLE_FORMATTER_CLASS("table.formatter", DefaultFormatter.class.getName(), PropertyType.STRING,
      "The Formatter class to apply on results in the shell"),
  TABLE_INTERPRETER_CLASS("table.interepreter", DefaultScanInterpreter.class.getName(),
      PropertyType.STRING, "The ScanInterpreter class to apply on scan arguments in the shell"),
  TABLE_CLASSLOADER_CONTEXT("table.class.loader.context", "", PropertyType.STRING,
      "The context to use for loading per-table resources, such as iterators"
          + " from the configured factory in `general.context.class.loader.factory`."),
  @Deprecated(since = "2.1.0", forRemoval = true)
  @ReplacedBy(property = TABLE_CLASSLOADER_CONTEXT)
  TABLE_CLASSPATH("table.classpath.context", "", PropertyType.STRING,
      "Per table classpath context"),
  TABLE_REPLICATION("table.replication", "false", PropertyType.BOOLEAN,
      "Is replication enabled for the given table"),
  TABLE_REPLICATION_TARGET("table.replication.target.", null, PropertyType.PREFIX,
      "Enumerate a mapping of other systems which this table should replicate"
          + " their data to. The key suffix is the identifying cluster name and the"
          + " value is an identifier for a location on the target system, e.g. the ID"
          + " of the table on the target to replicate to"),
  TABLE_SAMPLER("table.sampler", "", PropertyType.CLASSNAME,
      "The name of a class that implements org.apache.accumulo.core.Sampler."
          + " Setting this option enables storing a sample of data which can be"
          + " scanned. Always having a current sample can useful for query optimization"
          + " and data comprehension. After enabling sampling for an existing table,"
          + " a compaction is needed to compute the sample for existing data. The"
          + " compact command in the shell has an option to only compact RFiles without"
          + " sample data."),
  TABLE_SAMPLER_OPTS("table.sampler.opt.", null, PropertyType.PREFIX,
      "The property is used to set options for a sampler. If a sample had two"
          + " options like hasher and modulous, then the two properties"
          + " table.sampler.opt.hasher=${hash algorithm} and"
          + " table.sampler.opt.modulous=${mod} would be set."),
  TABLE_SUSPEND_DURATION("table.suspend.duration", "0s", PropertyType.TIMEDURATION,
      "For tablets belonging to this table: When a tablet server dies, allow"
          + " the tablet server this duration to revive before reassigning its tablets"
          + " to other tablet servers."),
  TABLE_SUMMARIZER_PREFIX("table.summarizer.", null, PropertyType.PREFIX,
      "Prefix for configuring summarizers for a table. Using this prefix"
          + " multiple summarizers can be configured with options for each one. Each"
          + " summarizer configured should have a unique id, this id can be anything."
          + " To add a summarizer set "
          + "`table.summarizer.<unique id>=<summarizer class name>.` If the summarizer has options"
          + ", then for each option set `table.summarizer.<unique id>.opt.<key>=<value>`."),
  @Experimental
  TABLE_DELETE_BEHAVIOR("table.delete.behavior",
      DeletingIterator.Behavior.PROCESS.name().toLowerCase(), PropertyType.STRING,
      "This determines what action to take when a delete marker is seen."
          + " Valid values are `process` and `fail` with `process` being the default.  When set to "
          + "`process`, deletes will supress data.  When set to `fail`, any deletes seen will cause"
          + " an exception. The purpose of `fail` is to support tables that never delete data and"
          + " need fast seeks within the timestamp range of a column. When setting this to fail, "
          + "also consider configuring the `" + NoDeleteConstraint.class.getName() + "` "
          + "constraint."),

  // VFS ClassLoader properties

  // this property shouldn't be used directly; it exists solely to document the default value
  // defined by its use in AccumuloVFSClassLoader when generating the property documentation
  @Deprecated(since = "2.1.0", forRemoval = true)
  VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY(
      org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY,
      "", PropertyType.STRING,
      "Configuration for a system level vfs classloader. Accumulo jar can be"
          + " configured here and loaded out of HDFS."),
  @Deprecated(since = "2.1.0", forRemoval = true)
  VFS_CONTEXT_CLASSPATH_PROPERTY(
      org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.VFS_CONTEXT_CLASSPATH_PROPERTY,
      null, PropertyType.PREFIX,
      "Properties in this category are define a classpath. These properties"
          + " start  with the category prefix, followed by a context name. The value is"
          + " a comma separated list of URIs. Supports full regex on filename alone."
          + " For example, general.vfs.context.classpath.cx1=hdfs://nn1:9902/mylibdir/*.jar."
          + " You can enable post delegation for a context, which will load classes from the"
          + " context first instead of the parent first. Do this by setting"
          + " `general.vfs.context.classpath.<name>.delegation=post`, where `<name>` is"
          + " your context name. If delegation is not specified, it defaults to loading"
          + " from parent classloader first."),

  // this property shouldn't be used directly; it exists solely to document the default value
  // defined by its use in AccumuloVFSClassLoader when generating the property documentation
  @Deprecated(since = "2.1.0", forRemoval = true)
  VFS_CLASSLOADER_CACHE_DIR(
      org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.VFS_CACHE_DIR,
      "${java.io.tmpdir}", PropertyType.ABSOLUTEPATH,
      "The base directory to use for the vfs cache. The actual cached files will be located"
          + " in a subdirectory, `accumulo-vfs-cache-<jvmProcessName>-${user.name}`, where"
          + " `<jvmProcessName>` is determined by the JVM's internal management engine."
          + " The cache will keep a soft reference to all of the classes loaded in the VM."
          + " This should be on local disk on each node with sufficient space."),

  // General properties for configuring replication
  REPLICATION_PREFIX("replication.", null, PropertyType.PREFIX,
      "Properties in this category affect the replication of data to other Accumulo instances."),
  REPLICATION_PEERS("replication.peer.", null, PropertyType.PREFIX,
      "Properties in this category control what systems data can be replicated to"),
  REPLICATION_PEER_USER("replication.peer.user.", null, PropertyType.PREFIX,
      "The username to provide when authenticating with the given peer"),
  @Sensitive
  REPLICATION_PEER_PASSWORD("replication.peer.password.", null, PropertyType.PREFIX,
      "The password to provide when authenticating with the given peer"),
  REPLICATION_PEER_KEYTAB("replication.peer.keytab.", null, PropertyType.PREFIX,
      "The keytab to use when authenticating with the given peer"),
  REPLICATION_NAME("replication.name", "", PropertyType.STRING,
      "Name of this cluster with respect to replication. Used to identify this"
          + " instance from other peers"),
  REPLICATION_MAX_WORK_QUEUE("replication.max.work.queue", "1000", PropertyType.COUNT,
      "Upper bound of the number of files queued for replication"),
  REPLICATION_WORK_ASSIGNMENT_SLEEP("replication.work.assignment.sleep", "30s",
      PropertyType.TIMEDURATION, "Amount of time to sleep between replication work assignment"),
  REPLICATION_WORKER_THREADS("replication.worker.threads", "4", PropertyType.COUNT,
      "Size of the threadpool that each tabletserver devotes to replicating data"),
  REPLICATION_RECEIPT_SERVICE_PORT("replication.receipt.service.port", "10002", PropertyType.PORT,
      "Listen port used by thrift service in tserver listening for replication"),
  REPLICATION_WORK_ATTEMPTS("replication.work.attempts", "10", PropertyType.COUNT,
      "Number of attempts to try to replicate some data before giving up and"
          + " letting it naturally be retried later"),
  REPLICATION_MIN_THREADS("replication.receiver.min.threads", "1", PropertyType.COUNT,
      "Minimum number of threads for replication"),
  REPLICATION_THREADCHECK("replication.receiver.threadcheck.time", "30s", PropertyType.TIMEDURATION,
      "The time between adjustments of the replication thread pool."),
  REPLICATION_MAX_UNIT_SIZE("replication.max.unit.size", "64M", PropertyType.BYTES,
      "Maximum size of data to send in a replication message"),
  REPLICATION_WORK_ASSIGNER("replication.work.assigner",
      "org.apache.accumulo.manager.replication.UnorderedWorkAssigner", PropertyType.CLASSNAME,
      "Replication WorkAssigner implementation to use"),
  REPLICATION_DRIVER_DELAY("replication.driver.delay", "0s", PropertyType.TIMEDURATION,
      "Amount of time to wait before the replication work loop begins in the manager."),
  REPLICATION_WORK_PROCESSOR_DELAY("replication.work.processor.delay", "0s",
      PropertyType.TIMEDURATION,
      "Amount of time to wait before first checking for replication work, not"
          + " useful outside of tests"),
  REPLICATION_WORK_PROCESSOR_PERIOD("replication.work.processor.period", "0s",
      PropertyType.TIMEDURATION,
      "Amount of time to wait before re-checking for replication work, not"
          + " useful outside of tests"),
  REPLICATION_TRACE_PERCENT("replication.trace.percent", "0.1", PropertyType.FRACTION,
      "The sampling percentage to use for replication traces"),
  REPLICATION_RPC_TIMEOUT("replication.rpc.timeout", "2m", PropertyType.TIMEDURATION,
      "Amount of time for a single replication RPC call to last before failing"
          + " the attempt. See replication.work.attempts."),
  // deprecated properties grouped at the end to reference property that replaces them
  @Deprecated(since = "1.6.0")
  @ReplacedBy(property = INSTANCE_VOLUMES)
  INSTANCE_DFS_URI("instance.dfs.uri", "", PropertyType.URI,
      "This property is deprecated since 1.6.0. "
          + "A url accumulo should use to connect to DFS. If this is empty, accumulo"
          + " will obtain this information from the hadoop configuration. This property"
          + " will only be used when creating new files if instance.volumes is empty."
          + " After an upgrade to 1.6.0 Accumulo will start using absolute paths to"
          + " reference files. Files created before a 1.6.0 upgrade are referenced via"
          + " relative paths. Relative paths will always be resolved using this config"
          + " (if empty using the hadoop config)."),
  @Deprecated(since = "1.6.0")
  @ReplacedBy(property = INSTANCE_VOLUMES)
  INSTANCE_DFS_DIR("instance.dfs.dir", "/accumulo", PropertyType.ABSOLUTEPATH,
      "This property is deprecated since 1.6.0. "
          + "HDFS directory in which accumulo instance will run. "
          + "Do not change after accumulo is initialized."),
  @Deprecated(since = "2.0.0")
  GENERAL_CLASSPATHS(org.apache.accumulo.start.classloader.AccumuloClassLoader.GENERAL_CLASSPATHS,
      "", PropertyType.STRING,
      "This property is deprecated since 2.0.0. The class path should instead be configured"
          + " by the launch environment (for example, accumulo-env.sh). A list of all"
          + " of the places to look for a class. Order does matter, as it will look for"
          + " the jar starting in the first location to the last. Supports full regex"
          + " on filename alone."),
  @Deprecated(since = "1.7.0")
  @ReplacedBy(property = TABLE_DURABILITY)
  TSERV_WAL_SYNC_METHOD("tserver.wal.sync.method", "hsync", PropertyType.STRING,
      "This property is deprecated since 1.7.0. Use table.durability instead."),
  @Deprecated(since = "1.7.0")
  @ReplacedBy(property = TABLE_DURABILITY)
  TABLE_WALOG_ENABLED("table.walog.enabled", "true", PropertyType.BOOLEAN,
      "This setting is deprecated since 1.7.0. Use table.durability=none instead."),
  @Deprecated(since = "2.0.0")
  @ReplacedBy(property = TSERV_SCAN_EXECUTORS_DEFAULT_THREADS)
  TSERV_READ_AHEAD_MAXCONCURRENT("tserver.readahead.concurrent.max", "16", PropertyType.COUNT,
      "This property is deprecated since 2.0.0, use tserver.scan.executors.default.threads "
          + "instead. The maximum number of concurrent read ahead that will execute. This "
          + "effectively limits the number of long running scans that can run concurrently "
          + "per tserver.\""),
  @Deprecated(since = "2.0.0")
  @ReplacedBy(property = TSERV_SCAN_EXECUTORS_META_THREADS)
  TSERV_METADATA_READ_AHEAD_MAXCONCURRENT("tserver.metadata.readahead.concurrent.max", "8",
      PropertyType.COUNT,
      "This property is deprecated since 2.0.0, use tserver.scan.executors.meta.threads instead. "
          + "The maximum number of concurrent metadata read ahead that will execute.");

  private String key;
  private String defaultValue;
  private String description;
  private boolean annotationsComputed = false;
  private boolean isSensitive;
  private boolean isDeprecated;
  private boolean isExperimental;
  private Property replacedBy = null;
  private PropertyType type;

  Property(String name, String defaultValue, PropertyType type, String description) {
    this.key = name;
    this.defaultValue = defaultValue;
    this.description = description;
    this.type = type;
  }

  @Override
  public String toString() {
    return this.key;
  }

  /**
   * Gets the key (string) for this property.
   *
   * @return key
   */
  public String getKey() {
    return this.key;
  }

  /**
   * Gets the default value for this property. System properties are interpolated into the value if
   * necessary.
   *
   * @return default value
   */
  public String getDefaultValue() {
    return this.defaultValue;
  }

  /**
   * Gets the type of this property.
   *
   * @return property type
   */
  public PropertyType getType() {
    return this.type;
  }

  /**
   * Gets the description of this property.
   *
   * @return description
   */
  public String getDescription() {
    return this.description;
  }

  /**
   * Checks if this property is experimental.
   *
   * @return true if this property is experimental
   */
  public boolean isExperimental() {
    Preconditions.checkState(annotationsComputed,
        "precomputeAnnotations() must be called before calling this method");
    return isExperimental;
  }

  /**
   * Checks if this property is deprecated.
   *
   * @return true if this property is deprecated
   */
  public boolean isDeprecated() {
    Preconditions.checkState(annotationsComputed,
        "precomputeAnnotations() must be called before calling this method");
    return isDeprecated;
  }

  /**
   * Checks if this property is sensitive.
   *
   * @return true if this property is sensitive
   */
  public boolean isSensitive() {
    Preconditions.checkState(annotationsComputed,
        "precomputeAnnotations() must be called before calling this method");
    return isSensitive;
  }

  public Property replacedBy() {
    Preconditions.checkState(annotationsComputed,
        "precomputeAnnotations() must be called before calling this method");
    return replacedBy;
  }

  private void precomputeAnnotations() {
    isSensitive =
        hasAnnotation(Sensitive.class) || hasPrefixWithAnnotation(getKey(), Sensitive.class);
    isDeprecated =
        hasAnnotation(Deprecated.class) || hasPrefixWithAnnotation(getKey(), Deprecated.class);
    isExperimental =
        hasAnnotation(Experimental.class) || hasPrefixWithAnnotation(getKey(), Experimental.class);
    if (hasAnnotation(ReplacedBy.class)) {
      ReplacedBy rb = getAnnotation(ReplacedBy.class);
      if (rb != null) {
        replacedBy = rb.property();
      }
    }
    annotationsComputed = true;
  }

  /**
   * Checks if a property with the given key is sensitive. The key must be for a valid property, and
   * must either itself be annotated as sensitive or have a prefix annotated as sensitive.
   *
   * @param key
   *          property key
   * @return true if property is sensitive
   */
  public static boolean isSensitive(String key) {
    Property prop = propertiesByKey.get(key);
    if (prop != null) {
      return prop.isSensitive();
    } else {
      for (String prefix : validPrefixes) {
        if (key.startsWith(prefix)) {
          if (propertiesByKey.get(prefix).isSensitive()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private <T extends Annotation> boolean hasAnnotation(Class<T> annotationType) {
    Logger log = LoggerFactory.getLogger(getClass());
    try {
      for (Annotation a : getClass().getField(name()).getAnnotations()) {
        if (annotationType.isInstance(a)) {
          return true;
        }
      }
    } catch (SecurityException | NoSuchFieldException e) {
      log.error("{}", e.getMessage(), e);
    }
    return false;
  }

  private <T extends Annotation> T getAnnotation(Class<T> annotationType) {
    Logger log = LoggerFactory.getLogger(getClass());
    try {
      for (Annotation a : getClass().getField(name()).getAnnotations()) {
        if (annotationType.isInstance(a)) {
          @SuppressWarnings("unchecked")
          T uncheckedA = (T) a;
          return uncheckedA;
        }
      }
    } catch (SecurityException | NoSuchFieldException e) {
      log.error("{}", e.getMessage(), e);
    }
    return null;
  }

  private static <T extends Annotation> boolean hasPrefixWithAnnotation(String key,
      Class<T> annotationType) {
    for (String prefix : validPrefixes) {
      if (key.startsWith(prefix)) {
        if (propertiesByKey.get(prefix).hasAnnotation(annotationType)) {
          return true;
        }
      }
    }

    return false;
  }

  private static HashSet<String> validTableProperties = null;
  private static HashSet<String> validProperties = null;
  private static HashSet<String> validPrefixes = null;
  private static HashMap<String,Property> propertiesByKey = null;

  private static boolean isKeyValidlyPrefixed(String key) {
    for (String prefix : validPrefixes) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks if the given property key is valid. A valid property key is either equal to the key of
   * some defined property or has a prefix matching some prefix defined in this class.
   *
   * @param key
   *          property key
   * @return true if key is valid (recognized, or has a recognized prefix)
   */
  public static boolean isValidPropertyKey(String key) {
    return validProperties.contains(key) || isKeyValidlyPrefixed(key);
  }

  /**
   * Checks if the given property key is a valid property and is of type boolean.
   *
   * @param key
   *          property key
   * @return true if key is valid and is of type boolean, false otherwise
   */
  public static boolean isValidBooleanPropertyKey(String key) {
    return validProperties.contains(key) && getPropertyByKey(key).getType() == PropertyType.BOOLEAN;
  }

  /**
   * Checks if the given property key is for a valid table property. A valid table property key is
   * either equal to the key of some defined table property (which each start with
   * {@link #TABLE_PREFIX}) or has a prefix matching {@link #TABLE_CONSTRAINT_PREFIX},
   * {@link #TABLE_ITERATOR_PREFIX}, or {@link #TABLE_LOCALITY_GROUP_PREFIX}.
   *
   * @param key
   *          property key
   * @return true if key is valid for a table property (recognized, or has a recognized prefix)
   */
  public static boolean isValidTablePropertyKey(String key) {
    return validTableProperties.contains(key) || (key.startsWith(Property.TABLE_PREFIX.getKey())
        && (key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())
            || key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())
            || key.startsWith(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey())
            || key.startsWith(Property.TABLE_COMPACTION_STRATEGY_PREFIX.getKey())
            || key.startsWith(Property.TABLE_REPLICATION_TARGET.getKey())
            || key.startsWith(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey())
            || key.startsWith(TABLE_SAMPLER_OPTS.getKey())
            || key.startsWith(TABLE_SUMMARIZER_PREFIX.getKey())
            || key.startsWith(TABLE_SCAN_DISPATCHER_OPTS.getKey())
            || key.startsWith(TABLE_COMPACTION_DISPATCHER_OPTS.getKey())
            || key.startsWith(TABLE_COMPACTION_CONFIGURER_OPTS.getKey())
            || key.startsWith(TABLE_COMPACTION_SELECTOR_OPTS.getKey())));
  }

  private static final EnumSet<Property> fixedProperties =
      EnumSet.of(Property.TSERV_CLIENTPORT, Property.TSERV_NATIVEMAP_ENABLED,
          Property.TSERV_SCAN_MAX_OPENFILES, Property.MANAGER_CLIENTPORT, Property.GC_PORT);

  /**
   * Checks if the given property may be changed via Zookeeper, but not recognized until the restart
   * of some relevant daemon.
   *
   * @param key
   *          property key
   * @return true if property may be changed via Zookeeper but only heeded upon some restart
   */
  public static boolean isFixedZooPropertyKey(Property key) {
    return fixedProperties.contains(key);
  }

  /**
   * Checks if the given property key is valid for a property that may be changed via Zookeeper.
   *
   * @param key
   *          property key
   * @return true if key's property may be changed via Zookeeper
   */
  public static boolean isValidZooPropertyKey(String key) {
    // white list prefixes
    return key.startsWith(Property.TABLE_PREFIX.getKey())
        || key.startsWith(Property.TSERV_PREFIX.getKey())
        || key.startsWith(Property.MANAGER_PREFIX.getKey())
        || key.startsWith(Property.MASTER_PREFIX.getKey())
        || key.startsWith(Property.GC_PREFIX.getKey())
        || key.startsWith(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey())
        || key.startsWith(VFS_CONTEXT_CLASSPATH_PROPERTY.getKey())
        || key.startsWith(REPLICATION_PREFIX.getKey());
  }

  /**
   * Gets a {@link Property} instance with the given key.
   *
   * @param key
   *          property key
   * @return property, or null if not found
   */
  public static Property getPropertyByKey(String key) {
    return propertiesByKey.get(key);
  }

  /**
   * Checks if this property is expected to have a Java class as a value.
   *
   * @return true if this is property is a class property
   */
  public static boolean isClassProperty(String key) {
    return (key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())
        && key.substring(Property.TABLE_CONSTRAINT_PREFIX.getKey().length()).split("\\.").length
            == 1)
        || (key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())
            && key.substring(Property.TABLE_ITERATOR_PREFIX.getKey().length()).split("\\.").length
                == 2)
        || key.equals(Property.TABLE_LOAD_BALANCER.getKey());
  }

  /**
   * Creates a new instance of a class specified in a configuration property. The table classpath
   * context is used if set.
   *
   * @param conf
   *          configuration containing property
   * @param property
   *          property specifying class name
   * @param base
   *          base class of type
   * @param defaultInstance
   *          instance to use if creation fails
   * @return new class instance, or default instance if creation failed
   */
  public static <T> T createTableInstanceFromPropertyName(AccumuloConfiguration conf,
      Property property, Class<T> base, T defaultInstance) {
    String clazzName = conf.get(property);
    String context = ClassLoaderUtil.tableContext(conf);
    return ConfigurationTypeHelper.getClassInstance(context, clazzName, base, defaultInstance);
  }

  /**
   * Creates a new instance of a class specified in a configuration property.
   *
   * @param conf
   *          configuration containing property
   * @param property
   *          property specifying class name
   * @param base
   *          base class of type
   * @param defaultInstance
   *          instance to use if creation fails
   * @return new class instance, or default instance if creation failed
   */
  public static <T> T createInstanceFromPropertyName(AccumuloConfiguration conf, Property property,
      Class<T> base, T defaultInstance) {
    String clazzName = conf.get(property);
    return ConfigurationTypeHelper.getClassInstance(null, clazzName, base, defaultInstance);
  }

  static {
    // Precomputing information here avoids :
    // * Computing it each time a method is called
    // * Using synch to compute the first time a method is called
    propertiesByKey = new HashMap<>();
    validPrefixes = new HashSet<>();
    validProperties = new HashSet<>();

    for (Property p : Property.values()) {
      if (p.getType().equals(PropertyType.PREFIX)) {
        validPrefixes.add(p.getKey());
      } else {
        validProperties.add(p.getKey());
      }
      propertiesByKey.put(p.getKey(), p);
    }

    validTableProperties = new HashSet<>();
    for (Property p : Property.values()) {
      if (!p.getType().equals(PropertyType.PREFIX)
          && p.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
        validTableProperties.add(p.getKey());
      }
    }

    // order is very important here the following code relies on the maps and sets populated above
    for (Property p : Property.values()) {
      p.precomputeAnnotations();
    }
  }
}
