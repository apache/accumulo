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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.interpret.DefaultScanInterpreter;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum Property {
  // Crypto-related properties
  @Experimental
  CRYPTO_PREFIX("crypto.", null, PropertyType.PREFIX, "Properties in this category related to the configuration of both default and custom crypto modules."),
  @Experimental
  CRYPTO_MODULE_CLASS("crypto.module.class", "NullCryptoModule", PropertyType.STRING,
      "Fully qualified class name of the class that implements the CryptoModule interface, to be used in setting up encryption at rest for the WAL and "
          + "(future) other parts of the code."),
  @Experimental
  CRYPTO_CIPHER_SUITE("crypto.cipher.suite", "NullCipher", PropertyType.STRING,
      "Describes the cipher suite to use for rfile encryption. The value must be either NullCipher or in the form of algorithm/mode/padding, "
          + "e.g. AES/CBC/NoPadding"),
  @Experimental
  CRYPTO_WAL_CIPHER_SUITE("crypto.wal.cipher.suite", "", PropertyType.STRING,
      "Describes the cipher suite to use for the write-ahead log. Defaults to 'cyrpto.cipher.suite' "
          + "and will use that value for WAL encryption unless otherwise specified. Valid suite values include: an empty string, NullCipher, or a string the "
          + "form of algorithm/mode/padding, e.g. AES/CBC/NOPadding"),
  @Experimental
  CRYPTO_CIPHER_KEY_ALGORITHM_NAME("crypto.cipher.key.algorithm.name", "NullCipher", PropertyType.STRING,
      "States the name of the algorithm used for the key for the corresponding cipher suite. The key type must be compatible with the cipher suite."),
  @Experimental
  CRYPTO_BLOCK_STREAM_SIZE("crypto.block.stream.size", "1K", PropertyType.BYTES,
      "The size of the buffer above the cipher stream. Used for reading files and padding walog entries."),
  @Experimental
  CRYPTO_CIPHER_KEY_LENGTH("crypto.cipher.key.length", "128", PropertyType.STRING,
      "Specifies the key length *in bits* to use for the symmetric key, should probably be 128 or 256 unless you really know what you're doing"),
  @Experimental
  CRYPTO_SECURITY_PROVIDER("crypto.security.provider", "", PropertyType.STRING,
      "States the security provider to use, and defaults to the system configured provider"),
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
      "The cipher suite to use when encrypting session keys with a key encryption keyThis should be set to match the overall encryption algorithm "
          + "but with ECB mode and no padding unless you really know what you're doing and are sure you won't break internal file formats"),
  @Experimental
  CRYPTO_OVERRIDE_KEY_STRATEGY_WITH_CONFIGURED_STRATEGY("crypto.override.key.strategy.with.configured.strategy", "false", PropertyType.BOOLEAN,
      "The default behavior is to record the key encryption strategy with the encrypted file, and continue to use that strategy for the life "
          + "of that file. Sometimes, you change your strategy and want to use the new strategy, not the old one. (Most commonly, this will be "
          + "because you have moved key material from one spot to another.)  If you want to override the recorded key strategy with the one in "
          + "the configuration file, set this property to true."),
  // SSL properties local to each node (see also instance.ssl.enabled which must be consistent across all nodes in an instance)
  RPC_PREFIX("rpc.", null, PropertyType.PREFIX, "Properties in this category related to the configuration of SSL keys for RPC. See also instance.ssl.enabled"),
  RPC_SSL_KEYSTORE_PATH("rpc.javax.net.ssl.keyStore", "", PropertyType.PATH, "Path of the keystore file for the server's private SSL key"),
  @Sensitive
  RPC_SSL_KEYSTORE_PASSWORD("rpc.javax.net.ssl.keyStorePassword", "", PropertyType.STRING,
      "Password used to encrypt the SSL private keystore. Leave blank to use the Accumulo instance secret"),
  RPC_SSL_KEYSTORE_TYPE("rpc.javax.net.ssl.keyStoreType", "jks", PropertyType.STRING, "Type of SSL keystore"),
  RPC_SSL_TRUSTSTORE_PATH("rpc.javax.net.ssl.trustStore", "", PropertyType.PATH, "Path of the truststore file for the root cert"),
  @Sensitive
  RPC_SSL_TRUSTSTORE_PASSWORD("rpc.javax.net.ssl.trustStorePassword", "", PropertyType.STRING,
      "Password used to encrypt the SSL truststore. Leave blank to use no password"),
  RPC_SSL_TRUSTSTORE_TYPE("rpc.javax.net.ssl.trustStoreType", "jks", PropertyType.STRING, "Type of SSL truststore"),
  RPC_USE_JSSE("rpc.useJsse", "false", PropertyType.BOOLEAN, "Use JSSE system properties to configure SSL rather than the " + RPC_PREFIX.getKey()
      + "javax.net.ssl.* Accumulo properties"),
  RPC_SSL_CIPHER_SUITES("rpc.ssl.cipher.suites", "", PropertyType.STRING, "Comma separated list of cipher suites that can be used by accepted connections"),
  RPC_SSL_ENABLED_PROTOCOLS("rpc.ssl.server.enabled.protocols", "TLSv1,TLSv1.1,TLSv1.2", PropertyType.STRING,
      "Comma separated list of protocols that can be used to accept connections"),
  // TLSv1.2 should be used as the default when JDK6 support is dropped
  RPC_SSL_CLIENT_PROTOCOL("rpc.ssl.client.protocol", "TLSv1", PropertyType.STRING,
      "The protocol used to connect to a secure server, must be in the list of enabled protocols on the server side (rpc.ssl.server.enabled.protocols)"),
  /**
   * @since 1.7.0
   */
  RPC_SASL_QOP("rpc.sasl.qop", "auth", PropertyType.STRING,
      "The quality of protection to be used with SASL. Valid values are 'auth', 'auth-int', and 'auth-conf'"),

  // instance properties (must be the same for every node in an instance)
  INSTANCE_PREFIX("instance.", null, PropertyType.PREFIX,
      "Properties in this category must be consistent throughout a cloud. This is enforced and servers won't be able to communicate if these differ."),
  INSTANCE_ZK_HOST("instance.zookeeper.host", "localhost:2181", PropertyType.HOSTLIST, "Comma separated list of zookeeper servers"),
  INSTANCE_ZK_TIMEOUT("instance.zookeeper.timeout", "30s", PropertyType.TIMEDURATION,
      "Zookeeper session timeout; max value when represented as milliseconds should be no larger than " + Integer.MAX_VALUE),
  @Deprecated
  INSTANCE_DFS_URI("instance.dfs.uri", "", PropertyType.URI,
      "A url accumulo should use to connect to DFS. If this is empty, accumulo will obtain this information from the hadoop configuration. This property "
          + "will only be used when creating new files if instance.volumes is empty. After an upgrade to 1.6.0 Accumulo will start using absolute paths to "
          + "reference files. Files created before a 1.6.0 upgrade are referenced via relative paths. Relative paths will always be resolved using this "
          + "config (if empty using the hadoop config)."),
  @Deprecated
  INSTANCE_DFS_DIR("instance.dfs.dir", "/accumulo", PropertyType.ABSOLUTEPATH,
      "HDFS directory in which accumulo instance will run. Do not change after accumulo is initialized."),
  @Sensitive
  INSTANCE_SECRET("instance.secret", "DEFAULT", PropertyType.STRING,
      "A secret unique to a given instance that all servers must know in order to communicate with one another."
          + "It should be changed prior to the initialization of Accumulo. To change it after Accumulo has been initialized, use the ChangeSecret tool "
          + "and then update accumulo-site.xml everywhere. Before using the ChangeSecret tool, make sure Accumulo is not running and you are logged "
          + "in as the user that controls Accumulo files in HDFS.  To use the ChangeSecret tool, run the command: "
          + "./bin/accumulo org.apache.accumulo.server.util.ChangeSecret"),
  INSTANCE_VOLUMES("instance.volumes", "", PropertyType.STRING,
      "A comma seperated list of dfs uris to use. Files will be stored across these filesystems. If this is empty, then instance.dfs.uri will be used. "
          + "After adding uris to this list, run 'accumulo init --add-volume' and then restart tservers. If entries are removed from this list then tservers "
          + "will need to be restarted. After a uri is removed from the list Accumulo will not create new files in that location, however Accumulo can still "
          + "reference files created at that location before the config change. To use a comma or other reserved characters in a URI use standard URI hex "
          + "encoding. For example replace commas with %2C."),
  INSTANCE_VOLUMES_REPLACEMENTS("instance.volumes.replacements", "", PropertyType.STRING,
      "Since accumulo stores absolute URIs changing the location of a namenode could prevent Accumulo from starting. The property helps deal with that "
          + "situation. Provide a comma separated list of uri replacement pairs here if a namenode location changes. Each pair shold be separated with a "
          + "space. For example, if hdfs://nn1 was replaced with hdfs://nnA and hdfs://nn2 was replaced with hdfs://nnB, then set this property to "
          + "'hdfs://nn1 hdfs://nnA,hdfs://nn2 hdfs://nnB' Replacements must be configured for use. To see which volumes are currently in use, run "
          + "'accumulo admin volumes -l'. To use a comma or other reserved characters in a URI use standard URI hex encoding. For example replace commas with "
          + "%2C."),
  INSTANCE_SECURITY_AUTHENTICATOR("instance.security.authenticator", "org.apache.accumulo.server.security.handler.ZKAuthenticator", PropertyType.CLASSNAME,
      "The authenticator class that accumulo will use to determine if a user has privilege to perform an action"),
  INSTANCE_SECURITY_AUTHORIZOR("instance.security.authorizor", "org.apache.accumulo.server.security.handler.ZKAuthorizor", PropertyType.CLASSNAME,
      "The authorizor class that accumulo will use to determine what labels a user has privilege to see"),
  INSTANCE_SECURITY_PERMISSION_HANDLER("instance.security.permissionHandler", "org.apache.accumulo.server.security.handler.ZKPermHandler",
      PropertyType.CLASSNAME, "The permission handler class that accumulo will use to determine if a user has privilege to perform an action"),
  INSTANCE_RPC_SSL_ENABLED("instance.rpc.ssl.enabled", "false", PropertyType.BOOLEAN,
      "Use SSL for socket connections from clients and among accumulo services. Mutually exclusive with SASL RPC configuration."),
  INSTANCE_RPC_SSL_CLIENT_AUTH("instance.rpc.ssl.clientAuth", "false", PropertyType.BOOLEAN, "Require clients to present certs signed by a trusted root"),
  /**
   * @since 1.7.0
   */
  INSTANCE_RPC_SASL_ENABLED("instance.rpc.sasl.enabled", "false", PropertyType.BOOLEAN,
      "Configures Thrift RPCs to require SASL with GSSAPI which supports Kerberos authentication. Mutually exclusive with SSL RPC configuration."),
  @Deprecated
  INSTANCE_RPC_SASL_PROXYUSERS("instance.rpc.sasl.impersonation.", null, PropertyType.PREFIX,
      "Prefix that allows configuration of users that are allowed to impersonate other users"),
  INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION("instance.rpc.sasl.allowed.user.impersonation", "", PropertyType.STRING,
      "One-line configuration property controlling what users are allowed to impersonate other users"),
  INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION("instance.rpc.sasl.allowed.host.impersonation", "", PropertyType.STRING,
      "One-line configuration property controlling the network locations (hostnames) that are allowed to impersonate other users"),

  // general properties
  GENERAL_PREFIX("general.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of accumulo overall, but do not have to be consistent throughout a cloud."),
  @Deprecated
  GENERAL_CLASSPATHS(AccumuloClassLoader.GENERAL_CLASSPATHS, "", PropertyType.STRING,
      "This property is deprecated. The class path should instead be configured by the launch environment (for example, accumulo-env.sh). "
          + "A list of all of the places to look for a class. Order does matter, as it will look for the jar "
          + "starting in the first location to the last. Supports full regex on filename alone."),
  GENERAL_DYNAMIC_CLASSPATHS(AccumuloVFSClassLoader.DYNAMIC_CLASSPATH_PROPERTY_NAME, AccumuloVFSClassLoader.DEFAULT_DYNAMIC_CLASSPATH_VALUE,
      PropertyType.STRING, "A list of all of the places where changes in jars or classes will force a reload of the classloader."),
  GENERAL_RPC_TIMEOUT("general.rpc.timeout", "120s", PropertyType.TIMEDURATION, "Time to wait on I/O for simple, short RPC calls"),
  @Experimental
  GENERAL_RPC_SERVER_TYPE("general.rpc.server.type", "", PropertyType.STRING,
      "Type of Thrift server to instantiate, see org.apache.accumulo.server.rpc.ThriftServerType for more information. Only useful for benchmarking thrift servers"),
  GENERAL_KERBEROS_KEYTAB("general.kerberos.keytab", "", PropertyType.PATH, "Path to the kerberos keytab to use. Leave blank if not using kerberoized hdfs"),
  GENERAL_KERBEROS_PRINCIPAL("general.kerberos.principal", "", PropertyType.STRING, "Name of the kerberos principal to use. _HOST will automatically be "
      + "replaced by the machines hostname in the hostname portion of the principal. Leave blank if not using kerberoized hdfs"),
  GENERAL_KERBEROS_RENEWAL_PERIOD("general.kerberos.renewal.period", "30s", PropertyType.TIMEDURATION, "The amount of time between attempts to perform "
      + "Kerberos ticket renewals. This does not equate to how often tickets are actually renewed (which is performed at 80% of the ticket lifetime)."),
  GENERAL_MAX_MESSAGE_SIZE("general.server.message.size.max", "1G", PropertyType.BYTES, "The maximum size of a message that can be sent to a server."),
  GENERAL_SIMPLETIMER_THREADPOOL_SIZE("general.server.simpletimer.threadpool.size", "1", PropertyType.COUNT, "The number of threads to use for "
      + "server-internal scheduled tasks"),
  // If you update the default type, be sure to update the default used for initialization failures in VolumeManagerImpl
  @Experimental
  GENERAL_VOLUME_CHOOSER("general.volume.chooser", "org.apache.accumulo.server.fs.RandomVolumeChooser", PropertyType.CLASSNAME,
      "The class that will be used to select which volume will be used to create new files."),
  GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS("general.security.credential.provider.paths", "", PropertyType.STRING,
      "Comma-separated list of paths to CredentialProviders"),
  GENERAL_LEGACY_METRICS("general.legacy.metrics", "false", PropertyType.BOOLEAN,
      "Use the old metric infrastructure configured by accumulo-metrics.xml, instead of Hadoop Metrics2"),
  GENERAL_ARBITRARY_PROP_PREFIX("general.custom.", null, PropertyType.PREFIX, "Prefix to be used for user defined system-wide properties. "
      + "This may be particularly useful for system-wide configuration for various user-implementations of "
      + "pluggable Accumulo features, such as the balancer or volume chooser."),
  GENERAL_DELEGATION_TOKEN_LIFETIME("general.delegation.token.lifetime", "7d", PropertyType.TIMEDURATION,
      "The length of time that delegation tokens and secret keys are valid"),
  GENERAL_DELEGATION_TOKEN_UPDATE_INTERVAL("general.delegation.token.update.interval", "1d", PropertyType.TIMEDURATION,
      "The length of time between generation of new secret keys"),
  GENERAL_MAX_SCANNER_RETRY_PERIOD("general.max.scanner.retry.period", "5s", PropertyType.TIMEDURATION,
      "The maximum amount of time that a Scanner should wait before retrying a failed RPC"),

  // properties that are specific to master server behavior
  MASTER_PREFIX("master.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the master server"),
  MASTER_CLIENTPORT("master.port.client", "9999", PropertyType.PORT, "The port used for handling client connections on the master"),
  MASTER_TABLET_BALANCER("master.tablet.balancer", "org.apache.accumulo.server.master.balancer.TableLoadBalancer", PropertyType.CLASSNAME,
      "The balancer class that accumulo will use to make tablet assignment and migration decisions."),
  MASTER_RECOVERY_MAXAGE("master.recovery.max.age", "60m", PropertyType.TIMEDURATION, "Recovery files older than this age will be removed."),
  MASTER_RECOVERY_MAXTIME("master.recovery.time.max", "30m", PropertyType.TIMEDURATION, "The maximum time to attempt recovery before giving up"),
  MASTER_BULK_RETRIES("master.bulk.retries", "3", PropertyType.COUNT, "The number of attempts to bulk import a RFile before giving up."),
  MASTER_BULK_THREADPOOL_SIZE("master.bulk.threadpool.size", "5", PropertyType.COUNT, "The number of threads to use when coordinating a bulk import."),
  MASTER_BULK_TIMEOUT("master.bulk.timeout", "5m", PropertyType.TIMEDURATION, "The time to wait for a tablet server to process a bulk import request"),
  MASTER_BULK_RENAME_THREADS("master.bulk.rename.threadpool.size", "20", PropertyType.COUNT,
      "The number of threads to use when moving user files to bulk ingest directories under accumulo control"),
  MASTER_BULK_TSERVER_REGEX("master.bulk.tserver.regex", "", PropertyType.STRING,
      "Regular expression that defines the set of Tablet Servers that will perform bulk imports"),
  MASTER_MINTHREADS("master.server.threads.minimum", "20", PropertyType.COUNT, "The minimum number of threads to use to handle incoming requests."),
  MASTER_THREADCHECK("master.server.threadcheck.time", "1s", PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool."),
  MASTER_RECOVERY_DELAY("master.recovery.delay", "10s", PropertyType.TIMEDURATION,
      "When a tablet server's lock is deleted, it takes time for it to completely quit. This delay gives it time before log recoveries begin."),
  MASTER_LEASE_RECOVERY_WAITING_PERIOD("master.lease.recovery.interval", "5s", PropertyType.TIMEDURATION,
      "The amount of time to wait after requesting a write-ahead log to be recovered"),
  MASTER_WALOG_CLOSER_IMPLEMETATION("master.walog.closer.implementation", "org.apache.accumulo.server.master.recovery.HadoopLogCloser", PropertyType.CLASSNAME,
      "A class that implements a mechanism to steal write access to a write-ahead log"),
  MASTER_FATE_THREADPOOL_SIZE("master.fate.threadpool.size", "4", PropertyType.COUNT,
      "The number of threads used to run fault-tolerant executions (FATE). These are primarily table operations like merge."),
  MASTER_REPLICATION_SCAN_INTERVAL("master.replication.status.scan.interval", "30s", PropertyType.TIMEDURATION,
      "Amount of time to sleep before scanning the status section of the replication table for new data"),
  MASTER_REPLICATION_COORDINATOR_PORT("master.replication.coordinator.port", "10001", PropertyType.PORT, "Port for the replication coordinator service"),
  MASTER_REPLICATION_COORDINATOR_MINTHREADS("master.replication.coordinator.minthreads", "4", PropertyType.COUNT,
      "Minimum number of threads dedicated to answering coordinator requests"),
  MASTER_REPLICATION_COORDINATOR_THREADCHECK("master.replication.coordinator.threadcheck.time", "5s", PropertyType.TIMEDURATION,
      "The time between adjustments of the coordinator thread pool"),
  MASTER_STATUS_THREAD_POOL_SIZE("master.status.threadpool.size", "1", PropertyType.COUNT,
      "The number of threads to use when fetching the tablet server status for balancing."),
  MASTER_METADATA_SUSPENDABLE("master.metadata.suspendable", "false", PropertyType.BOOLEAN, "Allow tablets for the " + MetadataTable.NAME
      + " table to be suspended via table.suspend.duration."),

  // properties that are specific to tablet server behavior
  TSERV_PREFIX("tserver.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the tablet servers"),
  TSERV_CLIENT_TIMEOUT("tserver.client.timeout", "3s", PropertyType.TIMEDURATION, "Time to wait for clients to continue scans before closing a session."),
  TSERV_DEFAULT_BLOCKSIZE("tserver.default.blocksize", "1M", PropertyType.BYTES, "Specifies a default blocksize for the tserver caches"),
  TSERV_CACHE_MANAGER_IMPL("tserver.cache.manager.class", "org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager", PropertyType.STRING,
      "Specifies the class name of the block cache factory implementation. Alternative implementation is "
          + "org.apache.accumulo.core.file.blockfile.cache.tinylfu.TinyLfuBlockCacheManager"),
  TSERV_DATACACHE_SIZE("tserver.cache.data.size", "10%", PropertyType.MEMORY, "Specifies the size of the cache for RFile data blocks."),
  TSERV_INDEXCACHE_SIZE("tserver.cache.index.size", "25%", PropertyType.MEMORY, "Specifies the size of the cache for RFile index blocks."),
  TSERV_SUMMARYCACHE_SIZE("tserver.cache.summary.size", "10%", PropertyType.MEMORY, "Specifies the size of the cache for summary data on each tablet server."),
  TSERV_PORTSEARCH("tserver.port.search", "false", PropertyType.BOOLEAN, "if the ports above are in use, search higher ports until one is available"),
  TSERV_CLIENTPORT("tserver.port.client", "9997", PropertyType.PORT, "The port used for handling client connections on the tablet servers"),
  @Deprecated
  TSERV_MUTATION_QUEUE_MAX("tserver.mutation.queue.max", "1M", PropertyType.BYTES, "This setting is deprecated. See tserver.total.mutation.queue.max. "
      + "The amount of memory to use to store write-ahead-log mutations-per-session before flushing them. Since the buffer is per write session, consider the"
      + " max number of concurrent writer when configuring. When using Hadoop 2, Accumulo will call hsync() on the WAL . For a small number of "
      + "concurrent writers, increasing this buffer size decreases the frequncy of hsync calls. For a large number of concurrent writers a small buffers "
      + "size is ok because of group commit."),
  TSERV_TOTAL_MUTATION_QUEUE_MAX("tserver.total.mutation.queue.max", "5%", PropertyType.MEMORY,
      "The amount of memory used to store write-ahead-log mutations before flushing them."),
  TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN("tserver.tablet.split.midpoint.files.max", "300", PropertyType.COUNT,
      "To find a tablets split points, all RFiles are opened and their indexes are read. This setting determines how many "
          + "RFiles can be opened at once. When there are more RFiles than this setting multiple passes "
          + "must be made, which is slower. However opening too many RFiles at once can cause problems."),
  TSERV_WALOG_MAX_SIZE("tserver.walog.max.size", "1g", PropertyType.BYTES,
      "The maximum size for each write-ahead log. See comment for property tserver.memory.maps.max"),
  TSERV_WALOG_MAX_AGE("tserver.walog.max.age", "24h", PropertyType.TIMEDURATION, "The maximum age for each write-ahead log."),
  TSERV_WALOG_TOLERATED_CREATION_FAILURES("tserver.walog.tolerated.creation.failures", "50", PropertyType.COUNT,
      "The maximum number of failures tolerated when creating a new write-ahead log within the period specified by tserver.walog.failures.period."
          + " Exceeding this number of failures in the period causes the TabletServer to exit."),
  TSERV_WALOG_TOLERATED_WAIT_INCREMENT("tserver.walog.tolerated.wait.increment", "1000ms", PropertyType.TIMEDURATION,
      "The amount of time to wait between failures to create a WALog."),
  // Never wait longer than 5 mins for a retry
  TSERV_WALOG_TOLERATED_MAXIMUM_WAIT_DURATION("tserver.walog.maximum.wait.duration", "5m", PropertyType.TIMEDURATION,
      "The maximum amount of time to wait after a failure to create a write-ahead log."),
  TSERV_MAJC_DELAY("tserver.compaction.major.delay", "30s", PropertyType.TIMEDURATION,
      "Time a tablet server will sleep between checking which tablets need compaction."),
  TSERV_MAJC_THREAD_MAXOPEN("tserver.compaction.major.thread.files.open.max", "10", PropertyType.COUNT,
      "Max number of RFiles a major compaction thread can open at once. "),
  TSERV_SCAN_MAX_OPENFILES("tserver.scan.files.open.max", "100", PropertyType.COUNT,
      "Maximum total RFiles that all tablets in a tablet server can open for scans. "),
  TSERV_MAX_IDLE("tserver.files.open.idle", "1m", PropertyType.TIMEDURATION, "Tablet servers leave previously used RFiles open for future queries. "
      + "This setting determines how much time an unused RFile should be kept open until it is closed."),
  TSERV_NATIVEMAP_ENABLED("tserver.memory.maps.native.enabled", "true", PropertyType.BOOLEAN,
      "An in-memory data store for accumulo implemented in c++ that increases the amount of data accumulo can hold in memory and avoids Java GC pauses."),
  TSERV_MAXMEM("tserver.memory.maps.max", "33%", PropertyType.MEMORY,
      "Maximum amount of memory that can be used to buffer data written to a tablet server. There are two other properties that can effectively limit memory"
          + " usage table.compaction.minor.logs.threshold and tserver.walog.max.size. Ensure that table.compaction.minor.logs.threshold *"
          + " tserver.walog.max.size >= this property."),
  TSERV_MEM_MGMT("tserver.memory.manager", "org.apache.accumulo.server.tabletserver.LargestFirstMemoryManager", PropertyType.CLASSNAME,
      "An implementation of MemoryManger that accumulo will use."),
  TSERV_SESSION_MAXIDLE("tserver.session.idle.max", "1m", PropertyType.TIMEDURATION, "When a tablet server's SimpleTimer thread triggers to check "
      + "idle sessions, this configurable option will be used to evaluate scan sessions to determine if they can be closed due to inactivity"),
  TSERV_UPDATE_SESSION_MAXIDLE("tserver.session.update.idle.max", "1m", PropertyType.TIMEDURATION,
      "When a tablet server's SimpleTimer thread triggers to check "
          + "idle sessions, this configurable option will be used to evaluate update sessions to determine if they can be closed due to inactivity"),
  TSERV_READ_AHEAD_MAXCONCURRENT("tserver.readahead.concurrent.max", "16", PropertyType.COUNT,
      "The maximum number of concurrent read ahead that will execute. This effectively"
          + " limits the number of long running scans that can run concurrently per tserver."),
  TSERV_METADATA_READ_AHEAD_MAXCONCURRENT("tserver.metadata.readahead.concurrent.max", "8", PropertyType.COUNT,
      "The maximum number of concurrent metadata read ahead that will execute."),
  TSERV_MIGRATE_MAXCONCURRENT("tserver.migrations.concurrent.max", "1", PropertyType.COUNT,
      "The maximum number of concurrent tablet migrations for a tablet server"),
  TSERV_MAJC_MAXCONCURRENT("tserver.compaction.major.concurrent.max", "3", PropertyType.COUNT,
      "The maximum number of concurrent major compactions for a tablet server"),
  TSERV_MAJC_THROUGHPUT("tserver.compaction.major.throughput", "0B", PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major compactions on a TabletServer, or 0B for unlimited."),
  TSERV_MINC_MAXCONCURRENT("tserver.compaction.minor.concurrent.max", "4", PropertyType.COUNT,
      "The maximum number of concurrent minor compactions for a tablet server"),
  TSERV_MAJC_TRACE_PERCENT("tserver.compaction.major.trace.percent", "0.1", PropertyType.FRACTION, "The percent of major compactions to trace"),
  TSERV_MINC_TRACE_PERCENT("tserver.compaction.minor.trace.percent", "0.1", PropertyType.FRACTION, "The percent of minor compactions to trace"),
  TSERV_COMPACTION_WARN_TIME("tserver.compaction.warn.time", "10m", PropertyType.TIMEDURATION,
      "When a compaction has not made progress for this time period, a warning will be logged"),
  TSERV_BLOOM_LOAD_MAXCONCURRENT("tserver.bloom.load.concurrent.max", "4", PropertyType.COUNT,
      "The number of concurrent threads that will load bloom filters in the background. "
          + "Setting this to zero will make bloom filters load in the foreground."),
  TSERV_MONITOR_FS("tserver.monitor.fs", "true", PropertyType.BOOLEAN,
      "When enabled the tserver will monitor file systems and kill itself when one switches from rw to ro. This is usually and indication that Linux has"
          + " detected a bad disk."),
  TSERV_MEMDUMP_DIR("tserver.dir.memdump", "/tmp", PropertyType.PATH,
      "A long running scan could possibly hold memory that has been minor compacted. To prevent this, the in memory map is dumped to a local file and the "
          + "scan is switched to that local file. We can not switch to the minor compacted file because it may have been modified by iterators. The file "
          + "dumped to the local dir is an exact copy of what was in memory."),
  TSERV_BULK_PROCESS_THREADS("tserver.bulk.process.threads", "1", PropertyType.COUNT,
      "The master will task a tablet server with pre-processing a bulk import RFile prior to assigning it to the appropriate tablet servers. This configuration"
          + " value controls the number of threads used to process the files."),
  TSERV_BULK_ASSIGNMENT_THREADS("tserver.bulk.assign.threads", "1", PropertyType.COUNT,
      "The master delegates bulk import RFile processing and assignment to tablet servers. After file has been processed, the tablet server will assign"
          + " the file to the appropriate tablets on all servers. This property controls the number of threads used to communicate to the other servers."),
  TSERV_BULK_RETRY("tserver.bulk.retry.max", "5", PropertyType.COUNT,
      "The number of times the tablet server will attempt to assign a RFile to a tablet as it migrates and splits."),
  TSERV_BULK_TIMEOUT("tserver.bulk.timeout", "5m", PropertyType.TIMEDURATION, "The time to wait for a tablet server to process a bulk import request."),
  TSERV_MINTHREADS("tserver.server.threads.minimum", "20", PropertyType.COUNT, "The minimum number of threads to use to handle incoming requests."),
  TSERV_THREADCHECK("tserver.server.threadcheck.time", "1s", PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool."),
  TSERV_MAX_MESSAGE_SIZE("tserver.server.message.size.max", "1G", PropertyType.BYTES, "The maximum size of a message that can be sent to a tablet server."),
  TSERV_HOLD_TIME_SUICIDE("tserver.hold.time.max", "5m", PropertyType.TIMEDURATION,
      "The maximum time for a tablet server to be in the \"memory full\" state. If the tablet server cannot write out memory"
          + " in this much time, it will assume there is some failure local to its node, and quit. A value of zero is equivalent to forever."),
  TSERV_WAL_BLOCKSIZE("tserver.wal.blocksize", "0", PropertyType.BYTES,
      "The size of the HDFS blocks used to write to the Write-Ahead log. If zero, it will be 110% of tserver.walog.max.size (that is, try to use just one"
          + " block)"),
  TSERV_WAL_REPLICATION("tserver.wal.replication", "0", PropertyType.COUNT,
      "The replication to use when writing the Write-Ahead log to HDFS. If zero, it will use the HDFS default replication setting."),
  TSERV_RECOVERY_MAX_CONCURRENT("tserver.recovery.concurrent.max", "2", PropertyType.COUNT, "The maximum number of threads to use to sort logs during"
      + " recovery"),
  TSERV_SORT_BUFFER_SIZE("tserver.sort.buffer.size", "10%", PropertyType.MEMORY, "The amount of memory to use when sorting logs during recovery."),
  TSERV_ARCHIVE_WALOGS("tserver.archive.walogs", "false", PropertyType.BOOLEAN, "Keep copies of the WALOGs for debugging purposes"),
  TSERV_WORKQ_THREADS("tserver.workq.threads", "2", PropertyType.COUNT,
      "The number of threads for the distributed work queue. These threads are used for copying failed bulk import RFiles."),
  TSERV_WAL_SYNC("tserver.wal.sync", "true", PropertyType.BOOLEAN,
      "Use the SYNC_BLOCK create flag to sync WAL writes to disk. Prevents problems recovering from sudden system resets."),
  @Deprecated
  TSERV_WAL_SYNC_METHOD("tserver.wal.sync.method", "hsync", PropertyType.STRING, "This property is deprecated. Use table.durability instead."),
  TSERV_ASSIGNMENT_DURATION_WARNING("tserver.assignment.duration.warning", "10m", PropertyType.TIMEDURATION, "The amount of time an assignment can run "
      + " before the server will print a warning along with the current stack trace. Meant to help debug stuck assignments"),
  TSERV_REPLICATION_REPLAYERS("tserver.replication.replayer.", null, PropertyType.PREFIX,
      "Allows configuration of implementation used to apply replicated data"),
  TSERV_REPLICATION_DEFAULT_HANDLER("tserver.replication.default.replayer", "org.apache.accumulo.tserver.replication.BatchWriterReplicationReplayer",
      PropertyType.CLASSNAME, "Default AccumuloReplicationReplayer implementation"),
  TSERV_REPLICATION_BW_REPLAYER_MEMORY("tserver.replication.batchwriter.replayer.memory", "50M", PropertyType.BYTES,
      "Memory to provide to batchwriter to replay mutations for replication"),
  TSERV_ASSIGNMENT_MAXCONCURRENT("tserver.assignment.concurrent.max", "2", PropertyType.COUNT,
      "The number of threads available to load tablets. Recoveries are still performed serially."),
  TSERV_SLOW_FLUSH_MILLIS("tserver.slow.flush.time", "100ms", PropertyType.TIMEDURATION,
      "If a flush to the write-ahead log takes longer than this period of time, debugging information will written, and may result in a log rollover."),
  TSERV_SUMMARY_PARTITION_THREADS("tserver.summary.partition.threads", "10", PropertyType.COUNT,
      "Summary data must be retrieved from RFiles.  For a large number of RFiles, the files are broken into partitions of 100K files.  This setting determines "
          + "how many of these groups of 100K RFiles will be processed concurrently."),
  TSERV_SUMMARY_REMOTE_THREADS("tserver.summary.remote.threads", "128", PropertyType.COUNT,
      "For a partitioned group of 100K RFiles, those files are grouped by tablet server.  Then a remote tablet server is asked to gather summary data.  This "
          + "setting determines how many concurrent request are made per partition."),
  TSERV_SUMMARY_RETRIEVAL_THREADS("tserver.summary.retrieval.threads", "10", PropertyType.COUNT,
      "The number of threads on each tablet server available to retrieve summary data, that is not currently in cache, from RFiles."),

  // accumulo garbage collector properties
  GC_PREFIX("gc.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the accumulo garbage collector."),
  GC_CYCLE_START("gc.cycle.start", "30s", PropertyType.TIMEDURATION, "Time to wait before attempting to garbage collect any old RFiles or write-ahead logs."),
  GC_CYCLE_DELAY("gc.cycle.delay", "5m", PropertyType.TIMEDURATION, "Time between garbage collection cycles. In each cycle, old RFiles or write-ahead logs "
      + "no longer in use are removed from the filesystem."),
  GC_PORT("gc.port.client", "9998", PropertyType.PORT, "The listening port for the garbage collector's monitor service"),
  GC_DELETE_THREADS("gc.threads.delete", "16", PropertyType.COUNT, "The number of threads used to delete RFiles and write-ahead logs"),
  GC_TRASH_IGNORE("gc.trash.ignore", "false", PropertyType.BOOLEAN, "Do not use the Trash, even if it is configured."),
  GC_FILE_ARCHIVE("gc.file.archive", "false", PropertyType.BOOLEAN, "Archive any files/directories instead of moving to the HDFS trash or deleting."),
  GC_TRACE_PERCENT("gc.trace.percent", "0.01", PropertyType.FRACTION, "Percent of gc cycles to trace"),

  // properties that are specific to the monitor server behavior
  MONITOR_PREFIX("monitor.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of the monitor web server."),
  MONITOR_PORT("monitor.port.client", "9995", PropertyType.PORT, "The listening port for the monitor's http service"),
  MONITOR_LOG4J_PORT("monitor.port.log4j", "4560", PropertyType.PORT, "The listening port for the monitor's log4j logging collection."),
  @Deprecated
  MONITOR_BANNER_TEXT("monitor.banner.text", "", PropertyType.STRING, "The banner text displayed on the monitor page."),
  @Deprecated
  MONITOR_BANNER_COLOR("monitor.banner.color", "#c4c4c4", PropertyType.STRING, "The color of the banner text displayed on the monitor page."),
  @Deprecated
  MONITOR_BANNER_BACKGROUND("monitor.banner.background", "#304065", PropertyType.STRING,
      "The background color of the banner text displayed on the monitor page."),

  MONITOR_SSL_KEYSTORE("monitor.ssl.keyStore", "", PropertyType.PATH, "The keystore for enabling monitor SSL."),
  @Sensitive
  MONITOR_SSL_KEYSTOREPASS("monitor.ssl.keyStorePassword", "", PropertyType.STRING, "The keystore password for enabling monitor SSL."),
  MONITOR_SSL_KEYSTORETYPE("monitor.ssl.keyStoreType", "jks", PropertyType.STRING, "Type of SSL keystore"),
  MONITOR_SSL_TRUSTSTORE("monitor.ssl.trustStore", "", PropertyType.PATH, "The truststore for enabling monitor SSL."),
  @Sensitive
  MONITOR_SSL_TRUSTSTOREPASS("monitor.ssl.trustStorePassword", "", PropertyType.STRING, "The truststore password for enabling monitor SSL."),
  MONITOR_SSL_TRUSTSTORETYPE("monitor.ssl.trustStoreType", "jks", PropertyType.STRING, "Type of SSL truststore"),
  MONITOR_SSL_INCLUDE_CIPHERS("monitor.ssl.include.ciphers", "", PropertyType.STRING,
      "A comma-separated list of allows SSL Ciphers, see monitor.ssl.exclude.ciphers to disallow ciphers"),
  MONITOR_SSL_EXCLUDE_CIPHERS("monitor.ssl.exclude.ciphers", "", PropertyType.STRING,
      "A comma-separated list of disallowed SSL Ciphers, see mmonitor.ssl.include.ciphers to allow ciphers"),
  MONITOR_SSL_INCLUDE_PROTOCOLS("monitor.ssl.include.protocols", "TLSv1,TLSv1.1,TLSv1.2", PropertyType.STRING, "A comma-separate list of allowed SSL protocols"),

  MONITOR_LOCK_CHECK_INTERVAL("monitor.lock.check.interval", "5s", PropertyType.TIMEDURATION,
      "The amount of time to sleep between checking for the Montior ZooKeeper lock"),
  MONITOR_LOG_DATE_FORMAT("monitor.log.date.format", "yyyy/MM/dd HH:mm:ss,SSS", PropertyType.STRING, "The SimpleDateFormat string used to configure "
      + "the date shown on the 'Recent Logs' monitor page"),
  MONITOR_RESOURCES_EXTERNAL("monitor.resources.external", "", PropertyType.STRING,
      "A JSON Map of Strings. Each String should be an HTML tag of an external resource (JS or CSS) to be imported by the Monitor. \n"
          + "Be sure to wrap with CDATA tags. If this value is set, all of the external resources in the <head> tag of the Monitor will be replaced with \n"
          + "the tags set here. Be sure the jquery tag is first since other scripts will depend on it. The resources that are used by default "
          + "can be seen in accumulo/server/monitor/src/main/resources/templates/default.ftl"),

  TRACE_PREFIX("trace.", null, PropertyType.PREFIX, "Properties in this category affect the behavior of distributed tracing."),
  TRACE_SPAN_RECEIVERS("trace.span.receivers", "org.apache.accumulo.tracer.ZooTraceClient", PropertyType.CLASSNAMELIST,
      "A list of span receiver classes to send trace spans"),
  TRACE_SPAN_RECEIVER_PREFIX("trace.span.receiver.", null, PropertyType.PREFIX, "Prefix for span receiver configuration properties"),
  TRACE_ZK_PATH("trace.zookeeper.path", Constants.ZTRACERS, PropertyType.STRING, "The zookeeper node where tracers are registered"),
  TRACE_PORT("trace.port.client", "12234", PropertyType.PORT, "The listening port for the trace server"),
  TRACE_TABLE("trace.table", "trace", PropertyType.STRING, "The name of the table to store distributed traces"),
  TRACE_USER("trace.user", "root", PropertyType.STRING, "The name of the user to store distributed traces"),
  @Sensitive
  TRACE_PASSWORD("trace.password", "secret", PropertyType.STRING, "The password for the user used to store distributed traces"),
  @Sensitive
  TRACE_TOKEN_PROPERTY_PREFIX("trace.token.property.", null, PropertyType.PREFIX,
      "The prefix used to create a token for storing distributed traces. For each property required by trace.token.type, place this prefix in front of it."),
  TRACE_TOKEN_TYPE("trace.token.type", PasswordToken.class.getName(), PropertyType.CLASSNAME, "An AuthenticationToken type supported by the authorizer"),

  // per table properties
  TABLE_PREFIX("table.", null, PropertyType.PREFIX, "Properties in this category affect tablet server treatment of tablets, but can be configured "
      + "on a per-table basis. Setting these properties in the site file will override the default globally "
      + "for all tables and not any specific table. However, both the default and the global setting can be "
      + "overridden per table using the table operations API or in the shell, which sets the overridden value "
      + "in zookeeper. Restarting accumulo tablet servers after setting these properties in the site file "
      + "will cause the global setting to take effect. However, you must use the API or the shell to change "
      + "properties in zookeeper that are set on a table."),
  TABLE_ARBITRARY_PROP_PREFIX("table.custom.", null, PropertyType.PREFIX, "Prefix to be used for user defined arbitrary properties."),
  TABLE_MAJC_RATIO("table.compaction.major.ratio", "3", PropertyType.FRACTION,
      "Minimum ratio of total input size to maximum input RFile size for running a major compaction. When adjusting this property you may want to also "
          + "adjust table.file.max. Want to avoid the situation where only merging minor compactions occur."),
  TABLE_MAJC_COMPACTALL_IDLETIME("table.compaction.major.everything.idle", "1h", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have all "
          + "of its RFiles compacted into one. There is no guarantee an idle tablet will be compacted. "
          + "Compactions of idle tablets are only started when regular compactions are not running. Idle "
          + "compactions only take place for tablets that have one or more RFiles."),
  TABLE_SPLIT_THRESHOLD("table.split.threshold", "1G", PropertyType.BYTES, "A tablet is split when the combined size of RFiles exceeds this amount."),
  TABLE_MAX_END_ROW_SIZE("table.split.endrow.size.max", "10K", PropertyType.BYTES, "Maximum size of end row"),
  TABLE_MINC_LOGS_MAX("table.compaction.minor.logs.threshold", "3", PropertyType.COUNT,
      "When there are more than this many write-ahead logs against a tablet, it will be minor compacted. See comment for property tserver.memory.maps.max"),
  TABLE_MINC_COMPACT_IDLETIME("table.compaction.minor.idle", "5m", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have its "
          + "in-memory map flushed to disk in a minor compaction. There is no guarantee an idle " + "tablet will be compacted."),
  TABLE_MINC_MAX_MERGE_FILE_SIZE("table.compaction.minor.merge.file.size.max", "0", PropertyType.BYTES,
      "The max RFile size used for a merging minor compaction. The default value of 0 disables a max file size."),
  TABLE_SCAN_MAXMEM("table.scan.max.memory", "512K", PropertyType.BYTES,
      "The maximum amount of memory that will be used to cache results of a client query/scan. "
          + "Once this limit is reached, the buffered data is sent to the client."),
  TABLE_FILE_TYPE("table.file.type", RFile.EXTENSION, PropertyType.STRING, "Change the type of file a table writes"),
  TABLE_LOAD_BALANCER("table.balancer", "org.apache.accumulo.server.master.balancer.DefaultLoadBalancer", PropertyType.STRING,
      "This property can be set to allow the LoadBalanceByTable load balancer to change the called Load Balancer for this table"),
  TABLE_FILE_COMPRESSION_TYPE("table.file.compress.type", "gz", PropertyType.STRING,
      "Compression algorithm used on index and data blocks before they are written. Possible values: gz, snappy, lzo, none"),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE("table.file.compress.blocksize", "100K", PropertyType.BYTES,
      "The maximum size of data blocks in RFiles before they are compressed and written."),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX("table.file.compress.blocksize.index", "128K", PropertyType.BYTES,
      "The maximum size of index blocks in RFiles before they are compressed and written."),
  TABLE_FILE_BLOCK_SIZE("table.file.blocksize", "0B", PropertyType.BYTES,
      "The HDFS block size used when writing RFiles. When set to 0B, the value/defaults of HDFS property 'dfs.block.size' will be used."),
  TABLE_FILE_REPLICATION("table.file.replication", "0", PropertyType.COUNT,
      "The number of replicas for a table's RFiles in HDFS. When set to 0, HDFS defaults are used."),
  TABLE_FILE_MAX("table.file.max", "15", PropertyType.COUNT,
      "The maximum number of RFiles each tablet in a table can have. When adjusting this property you may want to consider adjusting"
          + " table.compaction.major.ratio also. Setting this property to 0 will make it default to tserver.scan.files.open.max-1, this will prevent a"
          + " tablet from having more RFiles than can be opened. Setting this property low may throttle ingest and increase query performance."),
  TABLE_FILE_SUMMARY_MAX_SIZE("table.file.summary.maxSize", "256K", PropertyType.BYTES, "The maximum size summary that will be stored. The number of"
      + " RFiles that had summary data exceeding this threshold is reported by Summary.getFileStatistics().getLarge().  When adjusting this"
      + " consider the expected number RFiles with summaries on each tablet server and the summary cache size."),
  @Deprecated
  TABLE_WALOG_ENABLED("table.walog.enabled", "true", PropertyType.BOOLEAN, "This setting is deprecated.  Use table.durability=none instead."),
  TABLE_BLOOM_ENABLED("table.bloom.enabled", "false", PropertyType.BOOLEAN, "Use bloom filters on this table."),
  TABLE_BLOOM_LOAD_THRESHOLD("table.bloom.load.threshold", "1", PropertyType.COUNT,
      "This number of seeks that would actually use a bloom filter must occur before a RFile's bloom filter is loaded."
          + " Set this to zero to initiate loading of bloom filters when a RFile is opened."),
  TABLE_BLOOM_SIZE("table.bloom.size", "1048576", PropertyType.COUNT, "Bloom filter size, as number of keys."),
  TABLE_BLOOM_ERRORRATE("table.bloom.error.rate", "0.5%", PropertyType.FRACTION, "Bloom filter error rate."),
  TABLE_BLOOM_KEY_FUNCTOR("table.bloom.key.functor", "org.apache.accumulo.core.file.keyfunctor.RowFunctor", PropertyType.CLASSNAME,
      "A function that can transform the key prior to insertion and check of bloom filter. org.apache.accumulo.core.file.keyfunctor.RowFunctor,"
          + ",org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor, and org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor are"
          + " allowable values. One can extend any of the above mentioned classes to perform specialized parsing of the key. "),
  TABLE_BLOOM_HASHTYPE("table.bloom.hash.type", "murmur", PropertyType.STRING, "The bloom filter hash type"),
  TABLE_DURABILITY("table.durability", "sync", PropertyType.DURABILITY, "The durability used to write to the write-ahead log."
      + " Legal values are: none, which skips the write-ahead log; "
      + "log, which sends the data to the write-ahead log, but does nothing to make it durable; " + "flush, which pushes data to the file system; and "
      + "sync, which ensures the data is written to disk."),
  TABLE_FAILURES_IGNORE("table.failures.ignore", "false", PropertyType.BOOLEAN,
      "If you want queries for your table to hang or fail when data is missing from the system, "
          + "then set this to false. When this set to true missing data will be reported but queries "
          + "will still run possibly returning a subset of the data."),
  TABLE_DEFAULT_SCANTIME_VISIBILITY("table.security.scan.visibility.default", "", PropertyType.STRING,
      "The security label that will be assumed at scan time if an entry does not have a visibility set.\n"
          + "Note: An empty security label is displayed as []. The scan results will show an empty visibility even if "
          + "the visibility from this setting is applied to the entry.\n"
          + "CAUTION: If a particular key has an empty security label AND its table's default visibility is also empty, "
          + "access will ALWAYS be granted for users with permission to that table. Additionally, if this field is changed, "
          + "all existing data with an empty visibility label will be interpreted with the new label on the next scan."),
  TABLE_LOCALITY_GROUPS("table.groups.enabled", "", PropertyType.STRING, "A comma separated list of locality group names to enable for this table."),
  TABLE_CONSTRAINT_PREFIX("table.constraint.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that add constraints to a table. "
          + "These properties start with the category prefix, followed by a number, and their values "
          + "correspond to a fully qualified Java class that implements the Constraint interface.\n"
          + "For example:\ntable.constraint.1 = org.apache.accumulo.core.constraints.MyCustomConstraint\n"
          + "and:\ntable.constraint.2 = my.package.constraints.MySecondConstraint"),
  TABLE_INDEXCACHE_ENABLED("table.cache.index.enable", "true", PropertyType.BOOLEAN, "Determines whether index block cache is enabled for a table."),
  TABLE_BLOCKCACHE_ENABLED("table.cache.block.enable", "false", PropertyType.BOOLEAN, "Determines whether data block cache is enabled for a table."),
  TABLE_ITERATOR_PREFIX("table.iterator.", null, PropertyType.PREFIX,
      "Properties in this category specify iterators that are applied at various stages (scopes) of interaction "
          + "with a table. These properties start with the category prefix, followed by a scope (minc, majc, scan, etc.), "
          + "followed by a period, followed by a name, as in table.iterator.scan.vers, or table.iterator.scan.custom. "
          + "The values for these properties are a number indicating the ordering in which it is applied, and a class name "
          + "such as:\n table.iterator.scan.vers = 10,org.apache.accumulo.core.iterators.VersioningIterator\n "
          + "These iterators can take options if additional properties are set that look like this property, "
          + "but are suffixed with a period, followed by 'opt' followed by another period, and a property name.\n"
          + "For example, table.iterator.minc.vers.opt.maxVersions = 3"),
  TABLE_ITERATOR_SCAN_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.scan.name() + ".", null, PropertyType.PREFIX,
      "Convenience prefix to find options for the scan iterator scope"),
  TABLE_ITERATOR_MINC_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.minc.name() + ".", null, PropertyType.PREFIX,
      "Convenience prefix to find options for the minc iterator scope"),
  TABLE_ITERATOR_MAJC_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.majc.name() + ".", null, PropertyType.PREFIX,
      "Convenience prefix to find options for the majc iterator scope"),
  TABLE_LOCALITY_GROUP_PREFIX("table.group.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that define locality groups in a table. These properties start "
          + "with the category prefix, followed by a name, followed by a period, and followed by a property for that group.\n"
          + "For example table.group.group1=x,y,z sets the column families for a group called group1. Once configured, "
          + "group1 can be enabled by adding it to the list of groups in the " + TABLE_LOCALITY_GROUPS.getKey() + " property.\n"
          + "Additional group options may be specified for a named group by setting table.group.<name>.opt.<key>=<value>."),
  TABLE_FORMATTER_CLASS("table.formatter", DefaultFormatter.class.getName(), PropertyType.STRING, "The Formatter class to apply on results in the shell"),
  TABLE_INTERPRETER_CLASS("table.interepreter", DefaultScanInterpreter.class.getName(), PropertyType.STRING,
      "The ScanInterpreter class to apply on scan arguments in the shell"),
  TABLE_CLASSPATH("table.classpath.context", "", PropertyType.STRING, "Per table classpath context"),
  TABLE_COMPACTION_STRATEGY("table.majc.compaction.strategy", "org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy", PropertyType.CLASSNAME,
      "A customizable major compaction strategy."),
  TABLE_COMPACTION_STRATEGY_PREFIX("table.majc.compaction.strategy.opts.", null, PropertyType.PREFIX,
      "Properties in this category are used to configure the compaction strategy."),
  TABLE_REPLICATION("table.replication", "false", PropertyType.BOOLEAN, "Is replication enabled for the given table"),
  TABLE_REPLICATION_TARGET("table.replication.target.", null, PropertyType.PREFIX, "Enumerate a mapping of other systems which this table should "
      + "replicate their data to. The key suffix is the identifying cluster name and the value is an identifier for a location on the target system, "
      + "e.g. the ID of the table on the target to replicate to"),
  TABLE_SAMPLER(
      "table.sampler",
      "",
      PropertyType.CLASSNAME,
      "The name of a class that implements org.apache.accumulo.core.Sampler.  Setting this option enables storing a sample of data which can be scanned."
          + "  Always having a current sample can useful for query optimization and data comprehension.   After enabling sampling for an existing table, a compaction "
          + "is needed to compute the sample for existing data.  The compact command in the shell has an option to only compact RFiles without sample data."),
  TABLE_SAMPLER_OPTS("table.sampler.opt.", null, PropertyType.PREFIX,
      "The property is used to set options for a sampler.  If a sample had two options like hasher and modulous, then the two properties "
          + "table.sampler.opt.hasher=${hash algorithm} and table.sampler.opt.modulous=${mod} would be set."),
  TABLE_SUSPEND_DURATION("table.suspend.duration", "0s", PropertyType.TIMEDURATION,
      "For tablets belonging to this table: When a tablet server dies, allow the tablet server this duration to revive before reassigning its tablets"
          + "to other tablet servers."),
  TABLE_SUMMARIZER_PREFIX(
      "table.summarizer.",
      null,
      PropertyType.PREFIX,
      "Prefix for configuring summarizers for a table.  Using this prefix multiple summarizers can be configured with options for each one. Each summarizer configured "
          + "should have a unique id, this id can be anything. To add a summarizer set table.summarizer.<unique id>=<summarizer class name>.  If the summarizer has options, "
          + "then for each option set table.summarizer.<unique id>.opt.<key>=<value>."),

  // VFS ClassLoader properties
  VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY(AccumuloVFSClassLoader.VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY, "", PropertyType.STRING,
      "Configuration for a system level vfs classloader. Accumulo jar can be configured here and loaded out of HDFS."),
  VFS_CONTEXT_CLASSPATH_PROPERTY(AccumuloVFSClassLoader.VFS_CONTEXT_CLASSPATH_PROPERTY, null, PropertyType.PREFIX,
      "Properties in this category are define a classpath. These properties start  with the category prefix, followed by a context name. "
          + "The value is a comma seperated list of URIs. Supports full regex on filename alone. For example, "
          + "general.vfs.context.classpath.cx1=hdfs://nn1:9902/mylibdir/*.jar. "
          + "You can enable post delegation for a context, which will load classes from the context first instead of the parent first. "
          + "Do this by setting general.vfs.context.classpath.<name>.delegation=post, where <name> is your context name"
          + "If delegation is not specified, it defaults to loading from parent classloader first."),
  @Interpolated
  VFS_CLASSLOADER_CACHE_DIR(AccumuloVFSClassLoader.VFS_CACHE_DIR, "${java.io.tmpdir}" + File.separator + "accumulo-vfs-cache-${user.name}",
      PropertyType.ABSOLUTEPATH, "Directory to use for the vfs cache. The cache will keep a soft reference to all of the classes loaded in the VM."
          + " This should be on local disk on each node with sufficient space. It defaults to ${java.io.tmpdir}/accumulo-vfs-cache-${user.name}"),

  @Interpolated
  @Experimental
  GENERAL_MAVEN_PROJECT_BASEDIR(AccumuloClassLoader.MAVEN_PROJECT_BASEDIR_PROPERTY_NAME, AccumuloClassLoader.DEFAULT_MAVEN_PROJECT_BASEDIR_VALUE,
      PropertyType.ABSOLUTEPATH, "Set this to automatically add maven target/classes directories to your dynamic classpath"),

  // General properties for configuring replication
  REPLICATION_PREFIX("replication.", null, PropertyType.PREFIX, "Properties in this category affect the replication of data to other Accumulo instances."),
  REPLICATION_PEERS("replication.peer.", null, PropertyType.PREFIX, "Properties in this category control what systems data can be replicated to"),
  REPLICATION_PEER_USER("replication.peer.user.", null, PropertyType.PREFIX, "The username to provide when authenticating with the given peer"),
  @Sensitive
  REPLICATION_PEER_PASSWORD("replication.peer.password.", null, PropertyType.PREFIX, "The password to provide when authenticating with the given peer"),
  REPLICATION_PEER_KEYTAB("replication.peer.keytab.", null, PropertyType.PREFIX, "The keytab to use when authenticating with the given peer"),
  REPLICATION_NAME("replication.name", "", PropertyType.STRING,
      "Name of this cluster with respect to replication. Used to identify this instance from other peers"),
  REPLICATION_MAX_WORK_QUEUE("replication.max.work.queue", "1000", PropertyType.COUNT, "Upper bound of the number of files queued for replication"),
  REPLICATION_WORK_ASSIGNMENT_SLEEP("replication.work.assignment.sleep", "30s", PropertyType.TIMEDURATION,
      "Amount of time to sleep between replication work assignment"),
  REPLICATION_WORKER_THREADS("replication.worker.threads", "4", PropertyType.COUNT, "Size of the threadpool that each tabletserver devotes to replicating data"),
  REPLICATION_RECEIPT_SERVICE_PORT("replication.receipt.service.port", "10002", PropertyType.PORT,
      "Listen port used by thrift service in tserver listening for replication"),
  REPLICATION_WORK_ATTEMPTS("replication.work.attempts", "10", PropertyType.COUNT,
      "Number of attempts to try to replicate some data before giving up and letting it naturally be retried later"),
  REPLICATION_MIN_THREADS("replication.receiver.min.threads", "1", PropertyType.COUNT, "Minimum number of threads for replication"),
  REPLICATION_THREADCHECK("replication.receiver.threadcheck.time", "30s", PropertyType.TIMEDURATION,
      "The time between adjustments of the replication thread pool."),
  REPLICATION_MAX_UNIT_SIZE("replication.max.unit.size", "64M", PropertyType.BYTES, "Maximum size of data to send in a replication message"),
  REPLICATION_WORK_ASSIGNER("replication.work.assigner", "org.apache.accumulo.master.replication.UnorderedWorkAssigner", PropertyType.CLASSNAME,
      "Replication WorkAssigner implementation to use"),
  REPLICATION_DRIVER_DELAY("replication.driver.delay", "0s", PropertyType.TIMEDURATION,
      "Amount of time to wait before the replication work loop begins in the master."),
  REPLICATION_WORK_PROCESSOR_DELAY("replication.work.processor.delay", "0s", PropertyType.TIMEDURATION,
      "Amount of time to wait before first checking for replication work, not useful outside of tests"),
  REPLICATION_WORK_PROCESSOR_PERIOD("replication.work.processor.period", "0s", PropertyType.TIMEDURATION,
      "Amount of time to wait before re-checking for replication work, not useful outside of tests"),
  REPLICATION_TRACE_PERCENT("replication.trace.percent", "0.1", PropertyType.FRACTION, "The sampling percentage to use for replication traces"),
  REPLICATION_RPC_TIMEOUT("replication.rpc.timeout", "2m", PropertyType.TIMEDURATION,
      "Amount of time for a single replication RPC call to last before failing the attempt. See replication.work.attempts."),

  ;

  private String key, defaultValue, description;
  private PropertyType type;

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

  /**
   * Gets the key (string) for this property.
   *
   * @return key
   */
  public String getKey() {
    return this.key;
  }

  /**
   * Gets the default value for this property exactly as provided in its definition (i.e., without interpolation or conversion to absolute paths).
   *
   * @return raw default value
   */
  public String getRawDefaultValue() {
    return this.defaultValue;
  }

  /**
   * Gets the default value for this property. System properties are interpolated into the value if necessary.
   *
   * @return default value
   */
  public String getDefaultValue() {
    String v;
    if (isInterpolated()) {
      PropertiesConfiguration pconf = new PropertiesConfiguration();
      Properties systemProperties = System.getProperties();
      synchronized (systemProperties) {
        pconf.append(new MapConfiguration(systemProperties));
      }
      pconf.addProperty("hack_default_value", this.defaultValue);
      v = pconf.getString("hack_default_value");
    } else {
      v = getRawDefaultValue();
    }
    if (this.type == PropertyType.ABSOLUTEPATH && !(v.trim().equals("")))
      v = new File(v).getAbsolutePath();
    return v;
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

  private boolean isInterpolated() {
    return hasAnnotation(Interpolated.class) || hasPrefixWithAnnotation(getKey(), Interpolated.class);
  }

  /**
   * Checks if this property is experimental.
   *
   * @return true if this property is experimental
   */
  public boolean isExperimental() {
    return hasAnnotation(Experimental.class) || hasPrefixWithAnnotation(getKey(), Experimental.class);
  }

  /**
   * Checks if this property is deprecated.
   *
   * @return true if this property is deprecated
   */
  public boolean isDeprecated() {
    return hasAnnotation(Deprecated.class) || hasPrefixWithAnnotation(getKey(), Deprecated.class);
  }

  private volatile Boolean isSensitive = null;

  /**
   * Checks if this property is sensitive.
   *
   * @return true if this property is sensitive
   */
  public boolean isSensitive() {
    if (isSensitive == null) {
      isSensitive = hasAnnotation(Sensitive.class) || hasPrefixWithAnnotation(getKey(), Sensitive.class);
    }
    return isSensitive.booleanValue();
  }

  /**
   * Checks if a property with the given key is sensitive. The key must be for a valid property, and must either itself be annotated as sensitive or have a
   * prefix annotated as sensitive.
   *
   * @param key
   *          property key
   * @return true if property is sensitive
   */
  public static boolean isSensitive(String key) {
    return hasPrefixWithAnnotation(key, Sensitive.class);
  }

  private <T extends Annotation> boolean hasAnnotation(Class<T> annotationType) {
    Logger log = LoggerFactory.getLogger(getClass());
    try {
      for (Annotation a : getClass().getField(name()).getAnnotations())
        if (annotationType.isInstance(a))
          return true;
    } catch (SecurityException e) {
      log.error("{}", e.getMessage(), e);
    } catch (NoSuchFieldException e) {
      log.error("{}", e.getMessage(), e);
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

  /**
   * Checks if the given property key is valid. A valid property key is either equal to the key of some defined property or has a prefix matching some prefix
   * defined in this class.
   *
   * @param key
   *          property key
   * @return true if key is valid (recognized, or has a recognized prefix)
   */
  public synchronized static boolean isValidPropertyKey(String key) {
    if (validProperties == null) {
      validProperties = new HashSet<>();
      validPrefixes = new HashSet<>();

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

  /**
   * Checks if the given property key is for a valid table property. A valid table property key is either equal to the key of some defined table property (which
   * each start with {@link #TABLE_PREFIX}) or has a prefix matching {@link #TABLE_CONSTRAINT_PREFIX}, {@link #TABLE_ITERATOR_PREFIX}, or
   * {@link #TABLE_LOCALITY_GROUP_PREFIX}.
   *
   * @param key
   *          property key
   * @return true if key is valid for a table property (recognized, or has a recognized prefix)
   */
  public synchronized static boolean isValidTablePropertyKey(String key) {
    if (validTableProperties == null) {
      validTableProperties = new HashSet<>();
      for (Property p : Property.values()) {
        if (!p.getType().equals(PropertyType.PREFIX) && p.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
          validTableProperties.add(p.getKey());
        }
      }
    }

    return validTableProperties.contains(key) || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())
        || key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) || key.startsWith(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey())
        || key.startsWith(Property.TABLE_COMPACTION_STRATEGY_PREFIX.getKey()) || key.startsWith(Property.TABLE_REPLICATION_TARGET.getKey())
        || key.startsWith(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey()) || key.startsWith(TABLE_SAMPLER_OPTS.getKey())
        || key.startsWith(TABLE_SUMMARIZER_PREFIX.getKey());
  }

  /**
   * Properties we check the value of within the TabletServer request handling or maintenance processing loops.
   */
  public static final EnumSet<Property> HOT_PATH_PROPERTIES = EnumSet.of(Property.TSERV_CLIENT_TIMEOUT, Property.TSERV_TOTAL_MUTATION_QUEUE_MAX,
      Property.TSERV_ARCHIVE_WALOGS, Property.GC_TRASH_IGNORE, Property.TSERV_MAJC_DELAY, Property.TABLE_MINC_LOGS_MAX, Property.TSERV_MAJC_MAXCONCURRENT,
      Property.REPLICATION_WORKER_THREADS, Property.TABLE_DURABILITY, Property.INSTANCE_ZK_TIMEOUT, Property.TABLE_CLASSPATH,
      Property.MASTER_METADATA_SUSPENDABLE);

  private static final EnumSet<Property> fixedProperties = EnumSet.of(Property.TSERV_CLIENTPORT, Property.TSERV_NATIVEMAP_ENABLED,
      Property.TSERV_SCAN_MAX_OPENFILES, Property.MASTER_CLIENTPORT, Property.GC_PORT);

  /**
   * Checks if the given property may be changed via Zookeeper, but not recognized until the restart of some relevant daemon.
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
    return key.startsWith(Property.TABLE_PREFIX.getKey()) || key.startsWith(Property.TSERV_PREFIX.getKey()) || key.startsWith(Property.MASTER_PREFIX.getKey())
        || key.startsWith(Property.GC_PREFIX.getKey()) || key.startsWith(Property.MONITOR_PREFIX.getKey() + "banner.")
        || key.startsWith(VFS_CONTEXT_CLASSPATH_PROPERTY.getKey()) || key.startsWith(Property.TABLE_COMPACTION_STRATEGY_PREFIX.getKey())
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
    for (Property prop : Property.values())
      if (prop.getKey().equals(key))
        return prop;
    return null;
  }

  /**
   * Checks if this property is expected to have a Java class as a value.
   *
   * @return true if this is property is a class property
   */
  public static boolean isClassProperty(String key) {
    return (key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey()) && key.substring(Property.TABLE_CONSTRAINT_PREFIX.getKey().length()).split("\\.").length == 1)
        || (key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) && key.substring(Property.TABLE_ITERATOR_PREFIX.getKey().length()).split("\\.").length == 2)
        || key.equals(Property.TABLE_LOAD_BALANCER.getKey());
  }

  /**
   * Creates a new instance of a class specified in a configuration property. The table classpath context is used if set.
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
   * @see AccumuloVFSClassLoader
   */
  public static <T> T createTableInstanceFromPropertyName(AccumuloConfiguration conf, Property property, Class<T> base, T defaultInstance) {
    String clazzName = conf.get(property);
    String context = conf.get(TABLE_CLASSPATH);
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
   * @see AccumuloVFSClassLoader
   */
  public static <T> T createInstanceFromPropertyName(AccumuloConfiguration conf, Property property, Class<T> base, T defaultInstance) {
    String clazzName = conf.get(property);
    return ConfigurationTypeHelper.getClassInstance(null, clazzName, base, defaultInstance);
  }

  /**
   * Collects together properties from the given configuration pertaining to compaction strategies. The relevant properties all begin with the prefix in
   * {@link #TABLE_COMPACTION_STRATEGY_PREFIX}. In the returned map, the prefix is removed from each property's key.
   *
   * @param tableConf
   *          configuration
   * @return map of compaction strategy property keys and values, with the detection prefix removed from each key
   */
  public static Map<String,String> getCompactionStrategyOptions(AccumuloConfiguration tableConf) {
    Map<String,String> longNames = tableConf.getAllPropertiesWithPrefix(Property.TABLE_COMPACTION_STRATEGY_PREFIX);
    Map<String,String> result = new HashMap<>();
    for (Entry<String,String> entry : longNames.entrySet()) {
      result.put(entry.getKey().substring(Property.TABLE_COMPACTION_STRATEGY_PREFIX.getKey().length()), entry.getValue());
    }
    return result;
  }
}
