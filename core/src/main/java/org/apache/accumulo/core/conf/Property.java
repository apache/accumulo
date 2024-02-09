/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.function.Predicate;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.data.constraints.NoDeleteConstraint;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.spi.scan.ScanPrioritizer;
import org.apache.accumulo.core.spi.scan.ScanServerSelector;
import org.apache.accumulo.core.spi.scan.SimpleScanDispatcher;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public enum Property {
  COMPACTION_PREFIX("compaction.", null, PropertyType.PREFIX,
      "Both major and minor compaction properties can be included under this prefix.", "3.1.0"),
  COMPACTION_SERVICE_PREFIX(COMPACTION_PREFIX + "service.", null, PropertyType.PREFIX,
      "This prefix should be used to define all properties for the compaction services."
          + "See {% jlink -f org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner %}.\n"
          + "A new external compaction service would be defined like the following:\n"
          + "`compaction.service.newService.planner="
          + "\"org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner\".`\n"
          + "`compaction.service.newService.opts.queues=\""
          + "[{\"name\": \"small\", \"maxSize\":\"32M\"},"
          + "{ \"name\":\"medium\", \"maxSize\":\"512M\"},{\"name\":\"large\"}]`\n"
          + "`compaction.service.newService.opts.maxOpen=50`.\n"
          + "Additional options can be defined using the `compaction.service.<service>.opts.<option>` property.",
      "3.1.0"),
  COMPACTION_WARN_TIME(COMPACTION_PREFIX + "warn.time", "10m", PropertyType.TIMEDURATION,
      "When a compaction has not made progress for this time period, a warning will be logged.",
      "3.1.0"),
  // SSL properties local to each node (see also instance.ssl.enabled which must be consistent
  // across all nodes in an instance)
  RPC_PREFIX("rpc.", null, PropertyType.PREFIX,
      "Properties in this category related to the configuration of SSL keys for"
          + " RPC. See also `instance.ssl.enabled`.",
      "1.6.0"),
  RPC_BACKLOG("rpc.backlog", "50", PropertyType.COUNT,
      "Configures the TCP backlog for the server side sockets created by Thrift."
          + " This property is not used for SSL type server sockets. A value of zero"
          + " will use the Thrift default value.",
      "2.1.3"),
  RPC_SSL_KEYSTORE_PATH("rpc.javax.net.ssl.keyStore", "", PropertyType.PATH,
      "Path of the keystore file for the server's private SSL key.", "1.6.0"),
  @Sensitive
  RPC_SSL_KEYSTORE_PASSWORD("rpc.javax.net.ssl.keyStorePassword", "", PropertyType.STRING,
      "Password used to encrypt the SSL private keystore. "
          + "Leave blank to use the Accumulo instance secret.",
      "1.6.0"),
  RPC_SSL_KEYSTORE_TYPE("rpc.javax.net.ssl.keyStoreType", "jks", PropertyType.STRING,
      "Type of SSL keystore.", "1.6.0"),
  RPC_SSL_TRUSTSTORE_PATH("rpc.javax.net.ssl.trustStore", "", PropertyType.PATH,
      "Path of the truststore file for the root cert.", "1.6.0"),
  @Sensitive
  RPC_SSL_TRUSTSTORE_PASSWORD("rpc.javax.net.ssl.trustStorePassword", "", PropertyType.STRING,
      "Password used to encrypt the SSL truststore. Leave blank to use no password.", "1.6.0"),
  RPC_SSL_TRUSTSTORE_TYPE("rpc.javax.net.ssl.trustStoreType", "jks", PropertyType.STRING,
      "Type of SSL truststore.", "1.6.0"),
  RPC_USE_JSSE("rpc.useJsse", "false", PropertyType.BOOLEAN,
      "Use JSSE system properties to configure SSL rather than the " + RPC_PREFIX.getKey()
          + "javax.net.ssl.* Accumulo properties.",
      "1.6.0"),
  RPC_SSL_CIPHER_SUITES("rpc.ssl.cipher.suites", "", PropertyType.STRING,
      "Comma separated list of cipher suites that can be used by accepted connections.", "1.6.1"),
  RPC_SSL_ENABLED_PROTOCOLS("rpc.ssl.server.enabled.protocols", "TLSv1.3", PropertyType.STRING,
      "Comma separated list of protocols that can be used to accept connections.", "1.6.2"),
  RPC_SSL_CLIENT_PROTOCOL("rpc.ssl.client.protocol", "TLSv1.3", PropertyType.STRING,
      "The protocol used to connect to a secure server. Must be in the list of enabled protocols "
          + "on the server side `rpc.ssl.server.enabled.protocols`.",
      "1.6.2"),
  RPC_SASL_QOP("rpc.sasl.qop", "auth", PropertyType.STRING,
      "The quality of protection to be used with SASL. Valid values are 'auth', 'auth-int',"
          + " and 'auth-conf'.",
      "1.7.0"),

  // instance properties (must be the same for every node in an instance)
  INSTANCE_PREFIX("instance.", null, PropertyType.PREFIX,
      "Properties in this category must be consistent throughout a cloud. "
          + "This is enforced and servers won't be able to communicate if these differ.",
      "1.3.5"),
  INSTANCE_ZK_HOST("instance.zookeeper.host", "localhost:2181", PropertyType.HOSTLIST,
      "Comma separated list of zookeeper servers.", "1.3.5"),
  INSTANCE_ZK_TIMEOUT("instance.zookeeper.timeout", "30s", PropertyType.TIMEDURATION,
      "Zookeeper session timeout; "
          + "max value when represented as milliseconds should be no larger than "
          + Integer.MAX_VALUE + ".",
      "1.3.5"),
  @Sensitive
  INSTANCE_SECRET("instance.secret", "DEFAULT", PropertyType.STRING,
      "A secret unique to a given instance that all servers must know in order"
          + " to communicate with one another. It should be changed prior to the"
          + " initialization of Accumulo. To change it after Accumulo has been"
          + " initialized, use the ChangeSecret tool and then update accumulo.properties"
          + " everywhere. Before using the ChangeSecret tool, make sure Accumulo is not"
          + " running and you are logged in as the user that controls Accumulo files in"
          + " HDFS. To use the ChangeSecret tool, run the command: `./bin/accumulo"
          + " org.apache.accumulo.server.util.ChangeSecret`.",
      "1.3.5"),
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
          + " encoding. For example replace commas with %2C.",
      "1.6.0"),
  INSTANCE_VOLUME_CONFIG_PREFIX("instance.volume.config.", null, PropertyType.PREFIX,
      "Properties in this category are used to provide volume specific overrides to "
          + "the general filesystem client configuration. Properties using this prefix "
          + "should be in the form "
          + "'instance.volume.config.<volume-uri>.<property-name>=<property-value>. An "
          + "example: "
          + "'instance.volume.config.hdfs://namespace-a:8020/accumulo.dfs.client.hedged.read.threadpool.size=10'. "
          + "Note that when specifying property names that contain colons in the properties "
          + "files that the colons need to be escaped with a backslash.",
      "2.1.1"),
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
          + "example replace commas with %2C.",
      "1.6.0"),
  INSTANCE_VOLUMES_UPGRADE_RELATIVE("instance.volumes.upgrade.relative", "", PropertyType.STRING,
      "The volume dfs uri containing relative tablet file paths. Relative paths may exist in the metadata from "
          + "versions prior to 1.6. This property is only required if a relative path is detected "
          + "during the upgrade process and will only be used once.",
      "2.1.0"),
  @Experimental // interface uses unstable internal types, use with caution
  INSTANCE_SECURITY_AUTHENTICATOR("instance.security.authenticator",
      "org.apache.accumulo.server.security.handler.ZKAuthenticator", PropertyType.CLASSNAME,
      "The authenticator class that accumulo will use to determine if a user "
          + "has privilege to perform an action.",
      "1.5.0"),
  @Experimental // interface uses unstable internal types, use with caution
  INSTANCE_SECURITY_AUTHORIZOR("instance.security.authorizor",
      "org.apache.accumulo.server.security.handler.ZKAuthorizor", PropertyType.CLASSNAME,
      "The authorizor class that accumulo will use to determine what labels a "
          + "user has privilege to see.",
      "1.5.0"),
  @Experimental // interface uses unstable internal types, use with caution
  INSTANCE_SECURITY_PERMISSION_HANDLER("instance.security.permissionHandler",
      "org.apache.accumulo.server.security.handler.ZKPermHandler", PropertyType.CLASSNAME,
      "The permission handler class that accumulo will use to determine if a "
          + "user has privilege to perform an action.",
      "1.5.0"),
  INSTANCE_RPC_SSL_ENABLED("instance.rpc.ssl.enabled", "false", PropertyType.BOOLEAN,
      "Use SSL for socket connections from clients and among accumulo services. "
          + "Mutually exclusive with SASL RPC configuration.",
      "1.6.0"),
  INSTANCE_RPC_SSL_CLIENT_AUTH("instance.rpc.ssl.clientAuth", "false", PropertyType.BOOLEAN,
      "Require clients to present certs signed by a trusted root.", "1.6.0"),
  INSTANCE_RPC_SASL_ENABLED("instance.rpc.sasl.enabled", "false", PropertyType.BOOLEAN,
      "Configures Thrift RPCs to require SASL with GSSAPI which supports "
          + "Kerberos authentication. Mutually exclusive with SSL RPC configuration.",
      "1.7.0"),
  INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION("instance.rpc.sasl.allowed.user.impersonation", "",
      PropertyType.STRING,
      "One-line configuration property controlling what users are allowed to "
          + "impersonate other users.",
      "1.7.1"),
  INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION("instance.rpc.sasl.allowed.host.impersonation", "",
      PropertyType.STRING,
      "One-line configuration property controlling the network locations "
          + "(hostnames) that are allowed to impersonate other users.",
      "1.7.1"),
  // Crypto-related properties
  @Experimental
  INSTANCE_CRYPTO_PREFIX("instance.crypto.opts.", null, PropertyType.PREFIX,
      "Properties related to on-disk file encryption.", "2.0.0"),
  @Experimental
  @Sensitive
  INSTANCE_CRYPTO_SENSITIVE_PREFIX("instance.crypto.opts.sensitive.", null, PropertyType.PREFIX,
      "Sensitive properties related to on-disk file encryption.", "2.0.0"),
  @Experimental
  INSTANCE_CRYPTO_FACTORY("instance.crypto.opts.factory",
      "org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory", PropertyType.CLASSNAME,
      "The class which provides crypto services for on-disk file encryption. The default does nothing. To enable "
          + "encryption, replace this classname with an implementation of the"
          + "org.apache.accumulo.core.spi.crypto.CryptoFactory interface.",
      "2.1.0"),
  // general properties
  GENERAL_PREFIX("general.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of accumulo overall, but"
          + " do not have to be consistent throughout a cloud.",
      "1.3.5"),

  GENERAL_CONTEXT_CLASSLOADER_FACTORY("general.context.class.loader.factory", "",
      PropertyType.CLASSNAME,
      "Name of classloader factory to be used to create classloaders for named contexts,"
          + " such as per-table contexts set by `table.class.loader.context`.",
      "2.1.0"),
  GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MIN("general.file.name.allocation.batch.size.min", "100",
      PropertyType.COUNT,
      "The minimum number of filenames that will be allocated from ZooKeeper at a time.", "2.1.3"),
  GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MAX("general.file.name.allocation.batch.size.max", "200",
      PropertyType.COUNT,
      "The maximum number of filenames that will be allocated from ZooKeeper at a time.", "2.1.3"),
  GENERAL_RPC_TIMEOUT("general.rpc.timeout", "120s", PropertyType.TIMEDURATION,
      "Time to wait on I/O for simple, short RPC calls.", "1.3.5"),
  @Experimental
  GENERAL_RPC_SERVER_TYPE("general.rpc.server.type", "", PropertyType.STRING,
      "Type of Thrift server to instantiate, see "
          + "org.apache.accumulo.server.rpc.ThriftServerType for more information. "
          + "Only useful for benchmarking thrift servers.",
      "1.7.0"),
  GENERAL_KERBEROS_KEYTAB("general.kerberos.keytab", "", PropertyType.PATH,
      "Path to the kerberos keytab to use. Leave blank if not using kerberoized hdfs.", "1.4.1"),
  GENERAL_KERBEROS_PRINCIPAL("general.kerberos.principal", "", PropertyType.STRING,
      "Name of the kerberos principal to use. _HOST will automatically be "
          + "replaced by the machines hostname in the hostname portion of the "
          + "principal. Leave blank if not using kerberoized hdfs.",
      "1.4.1"),
  GENERAL_KERBEROS_RENEWAL_PERIOD("general.kerberos.renewal.period", "30s",
      PropertyType.TIMEDURATION,
      "The amount of time between attempts to perform Kerberos ticket renewals."
          + " This does not equate to how often tickets are actually renewed (which is"
          + " performed at 80% of the ticket lifetime).",
      "1.6.5"),
  GENERAL_MAX_MESSAGE_SIZE("general.server.message.size.max", "1G", PropertyType.BYTES,
      "The maximum size of a message that can be sent to a server.", "1.5.0"),
  @Experimental
  GENERAL_OPENTELEMETRY_ENABLED("general.opentelemetry.enabled", "false", PropertyType.BOOLEAN,
      "Enables tracing functionality using OpenTelemetry (assuming OpenTelemetry is configured).",
      "2.1.0"),
  GENERAL_THREADPOOL_SIZE("general.server.threadpool.size", "1", PropertyType.COUNT,
      "The number of threads to use for server-internal scheduled tasks.", "2.1.0"),
  // If you update the default type, be sure to update the default used for initialization failures
  // in VolumeManagerImpl
  @Experimental
  GENERAL_VOLUME_CHOOSER("general.volume.chooser", RandomVolumeChooser.class.getName(),
      PropertyType.CLASSNAME,
      "The class that will be used to select which volume will be used to create new files.",
      "1.6.0"),
  GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS("general.security.credential.provider.paths", "",
      PropertyType.STRING, "Comma-separated list of paths to CredentialProviders.", "1.6.1"),
  GENERAL_ARBITRARY_PROP_PREFIX("general.custom.", null, PropertyType.PREFIX,
      "Prefix to be used for user defined system-wide properties. This may be"
          + " particularly useful for system-wide configuration for various"
          + " user-implementations of pluggable Accumulo features, such as the balancer"
          + " or volume chooser.",
      "2.0.0"),
  GENERAL_DELEGATION_TOKEN_LIFETIME("general.delegation.token.lifetime", "7d",
      PropertyType.TIMEDURATION,
      "The length of time that delegation tokens and secret keys are valid.", "1.7.0"),
  GENERAL_DELEGATION_TOKEN_UPDATE_INTERVAL("general.delegation.token.update.interval", "1d",
      PropertyType.TIMEDURATION, "The length of time between generation of new secret keys.",
      "1.7.0"),
  GENERAL_LOW_MEM_DETECTOR_INTERVAL("general.low.mem.detector.interval", "5s",
      PropertyType.TIMEDURATION, "The time interval between low memory checks.", "3.0.0"),
  GENERAL_LOW_MEM_DETECTOR_THRESHOLD("general.low.mem.detector.threshold", "0.05",
      PropertyType.FRACTION,
      "The LowMemoryDetector will report when free memory drops below this percentage of total memory.",
      "3.0.0"),
  GENERAL_LOW_MEM_SCAN_PROTECTION("general.low.mem.protection.scan", "false", PropertyType.BOOLEAN,
      "Scans may be paused or return results early when the server "
          + "is low on memory and this property is set to true. Enabling this property will incur a slight "
          + "scan performance penalty when the server is not low on memory.",
      "3.0.0"),
  GENERAL_LOW_MEM_MINC_PROTECTION("general.low.mem.protection.compaction.minc", "false",
      PropertyType.BOOLEAN,
      "Minor compactions may be paused when the server "
          + "is low on memory and this property is set to true. Enabling this property will incur a slight "
          + "compaction performance penalty when the server is not low on memory.",
      "3.0.0"),
  GENERAL_LOW_MEM_MAJC_PROTECTION("general.low.mem.protection.compaction.majc", "false",
      PropertyType.BOOLEAN,
      "Major compactions may be paused when the server "
          + "is low on memory and this property is set to true. Enabling this property will incur a slight "
          + "compaction performance penalty when the server is not low on memory.",
      "3.0.0"),
  GENERAL_MAX_SCANNER_RETRY_PERIOD("general.max.scanner.retry.period", "5s",
      PropertyType.TIMEDURATION,
      "The maximum amount of time that a Scanner should wait before retrying a failed RPC.",
      "1.7.3"),
  GENERAL_MICROMETER_ENABLED("general.micrometer.enabled", "false", PropertyType.BOOLEAN,
      "Enables metrics functionality using Micrometer.", "2.1.0"),
  GENERAL_MICROMETER_JVM_METRICS_ENABLED("general.micrometer.jvm.metrics.enabled", "false",
      PropertyType.BOOLEAN, "Enables JVM metrics functionality using Micrometer.", "2.1.0"),
  GENERAL_MICROMETER_FACTORY("general.micrometer.factory", "", PropertyType.CLASSNAME,
      "Name of class that implements MeterRegistryFactory.", "2.1.0"),
  GENERAL_PROCESS_BIND_ADDRESS("general.process.bind.addr", "0.0.0.0", PropertyType.STRING,
      "The local IP address to which this server should bind for sending and receiving network traffic.",
      "3.0.0"),
  // properties that are specific to manager server behavior
  MANAGER_PREFIX("manager.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the manager server.", "2.1.0"),
  MANAGER_CLIENTPORT("manager.port.client", "9999", PropertyType.PORT,
      "The port used for handling client connections on the manager.", "1.3.5"),
  MANAGER_TABLET_BALANCER("manager.tablet.balancer",
      "org.apache.accumulo.core.spi.balancer.TableLoadBalancer", PropertyType.CLASSNAME,
      "The balancer class that accumulo will use to make tablet assignment and "
          + "migration decisions.",
      "1.3.5"),
  MANAGER_TABLET_GROUP_WATCHER_INTERVAL("manager.tablet.watcher.interval", "60s",
      PropertyType.TIMEDURATION,
      "Time to wait between scanning tablet states to identify tablets that need to be assigned, un-assigned, migrated, etc.",
      "2.1.2"),
  MANAGER_BULK_TIMEOUT("manager.bulk.timeout", "5m", PropertyType.TIMEDURATION,
      "The time to wait for a tablet server to process a bulk import request.", "1.4.3"),
  MANAGER_RENAME_THREADS("manager.rename.threadpool.size", "20", PropertyType.COUNT,
      "The number of threads to use when renaming user files during table import or bulk ingest.",
      "2.1.0"),
  MANAGER_MINTHREADS("manager.server.threads.minimum", "20", PropertyType.COUNT,
      "The minimum number of threads to use to handle incoming requests.", "1.4.0"),
  MANAGER_MINTHREADS_TIMEOUT("manager.server.threads.timeout", "0s", PropertyType.TIMEDURATION,
      "The time after which incoming request threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely.",
      "2.1.0"),
  MANAGER_THREADCHECK("manager.server.threadcheck.time", "1s", PropertyType.TIMEDURATION,
      "The time between adjustments of the server thread pool.", "1.4.0"),
  MANAGER_RECOVERY_DELAY("manager.recovery.delay", "10s", PropertyType.TIMEDURATION,
      "When a tablet server's lock is deleted, it takes time for it to "
          + "completely quit. This delay gives it time before log recoveries begin.",
      "1.5.0"),
  MANAGER_RECOVERY_WAL_EXISTENCE_CACHE_TIME("manager.recovery.wal.cache.time", "15s",
      PropertyType.TIMEDURATION,
      "Amount of time that the existence of recovery write-ahead logs is cached.", "2.1.2"),
  MANAGER_LEASE_RECOVERY_WAITING_PERIOD("manager.lease.recovery.interval", "5s",
      PropertyType.TIMEDURATION,
      "The amount of time to wait after requesting a write-ahead log to be recovered.", "1.5.0"),
  MANAGER_WAL_CLOSER_IMPLEMENTATION("manager.wal.closer.implementation",
      "org.apache.accumulo.server.manager.recovery.HadoopLogCloser", PropertyType.CLASSNAME,
      "A class that implements a mechanism to steal write access to a write-ahead log.", "2.1.0"),
  MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL("manager.fate.metrics.min.update.interval", "60s",
      PropertyType.TIMEDURATION, "Limit calls from metric sinks to zookeeper to update interval.",
      "1.9.3"),
  MANAGER_FATE_THREADPOOL_SIZE("manager.fate.threadpool.size", "4", PropertyType.COUNT,
      "The number of threads used to run fault-tolerant executions (FATE)."
          + " These are primarily table operations like merge.",
      "1.4.3"),
  MANAGER_STATUS_THREAD_POOL_SIZE("manager.status.threadpool.size", "0", PropertyType.COUNT,
      "The number of threads to use when fetching the tablet server status for balancing.  Zero "
          + "indicates an unlimited number of threads will be used.",
      "1.8.0"),
  MANAGER_METADATA_SUSPENDABLE("manager.metadata.suspendable", "false", PropertyType.BOOLEAN,
      "Allow tablets for the " + AccumuloTable.METADATA.tableName()
          + " table to be suspended via table.suspend.duration.",
      "1.8.0"),
  MANAGER_STARTUP_TSERVER_AVAIL_MIN_COUNT("manager.startup.tserver.avail.min.count", "0",
      PropertyType.COUNT,
      "Minimum number of tservers that need to be registered before manager will "
          + "start tablet assignment - checked at manager initialization, when manager gets lock. "
          + " When set to 0 or less, no blocking occurs. Default is 0 (disabled) to keep original "
          + " behaviour.",
      "1.10.0"),
  MANAGER_STARTUP_TSERVER_AVAIL_MAX_WAIT("manager.startup.tserver.avail.max.wait", "0",
      PropertyType.TIMEDURATION,
      "Maximum time manager will wait for tserver available threshold "
          + "to be reached before continuing. When set to 0 or less, will block "
          + "indefinitely. Default is 0 to block indefinitely. Only valid when tserver available "
          + "threshold is set greater than 0.",
      "1.10.0"),
  // properties that are specific to scan server behavior
  @Experimental
  SSERV_PREFIX("sserver.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the scan servers.", "2.1.0"),
  @Experimental
  SSERV_DATACACHE_SIZE("sserver.cache.data.size", "10%", PropertyType.MEMORY,
      "Specifies the size of the cache for RFile data blocks on each scan server.", "2.1.0"),
  @Experimental
  SSERV_INDEXCACHE_SIZE("sserver.cache.index.size", "25%", PropertyType.MEMORY,
      "Specifies the size of the cache for RFile index blocks on each scan server.", "2.1.0"),
  @Experimental
  SSERV_SUMMARYCACHE_SIZE("sserver.cache.summary.size", "10%", PropertyType.MEMORY,
      "Specifies the size of the cache for summary data on each scan server.", "2.1.0"),
  @Experimental
  SSERV_DEFAULT_BLOCKSIZE("sserver.default.blocksize", "1M", PropertyType.BYTES,
      "Specifies a default blocksize for the scan server caches.", "2.1.0"),
  @Experimental
  SSERV_GROUP_NAME("sserver.group", ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME,
      PropertyType.STRING,
      "Optional group name that will be made available to the "
          + "ScanServerSelector client plugin. Groups support at least two use cases:"
          + " dedicating resources to scans and/or using different hardware for scans.",
      "3.0.0"),
  @Experimental
  SSERV_CACHED_TABLET_METADATA_EXPIRATION("sserver.cache.metadata.expiration", "5m",
      PropertyType.TIMEDURATION, "The time after which cached tablet metadata will be refreshed.",
      "2.1.0"),
  @Experimental
  SSERV_PORTSEARCH("sserver.port.search", "true", PropertyType.BOOLEAN,
      "if the ports above are in use, search higher ports until one is available.", "2.1.0"),
  @Experimental
  SSERV_CLIENTPORT("sserver.port.client", "9996", PropertyType.PORT,
      "The port used for handling client connections on the tablet servers.", "2.1.0"),
  @Experimental
  SSERV_MAX_MESSAGE_SIZE("sserver.server.message.size.max", "1G", PropertyType.BYTES,
      "The maximum size of a message that can be sent to a scan server.", "2.1.0"),
  @Experimental
  SSERV_MINTHREADS("sserver.server.threads.minimum", "2", PropertyType.COUNT,
      "The minimum number of threads to use to handle incoming requests.", "2.1.0"),
  @Experimental
  SSERV_MINTHREADS_TIMEOUT("sserver.server.threads.timeout", "0s", PropertyType.TIMEDURATION,
      "The time after which incoming request threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely.",
      "2.1.0"),
  @Experimental
  SSERV_SCAN_EXECUTORS_PREFIX("sserver.scan.executors.", null, PropertyType.PREFIX,
      "Prefix for defining executors to service scans. See "
          + "[scan executors]({% durl administration/scan-executors %}) for an overview of why and"
          + " how to use this property. For each executor the number of threads, thread priority, "
          + "and an optional prioritizer can be configured. To configure a new executor, set "
          + "`sserver.scan.executors.<name>.threads=<number>`.  Optionally, can also set "
          + "`sserver.scan.executors.<name>.priority=<number 1 to 10>`, "
          + "`sserver.scan.executors.<name>.prioritizer=<class name>`, and "
          + "`sserver.scan.executors.<name>.prioritizer.opts.<key>=<value>`.",
      "2.1.0"),
  @Experimental
  SSERV_SCAN_EXECUTORS_DEFAULT_THREADS("sserver.scan.executors.default.threads", "16",
      PropertyType.COUNT, "The number of threads for the scan executor that tables use by default.",
      "2.1.0"),
  SSERV_SCAN_EXECUTORS_DEFAULT_PRIORITIZER("sserver.scan.executors.default.prioritizer", "",
      PropertyType.STRING,
      "Prioritizer for the default scan executor.  Defaults to none which "
          + "results in FIFO priority.  Set to a class that implements "
          + ScanPrioritizer.class.getName() + " to configure one.",
      "2.1.0"),
  @Experimental
  SSERV_SCAN_EXECUTORS_META_THREADS("sserver.scan.executors.meta.threads", "8", PropertyType.COUNT,
      "The number of threads for the metadata table scan executor.", "2.1.0"),
  @Experimental
  SSERVER_SCAN_REFERENCE_EXPIRATION_TIME("sserver.scan.reference.expiration", "5m",
      PropertyType.TIMEDURATION,
      "The amount of time a scan reference is unused before its deleted from metadata table.",
      "2.1.0"),
  @Experimental
  SSERV_THREADCHECK("sserver.server.threadcheck.time", "1s", PropertyType.TIMEDURATION,
      "The time between adjustments of the thrift server thread pool.", "2.1.0"),
  // properties that are specific to tablet server behavior
  TSERV_PREFIX("tserver.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the tablet servers.", "1.3.5"),
  TSERV_CLIENT_TIMEOUT("tserver.client.timeout", "3s", PropertyType.TIMEDURATION,
      "Time to wait for clients to continue scans before closing a session.", "1.3.5"),
  TSERV_DEFAULT_BLOCKSIZE("tserver.default.blocksize", "1M", PropertyType.BYTES,
      "Specifies a default blocksize for the tserver caches.", "1.3.5"),
  TSERV_CACHE_MANAGER_IMPL("tserver.cache.manager.class",
      "org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager", PropertyType.STRING,
      "Specifies the class name of the block cache factory implementation."
          + " Alternative implementation is"
          + " org.apache.accumulo.core.file.blockfile.cache.tinylfu.TinyLfuBlockCacheManager.",
      "2.0.0"),
  TSERV_DATACACHE_SIZE("tserver.cache.data.size", "10%", PropertyType.MEMORY,
      "Specifies the size of the cache for RFile data blocks.", "1.3.5"),
  TSERV_INDEXCACHE_SIZE("tserver.cache.index.size", "25%", PropertyType.MEMORY,
      "Specifies the size of the cache for RFile index blocks.", "1.3.5"),
  TSERV_SUMMARYCACHE_SIZE("tserver.cache.summary.size", "10%", PropertyType.MEMORY,
      "Specifies the size of the cache for summary data on each tablet server.", "2.0.0"),
  TSERV_PORTSEARCH("tserver.port.search", "false", PropertyType.BOOLEAN,
      "if the ports above are in use, search higher ports until one is available.", "1.3.5"),
  TSERV_CLIENTPORT("tserver.port.client", "9997", PropertyType.PORT,
      "The port used for handling client connections on the tablet servers.", "1.3.5"),
  TSERV_TOTAL_MUTATION_QUEUE_MAX("tserver.total.mutation.queue.max", "5%", PropertyType.MEMORY,
      "The amount of memory used to store write-ahead-log mutations before flushing them.",
      "1.7.0"),
  TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN("tserver.tablet.split.midpoint.files.max", "300",
      PropertyType.COUNT,
      "To find a tablets split points, all RFiles are opened and their indexes"
          + " are read. This setting determines how many RFiles can be opened at once."
          + " When there are more RFiles than this setting multiple passes must be"
          + " made, which is slower. However opening too many RFiles at once can cause"
          + " problems.",
      "1.3.5"),
  TSERV_WAL_MAX_REFERENCED("tserver.wal.max.referenced", "3", PropertyType.COUNT,
      "When a tablet server has more than this many write ahead logs, any tablet referencing older "
          + "logs over this threshold is minor compacted.  Also any tablet referencing this many "
          + "logs or more will be compacted.",
      "2.1.0"),
  TSERV_WAL_MAX_SIZE("tserver.wal.max.size", "1G", PropertyType.BYTES,
      "The maximum size for each write-ahead log. See comment for property"
          + " `tserver.memory.maps.max`.",
      "2.1.0"),
  TSERV_WAL_MAX_AGE("tserver.wal.max.age", "24h", PropertyType.TIMEDURATION,
      "The maximum age for each write-ahead log.", "2.1.0"),
  TSERV_WAL_TOLERATED_CREATION_FAILURES("tserver.wal.tolerated.creation.failures", "50",
      PropertyType.COUNT,
      "The maximum number of failures tolerated when creating a new write-ahead"
          + " log. Negative values will allow unlimited creation failures. Exceeding this"
          + " number of failures consecutively trying to create a new write-ahead log"
          + " causes the TabletServer to exit.",
      "2.1.0"),
  TSERV_WAL_TOLERATED_WAIT_INCREMENT("tserver.wal.tolerated.wait.increment", "1000ms",
      PropertyType.TIMEDURATION,
      "The amount of time to wait between failures to create or write a write-ahead log.", "2.1.0"),
  // Never wait longer than 5 mins for a retry
  TSERV_WAL_TOLERATED_MAXIMUM_WAIT_DURATION("tserver.wal.maximum.wait.duration", "5m",
      PropertyType.TIMEDURATION,
      "The maximum amount of time to wait after a failure to create or write a write-ahead log.",
      "2.1.0"),
  TSERV_SCAN_MAX_OPENFILES("tserver.scan.files.open.max", "100", PropertyType.COUNT,
      "Maximum total RFiles that all tablets in a tablet server can open for scans.", "1.4.0"),
  TSERV_MAX_IDLE("tserver.files.open.idle", "1m", PropertyType.TIMEDURATION,
      "Tablet servers leave previously used RFiles open for future queries."
          + " This setting determines how much time an unused RFile should be kept open"
          + " until it is closed.",
      "1.3.5"),
  TSERV_NATIVEMAP_ENABLED("tserver.memory.maps.native.enabled", "true", PropertyType.BOOLEAN,
      "An in-memory data store for accumulo implemented in c++ that increases"
          + " the amount of data accumulo can hold in memory and avoids Java GC pauses.",
      "1.3.5"),
  TSERV_MAXMEM("tserver.memory.maps.max", "33%", PropertyType.MEMORY,
      "Maximum amount of memory that can be used to buffer data written to a"
          + " tablet server. There are two other properties that can effectively limit"
          + " memory usage `table.compaction.minor.logs.threshold` and"
          + " `tserver.wal.max.size`. Ensure that `table.compaction.minor.logs.threshold`"
          + " * `tserver.wal.max.size` >= this property.",
      "1.3.5"),
  TSERV_SESSION_MAXIDLE("tserver.session.idle.max", "1m", PropertyType.TIMEDURATION,
      "When a tablet server's SimpleTimer thread triggers to check idle"
          + " sessions, this configurable option will be used to evaluate scan sessions"
          + " to determine if they can be closed due to inactivity.",
      "1.3.5"),
  TSERV_UPDATE_SESSION_MAXIDLE("tserver.session.update.idle.max", "1m", PropertyType.TIMEDURATION,
      "When a tablet server's SimpleTimer thread triggers to check idle"
          + " sessions, this configurable option will be used to evaluate update"
          + " sessions to determine if they can be closed due to inactivity.",
      "1.6.5"),
  TSERV_SCAN_EXECUTORS_PREFIX("tserver.scan.executors.", null, PropertyType.PREFIX,
      "Prefix for defining executors to service scans. See "
          + "[scan executors]({% durl administration/scan-executors %}) for an overview of why and"
          + " how to use this property. For each executor the number of threads, thread priority, "
          + "and an optional prioritizer can be configured. To configure a new executor, set "
          + "`tserver.scan.executors.<name>.threads=<number>`.  Optionally, can also set "
          + "`tserver.scan.executors.<name>.priority=<number 1 to 10>`, "
          + "`tserver.scan.executors.<name>.prioritizer=<class name>`, and "
          + "`tserver.scan.executors.<name>.prioritizer.opts.<key>=<value>`.",
      "2.0.0"),
  TSERV_SCAN_EXECUTORS_DEFAULT_THREADS("tserver.scan.executors.default.threads", "16",
      PropertyType.COUNT, "The number of threads for the scan executor that tables use by default.",
      "2.0.0"),
  TSERV_SCAN_EXECUTORS_DEFAULT_PRIORITIZER("tserver.scan.executors.default.prioritizer", "",
      PropertyType.STRING,
      "Prioritizer for the default scan executor.  Defaults to none which "
          + "results in FIFO priority.  Set to a class that implements "
          + ScanPrioritizer.class.getName() + " to configure one.",
      "2.0.0"),
  TSERV_SCAN_EXECUTORS_META_THREADS("tserver.scan.executors.meta.threads", "8", PropertyType.COUNT,
      "The number of threads for the metadata table scan executor.", "2.0.0"),
  TSERV_SCAN_RESULTS_MAX_TIMEOUT("tserver.scan.results.max.timeout", "1s",
      PropertyType.TIMEDURATION,
      "Max time for the thrift client handler to wait for scan results before timing out.",
      "2.1.0"),
  TSERV_MIGRATE_MAXCONCURRENT("tserver.migrations.concurrent.max", "1", PropertyType.COUNT,
      "The maximum number of concurrent tablet migrations for a tablet server.", "1.3.5"),
  TSERV_MAJC_DELAY("tserver.compaction.major.delay", "30s", PropertyType.TIMEDURATION,
      "Time a tablet server will sleep between checking which tablets need compaction.", "1.3.5"),
  @Deprecated(since = "3.1")
  @ReplacedBy(property = COMPACTION_SERVICE_PREFIX)
  TSERV_COMPACTION_SERVICE_PREFIX("tserver.compaction.major.service.", null, PropertyType.PREFIX,
      "Prefix for compaction services.", "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_ROOT_PLANNER("tserver.compaction.major.service.root.planner",
      DefaultCompactionPlanner.class.getName(), PropertyType.CLASSNAME,
      "Compaction planner for root tablet service.", "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_ROOT_RATE_LIMIT("tserver.compaction.major.service.root.rate.limit", "0B",
      PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major"
          + " compactions in this compaction service, or 0B for unlimited.  This property has"
          + " been deprecated in anticipation of it being removed in a future release that"
          + " removes the rate limiting feature.",
      "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_ROOT_MAX_OPEN(
      "tserver.compaction.major.service.root.planner.opts.maxOpen", "30", PropertyType.COUNT,
      "The maximum number of files a compaction will open.", "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_ROOT_EXECUTORS(
      "tserver.compaction.major.service.root.planner.opts.executors",
      "[{'name':'small','type':'internal','maxSize':'32M','numThreads':1},{'name':'huge','type':'internal','numThreads':1}]"
          .replaceAll("'", "\""),
      PropertyType.STRING,
      "See {% jlink -f org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner %}.",
      "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_META_PLANNER("tserver.compaction.major.service.meta.planner",
      DefaultCompactionPlanner.class.getName(), PropertyType.CLASSNAME,
      "Compaction planner for metadata table.", "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_META_RATE_LIMIT("tserver.compaction.major.service.meta.rate.limit", "0B",
      PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major"
          + " compactions in this compaction service, or 0B for unlimited. This property has"
          + " been deprecated in anticipation of it being removed in a future release that"
          + " removes the rate limiting feature.",
      "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_META_MAX_OPEN(
      "tserver.compaction.major.service.meta.planner.opts.maxOpen", "30", PropertyType.COUNT,
      "The maximum number of files a compaction will open.", "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_META_EXECUTORS(
      "tserver.compaction.major.service.meta.planner.opts.executors",
      "[{'name':'small','type':'internal','maxSize':'32M','numThreads':2},{'name':'huge','type':'internal','numThreads':2}]"
          .replaceAll("'", "\""),
      PropertyType.JSON,
      "See {% jlink -f org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner %}.",
      "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_DEFAULT_PLANNER("tserver.compaction.major.service.default.planner",
      DefaultCompactionPlanner.class.getName(), PropertyType.CLASSNAME,
      "Planner for default compaction service.", "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_DEFAULT_RATE_LIMIT("tserver.compaction.major.service.default.rate.limit",
      "0B", PropertyType.BYTES,
      "Maximum number of bytes to read or write per second over all major"
          + " compactions in this compaction service, or 0B for unlimited. This property has"
          + " been deprecated in anticipation of it being removed in a future release that"
          + " removes the rate limiting feature.",
      "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_DEFAULT_MAX_OPEN(
      "tserver.compaction.major.service.default.planner.opts.maxOpen", "10", PropertyType.COUNT,
      "The maximum number of files a compaction will open.", "2.1.0"),
  @Deprecated(since = "3.1")
  TSERV_COMPACTION_SERVICE_DEFAULT_EXECUTORS(
      "tserver.compaction.major.service.default.planner.opts.executors",
      "[{'name':'small','type':'internal','maxSize':'32M','numThreads':2},{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},{'name':'large','type':'internal','numThreads':2}]"
          .replaceAll("'", "\""),
      PropertyType.STRING,
      "See {% jlink -f org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner %}.",
      "2.1.0"),
  TSERV_MINC_MAXCONCURRENT("tserver.compaction.minor.concurrent.max", "4", PropertyType.COUNT,
      "The maximum number of concurrent minor compactions for a tablet server.", "1.3.5"),
  @Deprecated(since = "3.1")
  @ReplacedBy(property = COMPACTION_WARN_TIME)
  TSERV_COMPACTION_WARN_TIME("tserver.compaction.warn.time", "10m", PropertyType.TIMEDURATION,
      "When a compaction has not made progress for this time period, a warning will be logged.",
      "1.6.0"),
  TSERV_BLOOM_LOAD_MAXCONCURRENT("tserver.bloom.load.concurrent.max", "4", PropertyType.COUNT,
      "The number of concurrent threads that will load bloom filters in the background. "
          + "Setting this to zero will make bloom filters load in the foreground.",
      "1.3.5"),
  TSERV_MEMDUMP_DIR("tserver.dir.memdump", "/tmp", PropertyType.PATH,
      "A long running scan could possibly hold memory that has been minor"
          + " compacted. To prevent this, the in memory map is dumped to a local file"
          + " and the scan is switched to that local file. We can not switch to the"
          + " minor compacted file because it may have been modified by iterators. The"
          + " file dumped to the local dir is an exact copy of what was in memory.",
      "1.3.5"),
  TSERV_HEALTH_CHECK_FREQ("tserver.health.check.interval", "30m", PropertyType.TIMEDURATION,
      "The time between tablet server health checks.", "2.1.0"),
  TSERV_MINTHREADS("tserver.server.threads.minimum", "20", PropertyType.COUNT,
      "The minimum number of threads to use to handle incoming requests.", "1.4.0"),
  TSERV_MINTHREADS_TIMEOUT("tserver.server.threads.timeout", "0s", PropertyType.TIMEDURATION,
      "The time after which incoming request threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely.",
      "2.1.0"),
  TSERV_THREADCHECK("tserver.server.threadcheck.time", "1s", PropertyType.TIMEDURATION,
      "The time between adjustments of the server thread pool.", "1.4.0"),
  TSERV_MAX_MESSAGE_SIZE("tserver.server.message.size.max", "1G", PropertyType.BYTES,
      "The maximum size of a message that can be sent to a tablet server.", "1.6.0"),
  TSERV_LOG_BUSY_TABLETS_COUNT("tserver.log.busy.tablets.count", "0", PropertyType.COUNT,
      "Number of busiest tablets to log. Logged at interval controlled by "
          + "tserver.log.busy.tablets.interval. If <= 0, logging of busy tablets is disabled.",
      "1.10.0"),
  TSERV_LOG_BUSY_TABLETS_INTERVAL("tserver.log.busy.tablets.interval", "1h",
      PropertyType.TIMEDURATION, "Time interval between logging out busy tablets information.",
      "1.10.0"),
  TSERV_HOLD_TIME_SUICIDE("tserver.hold.time.max", "5m", PropertyType.TIMEDURATION,
      "The maximum time for a tablet server to be in the \"memory full\" state."
          + " If the tablet server cannot write out memory in this much time, it will"
          + " assume there is some failure local to its node, and quit. A value of zero"
          + " is equivalent to forever.",
      "1.4.0"),
  TSERV_WAL_BLOCKSIZE("tserver.wal.blocksize", "0", PropertyType.BYTES,
      "The size of the HDFS blocks used to write to the Write-Ahead log. If"
          + " zero, it will be 110% of `tserver.wal.max.size` (that is, try to use just"
          + " one block).",
      "1.5.0"),
  TSERV_WAL_REPLICATION("tserver.wal.replication", "0", PropertyType.COUNT,
      "The replication to use when writing the Write-Ahead log to HDFS. If"
          + " zero, it will use the HDFS default replication setting.",
      "1.5.0"),
  TSERV_WAL_SORT_MAX_CONCURRENT("tserver.wal.sort.concurrent.max", "2", PropertyType.COUNT,
      "The maximum number of threads to use to sort logs during recovery.", "2.1.0"),
  TSERV_WAL_SORT_BUFFER_SIZE("tserver.wal.sort.buffer.size", "10%", PropertyType.MEMORY,
      "The amount of memory to use when sorting logs during recovery.", "2.1.0"),
  TSERV_WAL_SORT_FILE_PREFIX("tserver.wal.sort.file.", null, PropertyType.PREFIX,
      "The rfile properties to use when sorting logs during recovery. Most of the properties"
          + " that begin with 'table.file' can be used here. For example, to set the compression"
          + " of the sorted recovery files to snappy use 'tserver.wal.sort.file.compress.type=snappy'.",
      "2.1.0"),
  @Deprecated(since = "2.1.3")
  TSERV_WORKQ_THREADS("tserver.workq.threads", "2", PropertyType.COUNT,
      "The number of threads for the distributed work queue. These threads are"
          + " used for copying failed bulk import RFiles. Note that as of version 3.1.0 this property"
          + " is not used and will be removed in a future release.",
      "1.4.2"),
  TSERV_WAL_SYNC("tserver.wal.sync", "true", PropertyType.BOOLEAN,
      "Use the SYNC_BLOCK create flag to sync WAL writes to disk. Prevents"
          + " problems recovering from sudden system resets.",
      "1.5.0"),
  TSERV_ASSIGNMENT_DURATION_WARNING("tserver.assignment.duration.warning", "10m",
      PropertyType.TIMEDURATION,
      "The amount of time an assignment can run before the server will print a"
          + " warning along with the current stack trace. Meant to help debug stuck"
          + " assignments.",
      "1.6.2"),
  TSERV_ASSIGNMENT_MAXCONCURRENT("tserver.assignment.concurrent.max", "2", PropertyType.COUNT,
      "The number of threads available to load tablets. Recoveries are still performed serially.",
      "1.7.0"),
  TSERV_SLOW_FLUSH_MILLIS("tserver.slow.flush.time", "100ms", PropertyType.TIMEDURATION,
      "If a flush to the write-ahead log takes longer than this period of time,"
          + " debugging information will written, and may result in a log rollover.",
      "1.8.0"),
  TSERV_SLOW_FILEPERMIT_MILLIS("tserver.slow.filepermit.time", "100ms", PropertyType.TIMEDURATION,
      "If a thread blocks more than this period of time waiting to get file permits,"
          + " debugging information will be written.",
      "1.9.3"),
  TSERV_SUMMARY_PARTITION_THREADS("tserver.summary.partition.threads", "10", PropertyType.COUNT,
      "Summary data must be retrieved from RFiles. For a large number of"
          + " RFiles, the files are broken into partitions of 100k files. This setting"
          + " determines how many of these groups of 100k RFiles will be processed"
          + " concurrently.",
      "2.0.0"),
  TSERV_SUMMARY_REMOTE_THREADS("tserver.summary.remote.threads", "128", PropertyType.COUNT,
      "For a partitioned group of 100k RFiles, those files are grouped by"
          + " tablet server. Then a remote tablet server is asked to gather summary"
          + " data. This setting determines how many concurrent request are made per"
          + " partition.",
      "2.0.0"),
  TSERV_SUMMARY_RETRIEVAL_THREADS("tserver.summary.retrieval.threads", "10", PropertyType.COUNT,
      "The number of threads on each tablet server available to retrieve"
          + " summary data, that is not currently in cache, from RFiles.",
      "2.0.0"),
  @Deprecated(since = "3.1")
  TSERV_LAST_LOCATION_MODE("tserver.last.location.mode", "compaction",
      PropertyType.LAST_LOCATION_MODE,
      "Describes how the system will record the 'last' location for tablets, which can be used for"
          + " assigning them when a cluster restarts. If 'compaction' is the mode, then the system"
          + " will record the location where the tablet's most recent compaction occurred. If"
          + " 'assignment' is the mode, then the most recently assigned location will be recorded."
          + " The manager.startup.tserver properties might also need to be set to ensure the"
          + " tserver is available before tablets are initially assigned if the 'last' location is"
          + " to be used. This property has been deprecated in anticipation of it being removed in"
          + " a future release that removes major compactions from the TabletServer, rendering this"
          + " feature moot.",
      "2.1.1"),
  // accumulo garbage collector properties
  GC_PREFIX("gc.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the accumulo garbage collector.",
      "1.3.5"),
  GC_CANDIDATE_BATCH_SIZE("gc.candidate.batch.size", "50%", PropertyType.MEMORY,
      "The amount of memory used as the batch size for garbage collection.", "2.1.0"),
  GC_CYCLE_START("gc.cycle.start", "30s", PropertyType.TIMEDURATION,
      "Time to wait before attempting to garbage collect any old RFiles or write-ahead logs.",
      "1.3.5"),
  GC_CYCLE_DELAY("gc.cycle.delay", "5m", PropertyType.TIMEDURATION,
      "Time between garbage collection cycles. In each cycle, old RFiles or write-ahead logs "
          + "no longer in use are removed from the filesystem.",
      "1.3.5"),
  GC_PORT("gc.port.client", "9998", PropertyType.PORT,
      "The listening port for the garbage collector's monitor service.", "1.3.5"),
  GC_DELETE_THREADS("gc.threads.delete", "16", PropertyType.COUNT,
      "The number of threads used to delete RFiles and write-ahead logs.", "1.3.5"),
  GC_SAFEMODE("gc.safemode", "false", PropertyType.BOOLEAN,
      "Provides listing of files to be deleted but does not delete any files.", "2.1.0"),
  GC_USE_FULL_COMPACTION("gc.post.metadata.action", "flush", PropertyType.GC_POST_ACTION,
      "When the gc runs it can make a lot of changes to the metadata, on completion, "
          + " to force the changes to be written to disk, the metadata and root tables can be flushed"
          + " and possibly compacted. Legal values are: compact - which both flushes and compacts the"
          + " metadata; flush - which flushes only (compactions may be triggered if required); or none.",
      "1.10.0"),

  // properties that are specific to the monitor server behavior
  MONITOR_PREFIX("monitor.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the monitor web server.", "1.3.5"),
  MONITOR_PORT("monitor.port.client", "9995", PropertyType.PORT,
      "The listening port for the monitor's http service.", "1.3.5"),
  MONITOR_SSL_KEYSTORE("monitor.ssl.keyStore", "", PropertyType.PATH,
      "The keystore for enabling monitor SSL.", "1.5.0"),
  @Sensitive
  MONITOR_SSL_KEYSTOREPASS("monitor.ssl.keyStorePassword", "", PropertyType.STRING,
      "The keystore password for enabling monitor SSL.", "1.5.0"),
  MONITOR_SSL_KEYSTORETYPE("monitor.ssl.keyStoreType", "jks", PropertyType.STRING,
      "Type of SSL keystore.", "1.7.0"),
  @Sensitive
  MONITOR_SSL_KEYPASS("monitor.ssl.keyPassword", "", PropertyType.STRING,
      "Optional: the password for the private key in the keyStore. When not provided, this "
          + "defaults to the keystore password.",
      "1.9.3"),
  MONITOR_SSL_TRUSTSTORE("monitor.ssl.trustStore", "", PropertyType.PATH,
      "The truststore for enabling monitor SSL.", "1.5.0"),
  @Sensitive
  MONITOR_SSL_TRUSTSTOREPASS("monitor.ssl.trustStorePassword", "", PropertyType.STRING,
      "The truststore password for enabling monitor SSL.", "1.5.0"),
  MONITOR_SSL_TRUSTSTORETYPE("monitor.ssl.trustStoreType", "jks", PropertyType.STRING,
      "Type of SSL truststore.", "1.7.0"),
  MONITOR_SSL_INCLUDE_CIPHERS("monitor.ssl.include.ciphers", "", PropertyType.STRING,
      "A comma-separated list of allows SSL Ciphers, see"
          + " monitor.ssl.exclude.ciphers to disallow ciphers.",
      "1.6.1"),
  MONITOR_SSL_EXCLUDE_CIPHERS("monitor.ssl.exclude.ciphers", "", PropertyType.STRING,
      "A comma-separated list of disallowed SSL Ciphers, see"
          + " monitor.ssl.include.ciphers to allow ciphers.",
      "1.6.1"),
  MONITOR_SSL_INCLUDE_PROTOCOLS("monitor.ssl.include.protocols", "TLSv1.3", PropertyType.STRING,
      "A comma-separate list of allowed SSL protocols.", "1.5.3"),
  MONITOR_LOCK_CHECK_INTERVAL("monitor.lock.check.interval", "5s", PropertyType.TIMEDURATION,
      "The amount of time to sleep between checking for the Monitor ZooKeeper lock.", "1.5.1"),
  MONITOR_RESOURCES_EXTERNAL("monitor.resources.external", "", PropertyType.JSON,
      "A JSON Map of Strings. Each String should be an HTML tag of an external"
          + " resource (JS or CSS) to be imported by the Monitor. Be sure to wrap"
          + " with CDATA tags. If this value is set, all of the external resources"
          + " in the `<head>` tag of the Monitor will be replaced with the tags set here."
          + " Be sure the jquery tag is first since other scripts will depend on it."
          + " The resources that are used by default can be seen in"
          + " `accumulo/server/monitor/src/main/resources/templates/default.ftl`.",
      "2.0.0"),
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
          + " to change properties in zookeeper that are set on a table.",
      "1.3.5"),
  TABLE_ARBITRARY_PROP_PREFIX("table.custom.", null, PropertyType.PREFIX,
      "Prefix to be used for user defined arbitrary properties.", "1.7.0"),
  TABLE_MINC_OUTPUT_DROP_CACHE("table.compaction.minor.output.drop.cache", "false",
      PropertyType.BOOLEAN,
      "Setting this property to true will call"
          + "FSDataOutputStream.setDropBehind(true) on the minor compaction output stream.",
      "2.1.1"),
  TABLE_MAJC_OUTPUT_DROP_CACHE("table.compaction.major.output.drop.cache", "false",
      PropertyType.BOOLEAN,
      "Setting this property to true will call"
          + "FSDataOutputStream.setDropBehind(true) on the major compaction output stream.",
      "2.1.1"),
  TABLE_MAJC_RATIO("table.compaction.major.ratio", "3", PropertyType.FRACTION,
      "Minimum ratio of total input size to maximum input RFile size for"
          + " running a major compaction.",
      "1.3.5"),
  TABLE_SPLIT_THRESHOLD("table.split.threshold", "1G", PropertyType.BYTES,
      "A tablet is split when the combined size of RFiles exceeds this amount.", "1.3.5"),
  TABLE_MAX_END_ROW_SIZE("table.split.endrow.size.max", "10k", PropertyType.BYTES,
      "Maximum size of end row.", "1.7.0"),
  TABLE_MINC_COMPACT_IDLETIME("table.compaction.minor.idle", "5m", PropertyType.TIMEDURATION,
      "After a tablet has been idle (no mutations) for this time period it may have its "
          + "in-memory map flushed to disk in a minor compaction. There is no guarantee an idle "
          + "tablet will be compacted.",
      "1.3.5"),
  TABLE_COMPACTION_DISPATCHER("table.compaction.dispatcher",
      SimpleCompactionDispatcher.class.getName(), PropertyType.CLASSNAME,
      "A configurable dispatcher that decides what compaction service a table should use.",
      "2.1.0"),
  TABLE_COMPACTION_DISPATCHER_OPTS("table.compaction.dispatcher.opts.", null, PropertyType.PREFIX,
      "Options for the table compaction dispatcher.", "2.1.0"),
  TABLE_COMPACTION_SELECTION_EXPIRATION("table.compaction.selection.expiration.ms", "2m",
      PropertyType.TIMEDURATION,
      "User compactions select files and are then queued for compaction, preventing these files "
          + "from being used in system compactions.  This timeout allows system compactions to cancel "
          + "the hold queued user compactions have on files, when its queued for more than the "
          + "specified time.  If a system compaction cancels a hold and runs, then the user compaction"
          + " can reselect and hold files after the system compaction runs.",
      "2.1.0"),
  TABLE_COMPACTION_SELECTOR("table.compaction.selector", "", PropertyType.CLASSNAME,
      "A configurable selector for a table that can periodically select file for mandatory "
          + "compaction, even if the files do not meet the compaction ratio.",
      "2.1.0"),
  TABLE_COMPACTION_SELECTOR_OPTS("table.compaction.selector.opts.", null, PropertyType.PREFIX,
      "Options for the table compaction dispatcher.", "2.1.0"),
  TABLE_COMPACTION_CONFIGURER("table.compaction.configurer", "", PropertyType.CLASSNAME,
      "A plugin that can dynamically configure compaction output files based on input files.",
      "2.1.0"),
  TABLE_COMPACTION_CONFIGURER_OPTS("table.compaction.configurer.opts.", null, PropertyType.PREFIX,
      "Options for the table compaction configuror.", "2.1.0"),
  // Crypto-related properties
  @Experimental
  TABLE_CRYPTO_PREFIX("table.crypto.opts.", null, PropertyType.PREFIX,
      "Properties related to on-disk file encryption.", "2.1.0"),
  @Experimental
  @Sensitive
  TABLE_CRYPTO_SENSITIVE_PREFIX("table.crypto.opts.sensitive.", null, PropertyType.PREFIX,
      "Sensitive properties related to on-disk file encryption.", "2.1.0"),
  TABLE_SCAN_DISPATCHER("table.scan.dispatcher", SimpleScanDispatcher.class.getName(),
      PropertyType.CLASSNAME,
      "This class is used to dynamically dispatch scans to configured scan executors.  Configured "
          + "classes must implement {% jlink " + ScanDispatcher.class.getName() + " %}. See "
          + "[scan executors]({% durl administration/scan-executors %}) for an overview of why"
          + " and how to use this property. This property is ignored for the root and metadata"
          + " table.  The metadata table always dispatches to a scan executor named `meta`.",
      "2.0.0"),
  TABLE_SCAN_DISPATCHER_OPTS("table.scan.dispatcher.opts.", null, PropertyType.PREFIX,
      "Options for the table scan dispatcher.", "2.0.0"),
  TABLE_SCAN_MAXMEM("table.scan.max.memory", "512k", PropertyType.BYTES,
      "The maximum amount of memory that will be used to cache results of a client query/scan. "
          + "Once this limit is reached, the buffered data is sent to the client.",
      "1.3.5"),
  TABLE_FILE_TYPE("table.file.type", RFile.EXTENSION, PropertyType.FILENAME_EXT,
      "Change the type of file a table writes.", "1.3.5"),
  TABLE_LOAD_BALANCER("table.balancer", "org.apache.accumulo.core.spi.balancer.SimpleLoadBalancer",
      PropertyType.STRING,
      "This property can be set to allow the LoadBalanceByTable load balancer"
          + " to change the called Load Balancer for this table.",
      "1.3.5"),
  TABLE_FILE_COMPRESSION_TYPE("table.file.compress.type", "gz", PropertyType.STRING,
      "Compression algorithm used on index and data blocks before they are"
          + " written. Possible values: zstd, gz, snappy, bzip2, lzo, lz4, none.",
      "1.3.5"),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE("table.file.compress.blocksize", "100k", PropertyType.BYTES,
      "The maximum size of data blocks in RFiles before they are compressed and written.", "1.3.5"),
  TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX("table.file.compress.blocksize.index", "128k",
      PropertyType.BYTES,
      "The maximum size of index blocks in RFiles before they are compressed and written.",
      "1.4.0"),
  TABLE_FILE_BLOCK_SIZE("table.file.blocksize", "0B", PropertyType.BYTES,
      "The HDFS block size used when writing RFiles. When set to 0B, the"
          + " value/defaults of HDFS property 'dfs.block.size' will be used.",
      "1.3.5"),
  TABLE_FILE_REPLICATION("table.file.replication", "0", PropertyType.COUNT,
      "The number of replicas for a table's RFiles in HDFS. When set to 0, HDFS"
          + " defaults are used.",
      "1.3.5"),
  TABLE_FILE_MAX("table.file.max", "15", PropertyType.COUNT,
      "The maximum number of RFiles each tablet in a table can have. When"
          + " adjusting this property you may want to consider adjusting"
          + " table.compaction.major.ratio also. Setting this property to 0 will make"
          + " it default to tserver.scan.files.open.max-1, this will prevent a tablet"
          + " from having more RFiles than can be opened. Prior to 2.1.0 this property"
          + " was used to trigger merging minor compactions, but merging minor compactions"
          + " were removed in 2.1.0. Now this property is only used by the"
          + " DefaultCompactionStrategy and the DefaultCompactionPlanner."
          + " The DefaultCompactionPlanner started using this property in 2.1.3, before"
          + " that it did not use the property.",
      "1.4.0"),
  TABLE_FILE_SUMMARY_MAX_SIZE("table.file.summary.maxSize", "256k", PropertyType.BYTES,
      "The maximum size summary that will be stored. The number of RFiles that"
          + " had summary data exceeding this threshold is reported by"
          + " Summary.getFileStatistics().getLarge(). When adjusting this consider the"
          + " expected number RFiles with summaries on each tablet server and the"
          + " summary cache size.",
      "2.0.0"),
  TABLE_BLOOM_ENABLED("table.bloom.enabled", "false", PropertyType.BOOLEAN,
      "Use bloom filters on this table.", "1.3.5"),
  TABLE_BLOOM_LOAD_THRESHOLD("table.bloom.load.threshold", "1", PropertyType.COUNT,
      "This number of seeks that would actually use a bloom filter must occur"
          + " before a RFile's bloom filter is loaded. Set this to zero to initiate"
          + " loading of bloom filters when a RFile is opened.",
      "1.3.5"),
  TABLE_BLOOM_SIZE("table.bloom.size", "1048576", PropertyType.COUNT,
      "Bloom filter size, as number of keys.", "1.3.5"),
  TABLE_BLOOM_ERRORRATE("table.bloom.error.rate", "0.5%", PropertyType.FRACTION,
      "Bloom filter error rate.", "1.3.5"),
  TABLE_BLOOM_KEY_FUNCTOR("table.bloom.key.functor",
      "org.apache.accumulo.core.file.keyfunctor.RowFunctor", PropertyType.CLASSNAME,
      "A function that can transform the key prior to insertion and check of"
          + " bloom filter. org.apache.accumulo.core.file.keyfunctor.RowFunctor,"
          + " org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor, and"
          + " org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor are"
          + " allowable values. One can extend any of the above mentioned classes to"
          + " perform specialized parsing of the key.",
      "1.3.5"),
  TABLE_BLOOM_HASHTYPE("table.bloom.hash.type", "murmur", PropertyType.STRING,
      "The bloom filter hash type.", "1.3.5"),
  TABLE_BULK_MAX_TABLETS("table.bulk.max.tablets", "0", PropertyType.COUNT,
      "The maximum number of tablets allowed for one bulk import file. Value of 0 is Unlimited. "
          + "This property is only enforced in the new bulk import API.",
      "2.1.0"),
  TABLE_DURABILITY("table.durability", "sync", PropertyType.DURABILITY,
      "The durability used to write to the write-ahead log. Legal values are:"
          + " none, which skips the write-ahead log; log, which sends the data to the"
          + " write-ahead log, but does nothing to make it durable; flush, which pushes"
          + " data to the file system; and sync, which ensures the data is written to disk.",
      "1.7.0"),

  TABLE_FAILURES_IGNORE("table.failures.ignore", "false", PropertyType.BOOLEAN,
      "If you want queries for your table to hang or fail when data is missing"
          + " from the system, then set this to false. When this set to true missing"
          + " data will be reported but queries will still run possibly returning a"
          + " subset of the data.",
      "1.3.5"),
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
          + " will be interpreted with the new label on the next scan.",
      "1.3.5"),
  TABLE_LOCALITY_GROUPS("table.groups.enabled", "", PropertyType.STRING,
      "A comma separated list of locality group names to enable for this table.", "1.3.5"),
  TABLE_CONSTRAINT_PREFIX("table.constraint.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that add"
          + " constraints to a table. These properties start with the category"
          + " prefix, followed by a number, and their values correspond to a fully"
          + " qualified Java class that implements the Constraint interface.\nFor example:\n"
          + "table.constraint.1 = org.apache.accumulo.core.constraints.MyCustomConstraint\n"
          + "and:\n table.constraint.2 = my.package.constraints.MySecondConstraint.",
      "1.3.5"),
  TABLE_INDEXCACHE_ENABLED("table.cache.index.enable", "true", PropertyType.BOOLEAN,
      "Determines whether index block cache is enabled for a table.", "1.3.5"),
  TABLE_BLOCKCACHE_ENABLED("table.cache.block.enable", "false", PropertyType.BOOLEAN,
      "Determines whether data block cache is enabled for a table.", "1.3.5"),
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
          + "For example, table.iterator.minc.vers.opt.maxVersions = 3.",
      "1.3.5"),
  TABLE_ITERATOR_SCAN_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.scan.name() + ".", null,
      PropertyType.PREFIX, "Convenience prefix to find options for the scan iterator scope.",
      "1.5.2"),
  TABLE_ITERATOR_MINC_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.minc.name() + ".", null,
      PropertyType.PREFIX, "Convenience prefix to find options for the minc iterator scope.",
      "1.5.2"),
  TABLE_ITERATOR_MAJC_PREFIX(TABLE_ITERATOR_PREFIX.getKey() + IteratorScope.majc.name() + ".", null,
      PropertyType.PREFIX, "Convenience prefix to find options for the majc iterator scope.",
      "1.5.2"),
  TABLE_LOCALITY_GROUP_PREFIX("table.group.", null, PropertyType.PREFIX,
      "Properties in this category are per-table properties that define"
          + " locality groups in a table. These properties start with the category"
          + " prefix, followed by a name, followed by a period, and followed by a"
          + " property for that group.\n"
          + "For example table.group.group1=x,y,z sets the column families for a"
          + " group called group1. Once configured, group1 can be enabled by adding"
          + " it to the list of groups in the " + TABLE_LOCALITY_GROUPS.getKey() + " property.\n"
          + "Additional group options may be specified for a named group by setting"
          + " `table.group.<name>.opt.<key>=<value>`.",
      "1.3.5"),
  TABLE_FORMATTER_CLASS("table.formatter", DefaultFormatter.class.getName(), PropertyType.STRING,
      "The Formatter class to apply on results in the shell.", "1.4.0"),
  TABLE_CLASSLOADER_CONTEXT("table.class.loader.context", "", PropertyType.STRING,
      "The context to use for loading per-table resources, such as iterators"
          + " from the configured factory in `general.context.class.loader.factory`.",
      "2.1.0"),
  TABLE_SAMPLER("table.sampler", "", PropertyType.CLASSNAME,
      "The name of a class that implements org.apache.accumulo.core.Sampler."
          + " Setting this option enables storing a sample of data which can be"
          + " scanned. Always having a current sample can useful for query optimization"
          + " and data comprehension. After enabling sampling for an existing table,"
          + " a compaction is needed to compute the sample for existing data. The"
          + " compact command in the shell has an option to only compact RFiles without"
          + " sample data.",
      "1.8.0"),
  TABLE_SAMPLER_OPTS("table.sampler.opt.", null, PropertyType.PREFIX,
      "The property is used to set options for a sampler. If a sample had two"
          + " options like hasher and modulous, then the two properties"
          + " table.sampler.opt.hasher=${hash algorithm} and"
          + " table.sampler.opt.modulous=${mod} would be set.",
      "1.8.0"),
  TABLE_SUSPEND_DURATION("table.suspend.duration", "0s", PropertyType.TIMEDURATION,
      "For tablets belonging to this table: When a tablet server dies, allow"
          + " the tablet server this duration to revive before reassigning its tablets"
          + " to other tablet servers.",
      "1.8.0"),
  TABLE_SUMMARIZER_PREFIX("table.summarizer.", null, PropertyType.PREFIX,
      "Prefix for configuring summarizers for a table. Using this prefix"
          + " multiple summarizers can be configured with options for each one. Each"
          + " summarizer configured should have a unique id, this id can be anything."
          + " To add a summarizer set "
          + "`table.summarizer.<unique id>=<summarizer class name>.` If the summarizer has options"
          + ", then for each option set `table.summarizer.<unique id>.opt.<key>=<value>`.",
      "2.0.0"),
  @Experimental
  TABLE_DELETE_BEHAVIOR("table.delete.behavior",
      DeletingIterator.Behavior.PROCESS.name().toLowerCase(), PropertyType.STRING,
      "This determines what action to take when a delete marker is seen."
          + " Valid values are `process` and `fail` with `process` being the default.  When set to "
          + "`process`, deletes will suppress data.  When set to `fail`, any deletes seen will cause"
          + " an exception. The purpose of `fail` is to support tables that never delete data and"
          + " need fast seeks within the timestamp range of a column. When setting this to fail, "
          + "also consider configuring the `" + NoDeleteConstraint.class.getName() + "` "
          + "constraint.",
      "2.0.0"),

  // Compactor properties
  @Experimental
  COMPACTOR_PREFIX("compactor.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the accumulo compactor server.", "2.1.0"),
  @Experimental
  COMPACTOR_PORTSEARCH("compactor.port.search", "false", PropertyType.BOOLEAN,
      "If the compactor.port.client is in use, search higher ports until one is available.",
      "2.1.0"),
  @Experimental
  COMPACTOR_CLIENTPORT("compactor.port.client", "9133", PropertyType.PORT,
      "The port used for handling client connections on the compactor servers.", "2.1.0"),
  @Experimental
  COMPACTOR_MINTHREADS("compactor.threads.minimum", "1", PropertyType.COUNT,
      "The minimum number of threads to use to handle incoming requests.", "2.1.0"),
  @Experimental
  COMPACTOR_MINTHREADS_TIMEOUT("compactor.threads.timeout", "0s", PropertyType.TIMEDURATION,
      "The time after which incoming request threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely.",
      "2.1.0"),
  @Experimental
  COMPACTOR_THREADCHECK("compactor.threadcheck.time", "1s", PropertyType.TIMEDURATION,
      "The time between adjustments of the server thread pool.", "2.1.0"),
  @Experimental
  COMPACTOR_MAX_MESSAGE_SIZE("compactor.message.size.max", "10M", PropertyType.BYTES,
      "The maximum size of a message that can be sent to a tablet server.", "2.1.0"),
  @Experimental
  COMPACTOR_QUEUE_NAME("compactor.queue", "", PropertyType.STRING,
      "The queue for which this Compactor will perform compactions.", "3.0.0"),
  // CompactionCoordinator properties
  @Experimental
  COMPACTION_COORDINATOR_PREFIX("compaction.coordinator.", null, PropertyType.PREFIX,
      "Properties in this category affect the behavior of the accumulo compaction coordinator server.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_THRIFTCLIENT_PORTSEARCH("compaction.coordinator.port.search", "false",
      PropertyType.BOOLEAN,
      "If the ports above are in use, search higher ports until one is available.", "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_CLIENTPORT("compaction.coordinator.port.client", "9132", PropertyType.PORT,
      "The port used for handling Thrift client connections on the compaction coordinator server.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_MINTHREADS("compaction.coordinator.threads.minimum", "1",
      PropertyType.COUNT, "The minimum number of threads to use to handle incoming requests.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_MINTHREADS_TIMEOUT("compaction.coordinator.threads.timeout", "0s",
      PropertyType.TIMEDURATION,
      "The time after which incoming request threads terminate with no work available.  Zero (0) will keep the threads alive indefinitely.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_THREADCHECK("compaction.coordinator.threadcheck.time", "1s",
      PropertyType.TIMEDURATION, "The time between adjustments of the server thread pool.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_MAX_MESSAGE_SIZE("compaction.coordinator.message.size.max", "10M",
      PropertyType.BYTES, "The maximum size of a message that can be sent to a tablet server.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL(
      "compaction.coordinator.compactor.dead.check.interval", "5m", PropertyType.TIMEDURATION,
      "The interval at which to check for dead compactors.", "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_FINALIZER_TSERVER_NOTIFIER_MAXTHREADS(
      "compaction.coordinator.compaction.finalizer.threads.maximum", "5", PropertyType.COUNT,
      "The maximum number of threads to use for notifying tablet servers that an external compaction has completed.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL(
      "compaction.coordinator.compaction.finalizer.check.interval", "60s",
      PropertyType.TIMEDURATION,
      "The interval at which to check for external compaction final state markers in the metadata table.",
      "2.1.0"),
  @Experimental
  COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL(
      "compaction.coordinator.tserver.check.interval", "1m", PropertyType.TIMEDURATION,
      "The interval at which to check the tservers for external compactions.", "2.1.0");

  private final String key;
  private final String defaultValue;
  private final String description;
  private String deprecatedSince;
  private final String availableSince;
  private boolean annotationsComputed = false;
  private boolean isSensitive;
  private boolean isDeprecated;
  private boolean isExperimental;
  private boolean isReplaced;
  private Property replacedBy = null;
  private final PropertyType type;

  Property(String name, String defaultValue, PropertyType type, String description,
      String availableSince) {
    this.key = name;
    this.defaultValue = defaultValue;
    this.description = description;
    this.availableSince = availableSince;
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
   * Gets the version in which the property was deprecated.
   *
   * @return Accumulo Version
   */
  public String deprecatedSince() {
    Preconditions.checkState(annotationsComputed,
        "precomputeAnnotations() must be called before calling this method");
    return deprecatedSince;
  }

  /**
   * Gets the version in which the property was introduced.
   *
   * @return Accumulo Version
   */
  public String availableSince() {
    return this.availableSince;
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

  /**
   * Checks if this property is replaced.
   *
   * @return true if this property is replaced
   */
  public boolean isReplaced() {
    Preconditions.checkState(annotationsComputed,
        "precomputeAnnotations() must be called before calling this method");
    return isReplaced;
  }

  /**
   * Gets the property in which the tagged property is replaced by.
   *
   * @return replacedBy
   */
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
    Deprecated dep = getAnnotation(Deprecated.class);
    if (dep != null) {
      deprecatedSince = dep.since();
    }
    isExperimental =
        hasAnnotation(Experimental.class) || hasPrefixWithAnnotation(getKey(), Experimental.class);
    isReplaced =
        hasAnnotation(ReplacedBy.class) || hasPrefixWithAnnotation(getKey(), ReplacedBy.class);
    ReplacedBy rb = getAnnotation(ReplacedBy.class);
    if (rb != null) {
      replacedBy = rb.property();
    } else {
      isReplaced = false;
    }
    annotationsComputed = true;
  }

  /**
   * Checks if a property with the given key is sensitive. The key must be for a valid property, and
   * must either itself be annotated as sensitive or have a prefix annotated as sensitive.
   *
   * @param key property key
   * @return true if property is sensitive
   */
  public static boolean isSensitive(String key) {
    Property prop = propertiesByKey.get(key);
    if (prop != null) {
      return prop.isSensitive();
    }
    return validPrefixes.stream().filter(key::startsWith).map(propertiesByKey::get)
        .anyMatch(Property::isSensitive);
  }

  private <T extends Annotation> boolean hasAnnotation(Class<T> annotationType) {
    return getAnnotation(annotationType) != null;
  }

  private <T extends Annotation> T getAnnotation(Class<T> annotationType) {
    try {
      return getClass().getField(name()).getAnnotation(annotationType);
    } catch (SecurityException | NoSuchFieldException e) {
      LoggerFactory.getLogger(getClass()).error("{}", e.getMessage(), e);
    }
    return null;
  }

  private static <T extends Annotation> boolean hasPrefixWithAnnotation(String key,
      Class<T> annotationType) {
    Predicate<Property> hasIt = prop -> prop.hasAnnotation(annotationType);
    return validPrefixes.stream().filter(key::startsWith).map(propertiesByKey::get).anyMatch(hasIt);
  }

  private static final HashSet<String> validTableProperties = new HashSet<>();
  private static final HashSet<String> validProperties = new HashSet<>();
  private static final HashSet<String> validPrefixes = new HashSet<>();
  private static final HashMap<String,Property> propertiesByKey = new HashMap<>();

  /**
   * Checks if the given property and value are valid. A property is valid if the property key is
   * valid see {@link #isValidPropertyKey} and that the value is a valid format for the type see
   * {@link PropertyType#isValidFormat}.
   *
   * @param key property key
   * @param value property value
   * @return true if key is valid (recognized, or has a recognized prefix)
   */
  public static boolean isValidProperty(final String key, final String value) {
    Property p = getPropertyByKey(key);
    if (p == null) {
      // If a key doesn't exist yet, then check if it follows a valid prefix
      return validPrefixes.stream().anyMatch(key::startsWith);
    }
    return (isValidPropertyKey(key) && p.getType().isValidFormat(value));
  }

  /**
   * Checks if the given property key is valid. A valid property key is either equal to the key of
   * some defined property or has a prefix matching some prefix defined in this class.
   *
   * @param key property key
   * @return true if key is valid (recognized, or has a recognized prefix)
   */
  public static boolean isValidPropertyKey(String key) {
    return validProperties.contains(key) || validPrefixes.stream().anyMatch(key::startsWith);

  }

  /**
   * Checks if the given property key is a valid property and is of type boolean.
   *
   * @param key property key
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
   * @param key property key
   * @return true if key is valid for a table property (recognized, or has a recognized prefix)
   */
  public static boolean isValidTablePropertyKey(String key) {
    return validTableProperties.contains(key) || (key.startsWith(Property.TABLE_PREFIX.getKey())
        && (key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())
            || key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())
            || key.startsWith(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey())
            || key.startsWith(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey())
            || key.startsWith(TABLE_SAMPLER_OPTS.getKey())
            || key.startsWith(TABLE_SUMMARIZER_PREFIX.getKey())
            || key.startsWith(TABLE_SCAN_DISPATCHER_OPTS.getKey())
            || key.startsWith(TABLE_COMPACTION_DISPATCHER_OPTS.getKey())
            || key.startsWith(TABLE_COMPACTION_CONFIGURER_OPTS.getKey())
            || key.startsWith(TABLE_COMPACTION_SELECTOR_OPTS.getKey()))
        || key.startsWith(TABLE_CRYPTO_PREFIX.getKey()));
  }

  public static final EnumSet<Property> fixedProperties = EnumSet.of(
      // port options
      GC_PORT, MANAGER_CLIENTPORT, TSERV_CLIENTPORT,

      // tserver cache options
      TSERV_CACHE_MANAGER_IMPL, TSERV_DATACACHE_SIZE, TSERV_INDEXCACHE_SIZE,
      TSERV_SUMMARYCACHE_SIZE,

      // others
      TSERV_NATIVEMAP_ENABLED, TSERV_SCAN_MAX_OPENFILES, MANAGER_RECOVERY_WAL_EXISTENCE_CACHE_TIME);

  /**
   * Checks if the given property may be changed via Zookeeper, but not recognized until the restart
   * of some relevant daemon.
   *
   * @param key property key
   * @return true if property may be changed via Zookeeper but only heeded upon some restart
   */
  public static boolean isFixedZooPropertyKey(Property key) {
    return fixedProperties.contains(key);
  }

  /**
   * Checks if the given property key is valid for a property that may be changed via Zookeeper.
   *
   * @param key property key
   * @return true if key's property may be changed via Zookeeper
   */
  public static boolean isValidZooPropertyKey(String key) {
    // white list prefixes
    return key.startsWith(Property.TABLE_PREFIX.getKey())
        || key.startsWith(Property.TSERV_PREFIX.getKey())
        || key.startsWith(Property.COMPACTION_SERVICE_PREFIX.getKey())
        || key.startsWith(Property.MANAGER_PREFIX.getKey())
        || key.startsWith(Property.GC_PREFIX.getKey())
        || key.startsWith(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey())
        || key.equals(Property.COMPACTION_WARN_TIME.getKey())
        || key.equals(Property.GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MIN.getKey())
        || key.equals(Property.GENERAL_FILE_NAME_ALLOCATION_BATCH_SIZE_MAX.getKey());
  }

  /**
   * Gets a {@link Property} instance with the given key.
   *
   * @param key property key
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
   * @param conf configuration containing property
   * @param property property specifying class name
   * @param base base class of type
   * @param defaultInstance instance to use if creation fails
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
   * @param conf configuration containing property
   * @param property property specifying class name
   * @param base base class of type
   * @param defaultInstance instance to use if creation fails
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
    Predicate<Property> isPrefix = p -> p.getType() == PropertyType.PREFIX;
    Arrays.stream(Property.values())
        // record all properties by key
        .peek(p -> propertiesByKey.put(p.getKey(), p))
        // save all the prefix properties
        .peek(p -> {
          if (isPrefix.test(p)) {
            validPrefixes.add(p.getKey());
          }
        })
        // only use the keys for the non-prefix properties from here on
        .filter(isPrefix.negate()).map(Property::getKey)
        // everything left is a valid property
        .peek(validProperties::add)
        // but some are also valid table properties
        .filter(k -> k.startsWith(Property.TABLE_PREFIX.getKey()))
        .forEach(validTableProperties::add);

    // order is very important here the following code relies on the maps and sets populated above
    Arrays.stream(Property.values()).forEach(Property::precomputeAnnotations);
  }
}
