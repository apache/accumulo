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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.CONDITIONAL_WRITER_CLEANUP_POOL;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.SCANNER_READ_AHEAD_POOL;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataCachedTabletObtainer;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.AmpleImpl;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.scan.ScanServerInfo;
import org.apache.accumulo.core.spi.scan.ScanServerSelector;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.tables.TableMapping;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Suppliers;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * This class represents any essential configuration and credentials needed to initiate RPC
 * operations throughout the code. It is intended to represent a shared object that contains these
 * things from when the client was first constructed. It is not public API, and is only an internal
 * representation of the context in which a client is executing RPCs. If additional parameters are
 * added to the public API that need to be used in the internals of Accumulo, they should be added
 * to this object for later retrieval, rather than as a separate parameter. Any state in this object
 * should be available at the time of its construction.
 */
public class ClientContext implements AccumuloClient {

  private static final Logger log = LoggerFactory.getLogger(ClientContext.class);

  private final ClientInfo info;
  private final Supplier<ZooCache> zooCache;

  private Credentials creds;
  private BatchWriterConfig batchWriterConfig;
  private ConditionalWriterConfig conditionalWriterConfig;
  private final AccumuloConfiguration accumuloConf;
  private final Configuration hadoopConf;
  private final Map<DataLevel,ConcurrentHashMap<TableId,ClientTabletCache>> tabletLocationCache;

  // These fields are very frequently accessed (each time a connection is created) and expensive to
  // compute, so cache them.
  private final Supplier<Long> timeoutSupplier;
  private final Supplier<SaslConnectionParams> saslSupplier;
  private final Supplier<SslConnectionParams> sslSupplier;
  private final Supplier<ScanServerSelector> scanServerSelectorSupplier;
  private final Supplier<ServiceLockPaths> serverPaths;
  private final Supplier<NamespaceMapping> namespaceMapping;
  private final Supplier<Cache<NamespaceId,TableMapping>> tableMappings;
  private TCredentials rpcCreds;
  private ThriftTransportPool thriftTransportPool;
  private ZookeeperLockChecker zkLockChecker;

  private final AtomicBoolean closed = new AtomicBoolean();

  private SecurityOperations secops = null;
  private final TableOperationsImpl tableops;
  private final NamespaceOperations namespaceops;
  private InstanceOperations instanceops = null;
  private final Supplier<ThreadPools> clientThreadPools;
  private ThreadPoolExecutor cleanupThreadPool;
  private ThreadPoolExecutor scannerReadaheadPool;
  private MeterRegistry micrometer;
  private Caches caches;

  private final AtomicBoolean zooKeeperOpened = new AtomicBoolean(false);
  private final AtomicBoolean zooCacheCreated = new AtomicBoolean(false);
  private final Supplier<ZooSession> zooSession;

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("This client was closed.");
    }
  }

  protected boolean isClosed() {
    return closed.get();
  }

  private ScanServerSelector createScanServerSelector() {
    String clazz = ClientProperty.SCAN_SERVER_SELECTOR.getValue(getClientProperties());
    try {
      Class<? extends ScanServerSelector> impl =
          Class.forName(clazz).asSubclass(ScanServerSelector.class);
      ScanServerSelector scanServerSelector = impl.getDeclaredConstructor().newInstance();

      Map<String,String> sserverProps = new HashMap<>();
      ClientProperty.getPrefix(getClientProperties(),
          ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey()).forEach((k, v) -> {
            sserverProps.put(
                k.toString()
                    .substring(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey().length()),
                v.toString());
          });

      scanServerSelector.init(new ScanServerSelector.InitParameters() {
        @Override
        public Map<String,String> getOptions() {
          return Collections.unmodifiableMap(sserverProps);
        }

        @Override
        public ServiceEnvironment getServiceEnv() {
          return new ClientServiceEnvironmentImpl(ClientContext.this);
        }

        @Override
        public Supplier<Collection<ScanServerInfo>> getScanServers() {
          return () -> getServerPaths()
              .getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).stream()
              .map(entry -> new ScanServerInfo() {
                @Override
                public String getAddress() {
                  return entry.getServer();
                }

                @Override
                public ResourceGroupId getGroup() {
                  return entry.getResourceGroup();
                }
              }).collect(Collectors.toSet());
        }
      });
      return scanServerSelector;
    } catch (ReflectiveOperationException | IllegalArgumentException | SecurityException e) {
      throw new RuntimeException("Error creating ScanServerSelector implementation: " + clazz, e);
    }
  }

  /**
   * Create a client context with the provided configuration. Clients should call
   * Accumulo.newClient() builder
   */
  public ClientContext(ClientInfo info, AccumuloConfiguration serverConf,
      UncaughtExceptionHandler ueh) {
    this.info = info;
    this.hadoopConf = info.getHadoopConf();

    var tabletCache =
        new EnumMap<DataLevel,ConcurrentHashMap<TableId,ClientTabletCache>>(DataLevel.class);
    for (DataLevel level : DataLevel.values()) {
      tabletCache.put(level, new ConcurrentHashMap<>());
    }
    this.tabletLocationCache = Collections.unmodifiableMap(tabletCache);

    this.zooSession = memoize(() -> {
      var zk =
          info.getZooKeeperSupplier(getClass().getSimpleName() + "(" + info.getPrincipal() + ")",
              ZooUtil.getRoot(getInstanceID())).get();
      zooKeeperOpened.set(true);
      return zk;
    });

    this.zooCache = memoize(() -> {
      var zc = new ZooCache(getZooSession(), createPersistentWatcherPaths());
      zooCacheCreated.set(true);
      return zc;
    });
    this.accumuloConf = serverConf;
    timeoutSupplier = memoizeWithExpiration(
        () -> getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT), 100, MILLISECONDS);
    sslSupplier = memoize(() -> SslConnectionParams.forClient(getConfiguration()));
    saslSupplier = memoizeWithExpiration(
        () -> SaslConnectionParams.from(getConfiguration(), getCredentials().getToken()), 100,
        MILLISECONDS);
    scanServerSelectorSupplier = memoize(this::createScanServerSelector);
    this.tableops = new TableOperationsImpl(this);
    this.namespaceops = new NamespaceOperationsImpl(this, tableops);
    this.serverPaths = Suppliers.memoize(() -> new ServiceLockPaths(this.getZooCache()));
    if (ueh == Threads.UEH) {
      clientThreadPools = () -> ThreadPools.getServerThreadPools();
    } else {
      // Provide a default UEH that just logs the error
      if (ueh == null) {
        clientThreadPools = () -> ThreadPools.getClientThreadPools(getConfiguration(), (t, e) -> {
          log.error("Caught an Exception in client background thread: {}. Thread is dead.", t, e);
        });
      } else {
        clientThreadPools = () -> ThreadPools.getClientThreadPools(getConfiguration(), ueh);
      }
    }
    this.namespaceMapping = memoize(() -> new NamespaceMapping(this));
    this.tableMappings =
        memoize(() -> getCaches().createNewBuilder(Caches.CacheName.TABLE_MAPPING_CACHE, true)
            .expireAfterAccess(10, MINUTES).build());
  }

  public Ample getAmple() {
    ensureOpen();
    return new AmpleImpl(this);
  }

  public synchronized Future<List<KeyValue>>
      submitScannerReadAheadTask(Callable<List<KeyValue>> c) {
    ensureOpen();
    if (scannerReadaheadPool == null) {
      scannerReadaheadPool = clientThreadPools.get().getPoolBuilder(SCANNER_READ_AHEAD_POOL)
          .numCoreThreads(0).numMaxThreads(Integer.MAX_VALUE).withTimeOut(3L, SECONDS)
          .withQueue(new SynchronousQueue<>()).build();
    }
    return scannerReadaheadPool.submit(c);
  }

  public synchronized void executeCleanupTask(Runnable r) {
    ensureOpen();
    if (cleanupThreadPool == null) {
      cleanupThreadPool = clientThreadPools.get().getPoolBuilder(CONDITIONAL_WRITER_CLEANUP_POOL)
          .numCoreThreads(1).withTimeOut(3L, SECONDS).build();
    }
    this.cleanupThreadPool.execute(r);
  }

  /**
   * @return ThreadPools instance optionally configured with client UncaughtExceptionHandler
   */
  public ThreadPools threadPools() {
    ensureOpen();
    return clientThreadPools.get();
  }

  /**
   * Retrieve the credentials used to construct this context
   */
  public synchronized Credentials getCredentials() {
    ensureOpen();
    if (creds == null) {
      creds = new Credentials(info.getPrincipal(), info.getAuthenticationToken());
    }
    return creds;
  }

  public String getPrincipal() {
    ensureOpen();
    return getCredentials().getPrincipal();
  }

  public AuthenticationToken getAuthenticationToken() {
    ensureOpen();
    return getCredentials().getToken();
  }

  private Properties getClientProperties() {
    return info.getClientProperties();
  }

  /**
   * Update the credentials in the current context after changing the current user's password or
   * other auth token
   */
  public synchronized void setCredentials(Credentials newCredentials) {
    ensureOpen();
    checkArgument(newCredentials != null, "newCredentials is null");
    creds = newCredentials;
    rpcCreds = null;
  }

  /**
   * Retrieve the configuration used to construct this context
   */
  public AccumuloConfiguration getConfiguration() {
    ensureOpen();
    return accumuloConf;
  }

  /**
   * Retrieve the hadoop configuration
   */
  public Configuration getHadoopConf() {
    ensureOpen();
    return this.hadoopConf;
  }

  /**
   * Retrieve the universal RPC client timeout from the configuration
   */
  public long getClientTimeoutInMillis() {
    ensureOpen();
    return timeoutSupplier.get();
  }

  /**
   * Retrieve SSL/TLS configuration to initiate an RPC connection to a server
   */
  public SslConnectionParams getClientSslParams() {
    ensureOpen();
    return sslSupplier.get();
  }

  /**
   * Retrieve SASL configuration to initiate an RPC connection to a server
   */
  public SaslConnectionParams getSaslParams() {
    ensureOpen();
    return saslSupplier.get();
  }

  static BatchWriterConfig getBatchWriterConfig(Properties props) {
    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();

    Long maxMemory = ClientProperty.BATCH_WRITER_MEMORY_MAX.getBytes(props);
    if (maxMemory != null) {
      batchWriterConfig.setMaxMemory(maxMemory);
    }
    Long maxLatency = ClientProperty.BATCH_WRITER_LATENCY_MAX.getTimeInMillis(props);
    if (maxLatency != null) {
      batchWriterConfig.setMaxLatency(maxLatency, MILLISECONDS);
    }
    Long timeout = ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getTimeInMillis(props);
    if (timeout != null) {
      batchWriterConfig.setTimeout(timeout, MILLISECONDS);
    }
    Integer maxThreads = ClientProperty.BATCH_WRITER_THREADS_MAX.getInteger(props);
    if (maxThreads != null) {
      batchWriterConfig.setMaxWriteThreads(maxThreads);
    }
    String durability = ClientProperty.BATCH_WRITER_DURABILITY.getValue(props);
    if (!durability.isEmpty()) {
      batchWriterConfig.setDurability(Durability.valueOf(durability.toUpperCase()));
    }
    return batchWriterConfig;
  }

  public synchronized BatchWriterConfig getBatchWriterConfig() {
    ensureOpen();
    if (batchWriterConfig == null) {
      batchWriterConfig = getBatchWriterConfig(getClientProperties());
    }
    return batchWriterConfig;
  }

  /**
   * @return the scan server selector implementation used for determining which scan servers will be
   *         used when performing an eventually consistent scan
   */
  public ScanServerSelector getScanServerSelector() {
    ensureOpen();
    return scanServerSelectorSupplier.get();
  }

  /**
   * @return map of live scan server addresses to lock uuids.
   */
  public Map<String,Pair<UUID,ResourceGroupId>> getScanServers() {
    ensureOpen();
    Map<String,Pair<UUID,ResourceGroupId>> liveScanServers = new HashMap<>();
    Set<ServiceLockPath> scanServerPaths =
        getServerPaths().getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    for (ServiceLockPath path : scanServerPaths) {
      try {
        ZcStat stat = new ZcStat();
        Optional<ServiceLockData> sld = ServiceLock.getLockData(getZooCache(), path, stat);
        if (sld.isPresent()) {
          final ServiceLockData data = sld.orElseThrow();
          final String addr = data.getAddressString(ThriftService.TABLET_SCAN);
          final UUID uuid = data.getServerUUID(ThriftService.TABLET_SCAN);
          final ResourceGroupId group = data.getGroup(ThriftService.TABLET_SCAN);
          liveScanServers.put(addr, new Pair<>(uuid, group));
        }
      } catch (IllegalArgumentException e) {
        log.error("Error validating zookeeper scan server node at path: " + path, e);
      }
    }
    return liveScanServers;
  }

  static ConditionalWriterConfig getConditionalWriterConfig(Properties props) {
    ConditionalWriterConfig conditionalWriterConfig = new ConditionalWriterConfig();

    Long timeout = ClientProperty.CONDITIONAL_WRITER_TIMEOUT_MAX.getTimeInMillis(props);
    if (timeout != null) {
      conditionalWriterConfig.setTimeout(timeout, MILLISECONDS);
    }
    String durability = ClientProperty.CONDITIONAL_WRITER_DURABILITY.getValue(props);
    if (!durability.isEmpty()) {
      conditionalWriterConfig.setDurability(Durability.valueOf(durability.toUpperCase()));
    }
    Integer maxThreads = ClientProperty.CONDITIONAL_WRITER_THREADS_MAX.getInteger(props);
    if (maxThreads != null) {
      conditionalWriterConfig.setMaxWriteThreads(maxThreads);
    }
    return conditionalWriterConfig;
  }

  public synchronized ConditionalWriterConfig getConditionalWriterConfig() {
    ensureOpen();
    if (conditionalWriterConfig == null) {
      conditionalWriterConfig = getConditionalWriterConfig(getClientProperties());
    }
    return conditionalWriterConfig;
  }

  /**
   * Serialize the credentials just before initiating the RPC call
   */
  public synchronized TCredentials rpcCreds() {
    ensureOpen();
    if (getCredentials().getToken().isDestroyed()) {
      rpcCreds = null;
    }

    if (rpcCreds == null) {
      rpcCreds = getCredentials().toThrift(getInstanceID());
    }

    return rpcCreds;
  }

  /**
   * Returns a unique string that identifies this instance of accumulo.
   *
   * @return a UUID
   */
  public InstanceId getInstanceID() {
    return info.getInstanceId();
  }

  /**
   * Returns the instance name given at system initialization time.
   *
   * @return current instance name
   */
  public String getInstanceName() {
    ensureOpen();
    return info.getInstanceName();
  }

  /**
   * Returns a comma-separated list of zookeeper servers the instance is using.
   *
   * @return the zookeeper servers this instance is using in "hostname:port" form
   */
  public String getZooKeepers() {
    ensureOpen();
    return info.getZooKeepers();
  }

  /**
   * Returns the zookeeper connection timeout.
   *
   * @return the configured timeout to connect to zookeeper
   */
  public int getZooKeepersSessionTimeOut() {
    ensureOpen();
    return info.getZooKeepersSessionTimeOut();
  }

  public ZooCache getZooCache() {
    return zooCache.get();
  }

  /**
   * Look for namespace ID in ZK.
   *
   * @throws NamespaceNotFoundException if not found
   */
  public synchronized NamespaceId getNamespaceId(String namespaceName)
      throws NamespaceNotFoundException {
    ensureOpen();
    var id = getNamespaceMapping().getNameToIdMap().get(namespaceName);
    if (id == null) {
      // maybe the namespace exists, but the mappings weren't updated from ZooCache yet... so try to
      // clear the cache and check again
      clearTableListCache();
      id = getNamespaceMapping().getNameToIdMap().get(namespaceName);
      throw new NamespaceNotFoundException(null, namespaceName,
          "getNamespaceId() failed to find namespace");
    }
    return id;
  }

  public synchronized Map<NamespaceId,String> getNamespaceIdToNameMap() {
    ensureOpen();
    return getNamespaceMapping().getIdToNameMap();
  }

  /**
   * Lookup table ID in ZK.
   *
   * @throws TableNotFoundException if not found; if the namespace was not found, this has a
   *         getCause() of NamespaceNotFoundException
   */
  public synchronized TableId getTableId(String tableName) throws TableNotFoundException {
    ensureOpen();
    Pair<String,String> qualified = TableNameUtil.qualify(tableName);
    NamespaceId nid;
    try {
      nid = getNamespaceId(qualified.getFirst());
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(tableName, e);
    }
    TableId tid = getTableMapping(nid).getNameToIdMap().get(qualified.getSecond());
    if (tid == null) {
      throw new TableNotFoundException(null, tableName,
          "No entry for this table found in the given namespace mapping");
    }
    return tid;
  }

  /**
   * Lookup table name in ZK.
   *
   * @throws TableNotFoundException if not found
   */
  public synchronized String getQualifiedTableName(TableId tableId) throws TableNotFoundException {
    ensureOpen();
    Map<NamespaceId,String> namespaceMapping = getNamespaceMapping().getIdToNameMap();
    for (Entry<NamespaceId,String> entry : namespaceMapping.entrySet()) {
      NamespaceId namespaceId = entry.getKey();
      String namespaceName = entry.getValue();
      String tName = getTableMapping(namespaceId).getIdToNameMap().get(tableId);
      if (tName != null) {
        return TableNameUtil.qualified(tName, namespaceName);
      }
    }
    throw new TableNotFoundException(tableId.canonical(), null,
        "No entry for this table Id found in table mappings");
  }

  public synchronized SortedMap<String,TableId> createQualifiedTableNameToIdMap() {
    ensureOpen();
    var result = new TreeMap<String,TableId>();
    getNamespaceMapping().getIdToNameMap().forEach((namespaceId, namespaceName) -> result
        .putAll(getTableMapping(namespaceId).createQualifiedNameToIdMap(namespaceName)));
    return result;
  }

  public synchronized SortedMap<TableId,String> createTableIdToQualifiedNameMap() {
    ensureOpen();
    var result = new TreeMap<TableId,String>();
    getNamespaceMapping().getIdToNameMap().forEach((namespaceId, namespaceName) -> result
        .putAll(getTableMapping(namespaceId).createIdToQualifiedNameMap(namespaceName)));
    return result;
  }

  public synchronized boolean tableNodeExists(TableId tableId) {
    ensureOpen();
    for (NamespaceId namespaceId : getNamespaceMapping().getIdToNameMap().keySet()) {
      if (getTableMapping(namespaceId).getIdToNameMap().containsKey(tableId)) {
        return true;
      }
    }
    return false;
  }

  public synchronized void clearTableListCache() {
    ensureOpen();
    getZooCache().clear(Constants.ZTABLES);
    getZooCache().clear(Constants.ZNAMESPACES);
  }

  public String getPrintableTableInfoFromId(TableId tableId) {
    try {
      return _printableTableInfo(getQualifiedTableName(tableId), tableId);
    } catch (TableNotFoundException e) {
      return _printableTableInfo(null, tableId);
    }
  }

  public String getPrintableTableInfoFromName(String tableName) {
    try {
      return _printableTableInfo(tableName, getTableId(tableName));
    } catch (TableNotFoundException e) {
      return _printableTableInfo(tableName, null);
    }
  }

  private synchronized String _printableTableInfo(String tableName, TableId tableId) {
    ensureOpen();
    return String.format("%s(ID:%s)", tableName == null ? "?" : tableName,
        tableId == null ? "?" : tableId.canonical());
  }

  public TableState getTableState(TableId tableId) {
    return getTableState(tableId, false);
  }

  /**
   * Get the current state of the table using the tableid. The boolean clearCache, if true will
   * clear the table state in zookeeper before fetching the state. Added with ACCUMULO-4574.
   *
   * @param tableId the table id
   * @param clearCachedState if true clear the table state in zookeeper before checking status
   * @return the table state.
   */
  public synchronized TableState getTableState(TableId tableId, boolean clearCachedState) {
    ensureOpen();
    String statePath = Constants.ZTABLES + "/" + tableId.canonical() + Constants.ZTABLE_STATE;
    ZooCache zc = getZooCache();
    if (clearCachedState) {
      zc.clear(statePath);
    }
    byte[] state = zc.get(statePath);
    if (state == null) {
      return TableState.UNKNOWN;
    }
    return TableState.valueOf(new String(state, UTF_8));
  }

  /**
   * Returns the namespace id for a given table ID.
   *
   * @param tableId The tableId
   * @return The namespace id which this table resides in.
   * @throws IllegalArgumentException if the table doesn't exist in ZooKeeper
   */
  public NamespaceId getNamespaceId(TableId tableId) throws TableNotFoundException {
    checkArgument(tableId != null, "tableId is null");

    if (SystemTables.containsTableId(tableId)) {
      return Namespace.ACCUMULO.id();
    }
    for (NamespaceId namespaceId : getNamespaceMapping().getIdToNameMap().keySet()) {
      if (getTableMapping(namespaceId).getIdToNameMap().containsKey(tableId)) {
        return namespaceId;
      }
    }
    throw new TableNotFoundException(tableId.canonical(), null,
        "No namespace found containing the given table ID " + tableId);
  }

  // use cases overlap with requireNotDeleted, but this throws a checked exception
  public TableId requireTableExists(TableId tableId, String tableName)
      throws TableNotFoundException {
    if (!tableNodeExists(tableId)) {
      throw new TableNotFoundException(tableId.canonical(), tableName, "Table no longer exists");
    }
    return tableId;
  }

  // use cases overlap with requireTableExists, but this throws a runtime exception
  public TableId requireNotDeleted(TableId tableId) {
    if (!tableNodeExists(tableId)) {
      throw new TableDeletedException(tableId.canonical());
    }
    return tableId;
  }

  public TableId requireNotOffline(TableId tableId, String tableName) {
    if (getTableState(tableId) == TableState.OFFLINE) {
      throw new TableOfflineException(tableId, tableName);
    }
    return tableId;
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    ensureOpen();
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchReader(this, requireNotOffline(getTableId(tableName), tableName),
        tableName, authorizations, numQueryThreads);
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException {
    ensureOpen();
    Integer numQueryThreads =
        ClientProperty.BATCH_SCANNER_NUM_QUERY_THREADS.getInteger(getClientProperties());
    requireNonNull(numQueryThreads);
    return createBatchScanner(tableName, authorizations, numQueryThreads);
  }

  @Override
  public BatchScanner createBatchScanner(String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    ensureOpen();
    Authorizations auths = securityOperations().getUserAuthorizations(getPrincipal());
    return createBatchScanner(tableName, auths);
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, BatchWriterConfig config) throws TableNotFoundException {
    ensureOpen();
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchDeleter(this, requireNotOffline(getTableId(tableName), tableName),
        tableName, authorizations, numQueryThreads, config.merge(getBatchWriterConfig()));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    return createBatchDeleter(tableName, authorizations, numQueryThreads, new BatchWriterConfig());
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config)
      throws TableNotFoundException {
    ensureOpen();
    // we used to allow null inputs for bw config
    if (config == null) {
      config = new BatchWriterConfig();
    }
    return new BatchWriterImpl(this, requireNotOffline(getTableId(tableName), tableName),
        config.merge(getBatchWriterConfig()));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName) throws TableNotFoundException {
    return createBatchWriter(tableName, new BatchWriterConfig());
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    ensureOpen();
    return new MultiTableBatchWriterImpl(this, config.merge(getBatchWriterConfig()));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter() {
    return createMultiTableBatchWriter(new BatchWriterConfig());
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config)
      throws TableNotFoundException {
    ensureOpen();
    if (config == null) {
      config = new ConditionalWriterConfig();
    }
    return new ConditionalWriterImpl(this, requireNotOffline(getTableId(tableName), tableName),
        tableName, config.merge(getConditionalWriterConfig()));
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName) throws TableNotFoundException {
    return createConditionalWriter(tableName, null);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException {
    ensureOpen();
    checkArgument(authorizations != null, "authorizations is null");
    Scanner scanner =
        new ScannerImpl(this, requireNotOffline(getTableId(tableName), tableName), authorizations);
    Integer batchSize = ClientProperty.SCANNER_BATCH_SIZE.getInteger(getClientProperties());
    if (batchSize != null) {
      scanner.setBatchSize(batchSize);
    }
    return scanner;
  }

  @Override
  public Scanner createScanner(String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Authorizations auths = securityOperations().getUserAuthorizations(getPrincipal());
    return createScanner(tableName, auths);
  }

  @Override
  public String whoami() {
    ensureOpen();
    return getCredentials().getPrincipal();
  }

  @Override
  public synchronized TableOperations tableOperations() {
    ensureOpen();
    return tableops;
  }

  @Override
  public synchronized NamespaceOperations namespaceOperations() {
    ensureOpen();
    return namespaceops;
  }

  @Override
  public synchronized SecurityOperations securityOperations() {
    ensureOpen();
    if (secops == null) {
      secops = new SecurityOperationsImpl(this);
    }

    return secops;
  }

  @Override
  public synchronized InstanceOperations instanceOperations() {
    ensureOpen();
    if (instanceops == null) {
      instanceops = new InstanceOperationsImpl(this);
    }

    return instanceops;
  }

  @Override
  public Properties properties() {
    ensureOpen();
    Properties result = new Properties();
    getClientProperties().forEach((key, value) -> {
      if (!key.equals(ClientProperty.AUTH_TOKEN.getKey())) {
        result.setProperty((String) key, (String) value);
      }
    });
    return result;
  }

  public AuthenticationToken token() {
    ensureOpen();
    return getAuthenticationToken();
  }

  @Override
  public synchronized void close() {
    if (closed.compareAndSet(false, true)) {
      if (thriftTransportPool != null) {
        log.debug("Closing Thrift Transport Pool");
        thriftTransportPool.shutdown();
      }
      if (scannerReadaheadPool != null) {
        log.debug("Closing Scanner ReadAhead Pool");
        scannerReadaheadPool.shutdownNow(); // abort all tasks, client is shutting down
      }
      if (cleanupThreadPool != null) {
        log.debug("Closing Cleanup ThreadPool");
        cleanupThreadPool.shutdown(); // wait for shutdown tasks to execute
      }
      if (zooCacheCreated.get()) {
        log.debug("Closing ZooCache");
        zooCache.get().close();
      }
      if (zooKeeperOpened.get()) {
        log.debug("Closing ZooSession");
        zooSession.get().close();
      }
    }
  }

  public static class ClientBuilderImpl<T>
      implements InstanceArgs<T>, PropertyOptions<T>, AuthenticationArgs<T>, ConnectionOptions<T>,
      SslOptions<T>, SaslOptions<T>, ClientFactory<T>, FromOptions<T> {

    private Properties properties = new Properties();
    private Optional<AuthenticationToken> tokenOpt = Optional.empty();
    private final Function<ClientBuilderImpl<T>,T> builderFunction;
    private UncaughtExceptionHandler ueh = null;

    public ClientBuilderImpl(Function<ClientBuilderImpl<T>,T> builderFunction) {
      this.builderFunction = builderFunction;
    }

    private ClientInfo getClientInfo() {
      // validate the token in the properties if not provided here
      ClientProperty.validate(properties, tokenOpt.isEmpty());
      return new ClientInfoImpl(properties, tokenOpt);
    }

    private UncaughtExceptionHandler getUncaughtExceptionHandler() {
      return ueh;
    }

    @Override
    public T build() {
      return builderFunction.apply(this);
    }

    public static AccumuloClient buildClient(ClientBuilderImpl<AccumuloClient> cbi) {
      // ClientContext closes reservation unless a RuntimeException is thrown
      ClientInfo info = cbi.getClientInfo();
      var config = ClientConfConverter.toAccumuloConf(info.getClientProperties());
      return new ClientContext(info, config, cbi.getUncaughtExceptionHandler());
    }

    public static Properties buildProps(ClientBuilderImpl<Properties> cbi) {
      ClientProperty.validate(cbi.properties);
      return cbi.properties;
    }

    @Override
    public AuthenticationArgs<T> to(CharSequence instanceName, CharSequence zookeepers) {
      setProperty(ClientProperty.INSTANCE_NAME, instanceName);
      setProperty(ClientProperty.INSTANCE_ZOOKEEPERS, zookeepers);
      return this;
    }

    @Override
    public SslOptions<T> truststore(CharSequence path) {
      setProperty(ClientProperty.SSL_TRUSTSTORE_PATH, path);
      return this;
    }

    @Override
    public SslOptions<T> truststore(CharSequence path, CharSequence password, CharSequence type) {
      setProperty(ClientProperty.SSL_TRUSTSTORE_PATH, path);
      setProperty(ClientProperty.SSL_TRUSTSTORE_PASSWORD, password);
      setProperty(ClientProperty.SSL_TRUSTSTORE_TYPE, type);
      return this;
    }

    @Override
    public SslOptions<T> keystore(CharSequence path) {
      setProperty(ClientProperty.SSL_KEYSTORE_PATH, path);
      return this;
    }

    @Override
    public SslOptions<T> keystore(CharSequence path, CharSequence password, CharSequence type) {
      setProperty(ClientProperty.SSL_KEYSTORE_PATH, path);
      setProperty(ClientProperty.SSL_KEYSTORE_PASSWORD, password);
      setProperty(ClientProperty.SSL_KEYSTORE_TYPE, type);
      return this;
    }

    @Override
    public SslOptions<T> useJsse() {
      setProperty(ClientProperty.SSL_USE_JSSE, "true");
      return this;
    }

    @Override
    public ConnectionOptions<T> zkTimeout(int timeout) {
      ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.setTimeInMillis(properties, (long) timeout);
      return this;
    }

    @Override
    public SslOptions<T> useSsl() {
      setProperty(ClientProperty.SSL_ENABLED, "true");
      return this;
    }

    @Override
    public SaslOptions<T> useSasl() {
      setProperty(ClientProperty.SASL_ENABLED, "true");
      return this;
    }

    @Override
    public ConnectionOptions<T> batchWriterConfig(BatchWriterConfig batchWriterConfig) {
      ClientProperty.BATCH_WRITER_MEMORY_MAX.setBytes(properties, batchWriterConfig.getMaxMemory());
      ClientProperty.BATCH_WRITER_LATENCY_MAX.setTimeInMillis(properties,
          batchWriterConfig.getMaxLatency(MILLISECONDS));
      ClientProperty.BATCH_WRITER_TIMEOUT_MAX.setTimeInMillis(properties,
          batchWriterConfig.getTimeout(MILLISECONDS));
      setProperty(ClientProperty.BATCH_WRITER_THREADS_MAX, batchWriterConfig.getMaxWriteThreads());
      setProperty(ClientProperty.BATCH_WRITER_DURABILITY,
          batchWriterConfig.getDurability().toString());
      return this;
    }

    @Override
    public ConnectionOptions<T> batchScannerQueryThreads(int numQueryThreads) {
      setProperty(ClientProperty.BATCH_SCANNER_NUM_QUERY_THREADS, numQueryThreads);
      return this;
    }

    @Override
    public ConnectionOptions<T> scannerBatchSize(int batchSize) {
      setProperty(ClientProperty.SCANNER_BATCH_SIZE, batchSize);
      return this;
    }

    @Override
    public SaslOptions<T> primary(CharSequence kerberosServerPrimary) {
      setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY, kerberosServerPrimary);
      return this;
    }

    @Override
    public SaslOptions<T> qop(CharSequence qualityOfProtection) {
      setProperty(ClientProperty.SASL_QOP, qualityOfProtection);
      return this;
    }

    @Override
    public FromOptions<T> from(String propertiesFilePath) {
      return from(ClientInfoImpl.toProperties(propertiesFilePath));
    }

    @Override
    public FromOptions<T> from(Path propertiesFile) {
      return from(ClientInfoImpl.toProperties(propertiesFile));
    }

    @Override
    public FromOptions<T> from(URL propertiesURL) {
      return from(ClientInfoImpl.toProperties(propertiesURL));
    }

    @Override
    public FromOptions<T> from(Properties properties) {
      // Make a copy, so that this builder's subsequent methods don't mutate the
      // properties object provided by the caller
      this.properties = new Properties();
      this.properties.putAll(properties);
      return this;
    }

    @Override
    public ConnectionOptions<T> as(CharSequence username, CharSequence password) {
      setProperty(ClientProperty.AUTH_PRINCIPAL, username);
      ClientProperty.setPassword(properties, password);
      return this;
    }

    @Override
    public ConnectionOptions<T> as(CharSequence principal, Path keyTabFile) {
      setProperty(ClientProperty.AUTH_PRINCIPAL, principal);
      ClientProperty.setKerberosKeytab(properties, keyTabFile.toString());
      return this;
    }

    @Override
    public ConnectionOptions<T> as(CharSequence principal, AuthenticationToken token) {
      if (token.isDestroyed()) {
        throw new IllegalArgumentException("AuthenticationToken has been destroyed");
      }
      setProperty(ClientProperty.AUTH_PRINCIPAL, principal.toString());
      ClientProperty.setAuthenticationToken(properties, token);
      this.tokenOpt = Optional.of(token);
      return this;
    }

    public void setProperty(ClientProperty property, CharSequence value) {
      properties.setProperty(property.getKey(), value.toString());
    }

    public void setProperty(ClientProperty property, Long value) {
      setProperty(property, Long.toString(value));
    }

    public void setProperty(ClientProperty property, Integer value) {
      setProperty(property, Integer.toString(value));
    }

    @Override
    public ClientFactory<T> withUncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
      this.ueh = ueh;
      return this;
    }

  }

  public ZooSession getZooSession() {
    ensureOpen();
    return zooSession.get();
  }

  protected long getTransportPoolMaxAgeMillis() {
    return ClientProperty.RPC_TRANSPORT_IDLE_TIMEOUT.getTimeInMillis(getClientProperties());
  }

  public synchronized ThriftTransportPool getTransportPool() {
    return getTransportPoolImpl(false);
  }

  protected synchronized ThriftTransportPool getTransportPoolImpl(boolean shouldHalt) {
    ensureOpen();
    if (thriftTransportPool == null) {
      LongSupplier maxAgeSupplier = () -> {
        try {
          return getTransportPoolMaxAgeMillis();
        } catch (IllegalStateException e) {
          if (closed.get()) {
            // The transport pool has a background thread that may call this supplier in the middle
            // of closing. This is here to avoid spurious exceptions from race conditions that
            // happen when closing a client.
            return ConfigurationTypeHelper
                .getTimeInMillis(ClientProperty.RPC_TRANSPORT_IDLE_TIMEOUT.getDefaultValue());
          }
          throw e;
        }
      };
      thriftTransportPool = ThriftTransportPool.startNew(maxAgeSupplier, shouldHalt);
    }
    return thriftTransportPool;
  }

  public MeterRegistry getMeterRegistry() {
    ensureOpen();
    return micrometer;
  }

  public void setMeterRegistry(MeterRegistry micrometer) {
    ensureOpen();
    this.micrometer = micrometer;
    getCaches();
  }

  public synchronized Caches getCaches() {
    ensureOpen();
    if (caches == null) {
      caches = Caches.getInstance();
      if (micrometer != null
          && getConfiguration().getBoolean(Property.GENERAL_MICROMETER_CACHE_METRICS_ENABLED)) {
        caches.registerMetrics(micrometer);
      }
    }
    return caches;
  }

  public synchronized ZookeeperLockChecker getTServerLockChecker() {
    ensureOpen();
    if (this.zkLockChecker == null) {
      // make this use its own ZooSession and ZooCache, because this is used by the
      // tablet location cache, which is a static singleton reused by multiple clients
      // so, it can't rely on being able to continue to use the same client's ZooCache,
      // because that client could be closed, and its ZooSession also closed
      // this needs to be fixed; TODO https://github.com/apache/accumulo/issues/2301
      var zk = info.getZooKeeperSupplier(ZookeeperLockChecker.class.getSimpleName(),
          ZooUtil.getRoot(getInstanceID())).get();
      this.zkLockChecker = new ZookeeperLockChecker(new ZooCache(zk, Set.of(Constants.ZTSERVERS)));
    }
    return this.zkLockChecker;
  }

  public ServiceLockPaths getServerPaths() {
    return this.serverPaths.get();
  }

  public NamespaceMapping getNamespaceMapping() {
    ensureOpen();
    NamespaceMapping namespaces = namespaceMapping.get();
    log.trace("Got namespace mapping: {}", namespaces);
    return namespaces;
  }

  public TableMapping getTableMapping(NamespaceId namespaceId) {
    ensureOpen();
    var mapping = tableMappings.get().asMap().computeIfAbsent(requireNonNull(namespaceId),
        id -> new TableMapping(this, id));
    log.trace("Got table mapping for namespaceId {}: {}", namespaceId, mapping);
    return mapping;
  }

  public ClientTabletCache getTabletLocationCache(TableId tableId) {
    ensureOpen();
    return tabletLocationCache.get(DataLevel.of(tableId)).computeIfAbsent(tableId,
        (TableId key) -> {
          var lockChecker = getTServerLockChecker();
          if (SystemTables.ROOT.tableId().equals(tableId)) {
            return new RootClientTabletCache(lockChecker);
          }
          var mlo = new MetadataCachedTabletObtainer();
          if (SystemTables.METADATA.tableId().equals(tableId)) {
            return new ClientTabletCacheImpl(SystemTables.METADATA.tableId(),
                getTabletLocationCache(SystemTables.ROOT.tableId()), mlo, lockChecker);
          } else {
            return new ClientTabletCacheImpl(tableId,
                getTabletLocationCache(SystemTables.METADATA.tableId()), mlo, lockChecker);
          }
        });
  }

  /**
   * Clear the currently cached tablet locations. The use of ConcurrentHashMap ensures this is
   * thread-safe. However, since the ConcurrentHashMap iterator is weakly consistent, it does not
   * block new locations from being cached. If new locations are added while this is executing, they
   * may be immediately invalidated by this code. Multiple calls to this method in different threads
   * may cause some location caches to be invalidated multiple times. That is okay, because cache
   * invalidation is idempotent.
   */
  public void clearTabletLocationCache() {
    tabletLocationCache.forEach((dataLevel, map) -> {
      // use iter.remove() instead of calling clear() on the map, to prevent clearing entries that
      // may not have been invalidated
      var iter = map.values().iterator();
      while (iter.hasNext()) {
        iter.next().invalidate();
        iter.remove();
      }
    });
  }

  private static Set<String> createPersistentWatcherPaths() {
    Set<String> pathsToWatch = new HashSet<>();
    for (String path : Set.of(Constants.ZCOMPACTORS, Constants.ZDEADTSERVERS, Constants.ZGC_LOCK,
        Constants.ZMANAGER_LOCK, Constants.ZMINI_LOCK, Constants.ZMONITOR_LOCK,
        Constants.ZNAMESPACES, Constants.ZRECOVERY, Constants.ZSSERVERS, Constants.ZTABLES,
        Constants.ZTSERVERS, Constants.ZUSERS, RootTable.ZROOT_TABLET, Constants.ZTEST_LOCK)) {
      pathsToWatch.add(path);
    }
    return pathsToWatch;
  }

}
