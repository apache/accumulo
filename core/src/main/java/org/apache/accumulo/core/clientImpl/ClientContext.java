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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.CONDITIONAL_WRITER_CLEANUP_POOL;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.SCANNER_READ_AHEAD_POOL;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.AmpleImpl;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.scan.ScanServerInfo;
import org.apache.accumulo.core.spi.scan.ScanServerSelector;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.tables.TableZooHelper;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  // These fields are very frequently accessed (each time a connection is created) and expensive to
  // compute, so cache them.
  private final Supplier<Long> timeoutSupplier;
  private final Supplier<SaslConnectionParams> saslSupplier;
  private final Supplier<SslConnectionParams> sslSupplier;
  private final Supplier<ScanServerSelector> scanServerSelectorSupplier;
  private final Supplier<ServiceLockPaths> serverPaths;
  private final NamespaceMapping namespaces;
  private TCredentials rpcCreds;
  private ThriftTransportPool thriftTransportPool;
  private ZookeeperLockChecker zkLockChecker;

  private final AtomicBoolean closed = new AtomicBoolean();

  private SecurityOperations secops = null;
  private final TableOperationsImpl tableops;
  private final NamespaceOperations namespaceops;
  private InstanceOperations instanceops = null;
  private final SingletonReservation singletonReservation;
  private final ThreadPools clientThreadPools;
  private ThreadPoolExecutor cleanupThreadPool;
  private ThreadPoolExecutor scannerReadaheadPool;
  private MeterRegistry micrometer;
  private Caches caches;

  private final AtomicBoolean zooKeeperOpened = new AtomicBoolean(false);
  private final Supplier<ZooSession> zooSession;

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("This client was closed.");
    }
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
          return () -> getServerPaths().getScanServer(rg -> true, AddressSelector.all(), true)
              .stream().map(entry -> new ScanServerInfo() {
                @Override
                public String getAddress() {
                  return entry.getServer();
                }

                @Override
                public String getGroup() {
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
   * Create a client context with the provided configuration. Legacy client code must provide a
   * no-op SingletonReservation to preserve behavior prior to 2.x. Clients since 2.x should call
   * Accumulo.newClient() builder, which will create a client reservation in
   * {@link ClientBuilderImpl#buildClient}
   */
  public ClientContext(SingletonReservation reservation, ClientInfo info,
      AccumuloConfiguration serverConf, UncaughtExceptionHandler ueh) {
    this.info = info;
    this.hadoopConf = info.getHadoopConf();

    this.zooSession = memoize(() -> {
      var zk = info
          .getZooKeeperSupplier(getClass().getSimpleName() + "(" + info.getPrincipal() + ")").get();
      zooKeeperOpened.set(true);
      return zk;
    });

    this.zooCache = memoize(() -> new ZooCache(getZooSession()));
    this.accumuloConf = serverConf;
    timeoutSupplier = memoizeWithExpiration(
        () -> getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT), 100, MILLISECONDS);
    sslSupplier = memoize(() -> SslConnectionParams.forClient(getConfiguration()));
    saslSupplier = memoizeWithExpiration(
        () -> SaslConnectionParams.from(getConfiguration(), getCredentials().getToken()), 100,
        MILLISECONDS);
    scanServerSelectorSupplier = memoize(this::createScanServerSelector);
    this.singletonReservation = Objects.requireNonNull(reservation);
    this.tableops = new TableOperationsImpl(this);
    this.namespaceops = new NamespaceOperationsImpl(this, tableops);
    this.serverPaths =
        Suppliers.memoize(() -> new ServiceLockPaths(this.getZooKeeperRoot(), this.getZooCache()));
    if (ueh == Threads.UEH) {
      clientThreadPools = ThreadPools.getServerThreadPools();
    } else {
      // Provide a default UEH that just logs the error
      if (ueh == null) {
        clientThreadPools = ThreadPools.getClientThreadPools((t, e) -> {
          log.error("Caught an Exception in client background thread: {}. Thread is dead.", t, e);
        });
      } else {
        clientThreadPools = ThreadPools.getClientThreadPools(ueh);
      }
    }
    this.namespaces = new NamespaceMapping(this);
  }

  public Ample getAmple() {
    ensureOpen();
    return new AmpleImpl(this);
  }

  public synchronized Future<List<KeyValue>>
      submitScannerReadAheadTask(Callable<List<KeyValue>> c) {
    ensureOpen();
    if (scannerReadaheadPool == null) {
      scannerReadaheadPool = clientThreadPools.getPoolBuilder(SCANNER_READ_AHEAD_POOL)
          .numCoreThreads(0).numMaxThreads(Integer.MAX_VALUE).withTimeOut(3L, SECONDS)
          .withQueue(new SynchronousQueue<>()).build();
    }
    return scannerReadaheadPool.submit(c);
  }

  public synchronized void executeCleanupTask(Runnable r) {
    ensureOpen();
    if (cleanupThreadPool == null) {
      cleanupThreadPool = clientThreadPools.getPoolBuilder(CONDITIONAL_WRITER_CLEANUP_POOL)
          .numCoreThreads(1).withTimeOut(3L, SECONDS).build();
    }
    this.cleanupThreadPool.execute(r);
  }

  /**
   * @return ThreadPools instance optionally configured with client UncaughtExceptionHandler
   */
  public ThreadPools threadPools() {
    ensureOpen();
    return clientThreadPools;
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
  public Map<String,Pair<UUID,String>> getScanServers() {
    ensureOpen();
    Map<String,Pair<UUID,String>> liveScanServers = new HashMap<>();
    Set<ServiceLockPath> scanServerPaths =
        getServerPaths().getScanServer(rg -> true, AddressSelector.all(), true);
    for (ServiceLockPath path : scanServerPaths) {
      try {
        ZcStat stat = new ZcStat();
        Optional<ServiceLockData> sld = ServiceLock.getLockData(getZooCache(), path, stat);
        if (sld.isPresent()) {
          final ServiceLockData data = sld.orElseThrow();
          final String addr = data.getAddressString(ThriftService.TABLET_SCAN);
          final UUID uuid = data.getServerUUID(ThriftService.TABLET_SCAN);
          final String group = data.getGroup(ThriftService.TABLET_SCAN);
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

  public String getZooKeeperRoot() {
    return ZooUtil.getRoot(getInstanceID());
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

  private TableZooHelper tableZooHelper;

  private synchronized TableZooHelper tableZooHelper() {
    ensureOpen();
    if (tableZooHelper == null) {
      tableZooHelper = new TableZooHelper(this);
    }
    return tableZooHelper;
  }

  public TableId getTableId(String tableName) throws TableNotFoundException {
    return tableZooHelper().getTableId(tableName);
  }

  public TableId _getTableIdDetectNamespaceNotFound(String tableName)
      throws NamespaceNotFoundException, TableNotFoundException {
    return tableZooHelper()._getTableIdDetectNamespaceNotFound(tableName);
  }

  public String getTableName(TableId tableId) throws TableNotFoundException {
    return tableZooHelper().getTableName(tableId);
  }

  public Map<String,TableId> getTableNameToIdMap() {
    return tableZooHelper().getTableMap().getNameToIdMap();
  }

  public Map<NamespaceId,String> getNamespaceIdToNameMap() {
    ensureOpen();
    return Namespaces.getIdToNameMap(this);
  }

  public Map<TableId,String> getTableIdToNameMap() {
    return tableZooHelper().getTableMap().getIdtoNameMap();
  }

  public boolean tableNodeExists(TableId tableId) {
    return tableZooHelper().tableNodeExists(tableId);
  }

  public void clearTableListCache() {
    tableZooHelper().clearTableListCache();
  }

  public String getPrintableTableInfoFromId(TableId tableId) {
    return tableZooHelper().getPrintableTableInfoFromId(tableId);
  }

  public String getPrintableTableInfoFromName(String tableName) {
    return tableZooHelper().getPrintableTableInfoFromName(tableName);
  }

  public TableState getTableState(TableId tableId) {
    return tableZooHelper().getTableState(tableId, false);
  }

  public TableState getTableState(TableId tableId, boolean clearCachedState) {
    return tableZooHelper().getTableState(tableId, clearCachedState);
  }

  public NamespaceId getNamespaceId(TableId tableId) throws TableNotFoundException {
    return tableZooHelper().getNamespaceId(tableId);
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
    Objects.requireNonNull(numQueryThreads);
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
      if (zooKeeperOpened.get()) {
        zooSession.get().close();
      }
      if (thriftTransportPool != null) {
        thriftTransportPool.shutdown();
      }
      if (tableZooHelper != null) {
        tableZooHelper.close();
      }
      if (scannerReadaheadPool != null) {
        scannerReadaheadPool.shutdownNow(); // abort all tasks, client is shutting down
      }
      if (cleanupThreadPool != null) {
        cleanupThreadPool.shutdown(); // wait for shutdown tasks to execute
      }
      singletonReservation.close();
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
      SingletonReservation reservation = SingletonManager.getClientReservation();
      try {
        // ClientContext closes reservation unless a RuntimeException is thrown
        ClientInfo info = cbi.getClientInfo();
        var config = ClientConfConverter.toAccumuloConf(info.getClientProperties());
        return new ClientContext(reservation, info, config, cbi.getUncaughtExceptionHandler());
      } catch (RuntimeException e) {
        reservation.close();
        throw e;
      }
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
    ensureOpen();
    if (thriftTransportPool == null) {
      thriftTransportPool = ThriftTransportPool.startNew(this::getTransportPoolMaxAgeMillis);
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
      var zk = info.getZooKeeperSupplier(ZookeeperLockChecker.class.getSimpleName()).get();
      this.zkLockChecker = new ZookeeperLockChecker(new ZooCache(zk), getZooKeeperRoot());
    }
    return this.zkLockChecker;
  }

  public ServiceLockPaths getServerPaths() {
    return this.serverPaths.get();
  }

  public NamespaceMapping getNamespaces() {
    ensureOpen();
    return namespaces;
  }

}
