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
package org.apache.accumulo.server;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoize;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.core.spi.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.metadata.ServerAmpleImpl;
import org.apache.accumulo.server.rpc.SaslServerConnectionParams;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a server context for Accumulo server components that operate with the system credentials
 * and have access to the system files and configuration.
 */
public class ServerContext extends ClientContext {
  private static final Logger log = LoggerFactory.getLogger(ServerContext.class);

  private final ServerInfo info;
  private final ZooReaderWriter zooReaderWriter;
  private final ServerDirs serverDirs;
  private final Supplier<ZooPropStore> propStore;

  // lazily loaded resources, only loaded when needed
  private final Supplier<TableManager> tableManager;
  private final Supplier<UniqueNameAllocator> nameAllocator;
  private final Supplier<ServerConfigurationFactory> serverConfFactory;
  private final Supplier<AuthenticationTokenSecretManager> secretManager;
  private final Supplier<ScheduledThreadPoolExecutor> sharedScheduledThreadPool;
  private final Supplier<AuditedSecurityOperation> securityOperation;
  private final Supplier<CryptoServiceFactory> cryptoFactorySupplier;

  public ServerContext(SiteConfiguration siteConfig) {
    this(new ServerInfo(siteConfig));
  }

  private ServerContext(ServerInfo info) {
    super(SingletonReservation.noop(), info, info.getSiteConfiguration(), Threads.UEH);
    this.info = info;
    zooReaderWriter = new ZooReaderWriter(info.getSiteConfiguration());
    serverDirs = info.getServerDirs();

    propStore = memoize(() -> ZooPropStore.initialize(getInstanceID(), getZooReaderWriter()));

    tableManager = memoize(() -> new TableManager(this));
    nameAllocator = memoize(() -> new UniqueNameAllocator(this));
    serverConfFactory = memoize(() -> new ServerConfigurationFactory(this, getSiteConfiguration()));
    secretManager = memoize(() -> new AuthenticationTokenSecretManager(getInstanceID(),
        getConfiguration().getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_LIFETIME)));
    cryptoFactorySupplier = memoize(() -> CryptoFactoryLoader.newInstance(getConfiguration()));
    sharedScheduledThreadPool = memoize(() -> ThreadPools.getServerThreadPools()
        .createGeneralScheduledExecutorService(getConfiguration()));
    securityOperation =
        memoize(() -> new AuditedSecurityOperation(this, SecurityOperation.getAuthorizor(this),
            SecurityOperation.getAuthenticator(this), SecurityOperation.getPermHandler(this)));
  }

  /**
   * Used during initialization to set the instance name and ID.
   */
  public static ServerContext initialize(SiteConfiguration siteConfig, String instanceName,
      InstanceId instanceID) {
    return new ServerContext(new ServerInfo(siteConfig, instanceName, instanceID));
  }

  /**
   * Override properties for testing
   */
  public static ServerContext override(SiteConfiguration siteConfig, String instanceName,
      String zooKeepers, int zkSessionTimeOut) {
    return new ServerContext(
        new ServerInfo(siteConfig, instanceName, zooKeepers, zkSessionTimeOut));
  }

  @Override
  public InstanceId getInstanceID() {
    return info.getInstanceID();
  }

  public SiteConfiguration getSiteConfiguration() {
    return info.getSiteConfiguration();
  }

  @Override
  public AccumuloConfiguration getConfiguration() {
    return serverConfFactory.get().getSystemConfiguration();
  }

  public TableConfiguration getTableConfiguration(TableId id) {
    return serverConfFactory.get().getTableConfiguration(id);
  }

  public NamespaceConfiguration getNamespaceConfiguration(NamespaceId namespaceId) {
    return serverConfFactory.get().getNamespaceConfiguration(namespaceId);
  }

  public DefaultConfiguration getDefaultConfiguration() {
    return DefaultConfiguration.getInstance();
  }

  public ServerDirs getServerDirs() {
    return serverDirs;
  }

  /**
   * A "client-side" assertion for servers to validate that they are logged in as the expected user,
   * per the configuration, before performing any RPC
   */
  // Should be private, but package-protected so EasyMock will work
  void enforceKerberosLogin() {
    final AccumuloConfiguration conf = getSiteConfiguration();
    // Unwrap _HOST into the FQDN to make the kerberos principal we'll compare against
    final String kerberosPrincipal =
        SecurityUtil.getServerPrincipal(conf.get(Property.GENERAL_KERBEROS_PRINCIPAL));
    UserGroupInformation loginUser;
    try {
      // The system user should be logged in via keytab when the process is started, not the
      // currentUser() like KerberosToken
      loginUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new RuntimeException("Could not get login user", e);
    }

    checkArgument(loginUser.hasKerberosCredentials(), "Server does not have Kerberos credentials");
    checkArgument(kerberosPrincipal.equals(loginUser.getUserName()),
        "Expected login user to be " + kerberosPrincipal + " but was " + loginUser.getUserName());
  }

  public VolumeManager getVolumeManager() {
    return info.getVolumeManager();
  }

  @Override
  public ZooReader getZooReader() {
    return getZooReaderWriter();
  }

  public ZooReaderWriter getZooReaderWriter() {
    return zooReaderWriter;
  }

  /**
   * Retrieve the SSL/TLS configuration for starting up a listening service
   */
  public SslConnectionParams getServerSslParams() {
    return SslConnectionParams.forServer(getConfiguration());
  }

  @Override
  public SaslServerConnectionParams getSaslParams() {
    AccumuloConfiguration conf = getSiteConfiguration();
    if (!conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      return null;
    }
    return new SaslServerConnectionParams(conf, getCredentials().getToken(), getSecretManager());
  }

  /**
   * Determine the type of Thrift server to instantiate given the server's configuration.
   *
   * @return A {@link ThriftServerType} value to denote the type of Thrift server to construct
   */
  public ThriftServerType getThriftServerType() {
    AccumuloConfiguration conf = getConfiguration();
    if (conf.getBoolean(Property.INSTANCE_RPC_SSL_ENABLED)) {
      if (conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        throw new IllegalStateException(
            "Cannot create a Thrift server capable of both SASL and SSL");
      }

      return ThriftServerType.SSL;
    } else if (conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      if (conf.getBoolean(Property.INSTANCE_RPC_SSL_ENABLED)) {
        throw new IllegalStateException(
            "Cannot create a Thrift server capable of both SASL and SSL");
      }

      return ThriftServerType.SASL;
    } else {
      // Lets us control the type of Thrift server created, primarily for benchmarking purposes
      String serverTypeName = conf.get(Property.GENERAL_RPC_SERVER_TYPE);
      return ThriftServerType.get(serverTypeName);
    }
  }

  public AuthenticationTokenSecretManager getSecretManager() {
    return secretManager.get();
  }

  public TableManager getTableManager() {
    return tableManager.get();
  }

  public UniqueNameAllocator getUniqueNameAllocator() {
    return nameAllocator.get();
  }

  public CryptoServiceFactory getCryptoFactory() {
    return cryptoFactorySupplier.get();
  }

  @Override
  public Ample getAmple() {
    return new ServerAmpleImpl(this);
  }

  public Set<String> getBaseUris() {
    return serverDirs.getBaseUris();
  }

  public List<Pair<Path,Path>> getVolumeReplacements() {
    return serverDirs.getVolumeReplacements();
  }

  public Set<String> getTablesDirs() {
    return serverDirs.getTablesDirs();
  }

  public Set<String> getRecoveryDirs() {
    return serverDirs.getRecoveryDirs();
  }

  /**
   * Check to see if this version of Accumulo can run against or upgrade the passed in data version.
   */
  public static void ensureDataVersionCompatible(int dataVersion) {
    if (!AccumuloDataVersion.CAN_RUN.contains(dataVersion)) {
      throw new IllegalStateException("This version of accumulo (" + Constants.VERSION
          + ") is not compatible with files stored using data version " + dataVersion);
    }
  }

  public void waitForZookeeperAndHdfs() {
    log.info("Attempting to talk to zookeeper");
    while (true) {
      try {
        getZooReaderWriter().getChildren(Constants.ZROOT);
        break;
      } catch (InterruptedException | KeeperException ex) {
        log.info("Waiting for accumulo to be initialized");
        sleepUninterruptibly(1, SECONDS);
      }
    }
    log.info("ZooKeeper connected and initialized, attempting to talk to HDFS");
    long sleep = 1000;
    int unknownHostTries = 3;
    while (true) {
      try {
        if (getVolumeManager().isReady()) {
          break;
        }
        log.warn("Waiting for the NameNode to leave safemode");
      } catch (IOException ex) {
        log.warn("Unable to connect to HDFS", ex);
      } catch (IllegalArgumentException e) {
        /* Unwrap the UnknownHostException so we can deal with it directly */
        if (e.getCause() instanceof UnknownHostException) {
          if (unknownHostTries > 0) {
            log.warn("Unable to connect to HDFS, will retry. cause: ", e.getCause());
            /*
             * We need to make sure our sleep period is long enough to avoid getting a cached
             * failure of the host lookup.
             */
            int ttl = AddressUtil.getAddressCacheNegativeTtl((UnknownHostException) e.getCause());
            sleep = Math.max(sleep, (ttl + 1) * 1000L);
          } else {
            log.error("Unable to connect to HDFS and exceeded the maximum number of retries.", e);
            throw e;
          }
          unknownHostTries--;
        } else {
          throw e;
        }
      }
      log.info("Backing off due to failure; current sleep period is {} seconds", sleep / 1000.);
      sleepUninterruptibly(sleep, TimeUnit.MILLISECONDS);
      /* Back off to give transient failures more time to clear. */
      sleep = Math.min(MINUTES.toMillis(1), sleep * 2);
    }
    log.info("Connected to HDFS");
  }

  /**
   * Wait for ZK and hdfs, check data version and some properties, and start thread to monitor
   * swappiness. Should only be called once during server start up.
   */
  public void init(String application) {
    final AccumuloConfiguration conf = getConfiguration();

    log.info("{} starting", application);
    log.info("Instance {}", getInstanceID());
    // It doesn't matter which Volume is used as they should all have the data version stored
    int dataVersion = serverDirs.getAccumuloPersistentVersion(getVolumeManager().getFirst());
    log.info("Data Version {}", dataVersion);
    waitForZookeeperAndHdfs();

    ensureDataVersionCompatible(dataVersion);

    TreeMap<String,String> sortedProps = new TreeMap<>();
    for (Map.Entry<String,String> entry : conf) {
      sortedProps.put(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String,String> entry : sortedProps.entrySet()) {
      String key = entry.getKey();
      log.info("{} = {}", key, (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
      Property prop = Property.getPropertyByKey(key);
      if (prop != null && conf.isPropertySet(prop)) {
        if (prop.isDeprecated()) {
          Property replacedBy = prop.replacedBy();
          if (replacedBy != null) {
            log.warn("{} is deprecated, use {} instead.", prop.getKey(), replacedBy.getKey());
          } else {
            log.warn("{} is deprecated", prop.getKey());
          }
        }
      }
    }

    monitorSwappiness();

    // Encourage users to configure TLS
    final String SSL = "SSL";
    for (Property sslProtocolProperty : Arrays.asList(Property.RPC_SSL_CLIENT_PROTOCOL,
        Property.RPC_SSL_ENABLED_PROTOCOLS, Property.MONITOR_SSL_INCLUDE_PROTOCOLS)) {
      String value = conf.get(sslProtocolProperty);
      if (value.contains(SSL)) {
        log.warn("It is recommended that {} only allow TLS", sslProtocolProperty);
      }
    }
  }

  private void monitorSwappiness() {
    ScheduledFuture<?> future = getScheduledExecutor().scheduleWithFixedDelay(() -> {
      try {
        String procFile = "/proc/sys/vm/swappiness";
        File swappiness = new File(procFile);
        if (swappiness.exists() && swappiness.canRead()) {
          try (InputStream is = new FileInputStream(procFile)) {
            byte[] buffer = new byte[10];
            int bytes = is.read(buffer);
            String setting = new String(buffer, 0, bytes, UTF_8);
            setting = setting.trim();
            if (bytes > 0 && Integer.parseInt(setting) > 10) {
              log.warn("System swappiness setting is greater than ten ({})"
                  + " which can cause time-sensitive operations to be delayed."
                  + " Accumulo is time sensitive because it needs to maintain"
                  + " distributed lock agreement.", setting);
            }
          }
        }
      } catch (Exception t) {
        log.error("", t);
      }
    }, SECONDS.toMillis(1), MINUTES.toMillis(10), TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  public ScheduledThreadPoolExecutor getScheduledExecutor() {
    return sharedScheduledThreadPool.get();
  }

  public PropStore getPropStore() {
    return propStore.get();
  }

  @Override
  protected long getTransportPoolMaxAgeMillis() {
    return getClientTimeoutInMillis();
  }

  public AuditedSecurityOperation getSecurityOperation() {
    return securityOperation.get();
  }

}
