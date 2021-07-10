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
package org.apache.accumulo.server;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope.WAL;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.crypto.CryptoServiceFactory.ClassloaderType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.ZooConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.metadata.ServerAmpleImpl;
import org.apache.accumulo.server.rpc.SaslServerConnectionParams;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Provides a server context for Accumulo server components that operate with the system credentials
 * and have access to the system files and configuration.
 */
public class ServerContext extends ClientContext {

  private final ServerInfo info;
  private final ZooReaderWriter zooReaderWriter;
  private TableManager tableManager;
  private UniqueNameAllocator nameAllocator;
  private ServerConfigurationFactory serverConfFactory = null;
  private DefaultConfiguration defaultConfig = null;
  private AccumuloConfiguration systemConfig = null;
  private AuthenticationTokenSecretManager secretManager;
  private CryptoService walCryptoService = null;
  private Map<TableId,CryptoService> tableCryptoMap;

  public ServerContext(SiteConfiguration siteConfig) {
    this(new ServerInfo(siteConfig));
  }

  private ServerContext(ServerInfo info) {
    super(SingletonReservation.noop(), info, info.getSiteConfiguration());
    this.info = info;
    zooReaderWriter = new ZooReaderWriter(info.getSiteConfiguration());
  }

  /**
   * Used during initialization to set the instance name and ID.
   */
  public static ServerContext initialize(SiteConfiguration siteConfig, String instanceName,
      String instanceID) {
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
  public String getInstanceID() {
    return info.getInstanceID();
  }

  /**
   * Should only be called by the Tablet server
   */
  public synchronized void setupLogCrypto() throws CryptoService.CryptoException {
    if (walCryptoService != null) {
      throw new CryptoService.CryptoException("Crypto Service "
          + walCryptoService.getClass().getName() + " already exists and cannot be setup again");
    }

    AccumuloConfiguration acuConf = getConfiguration();
    walCryptoService = CryptoServiceFactory.newInstance(acuConf, ClassloaderType.ACCUMULO, WAL);
  }

  public SiteConfiguration getSiteConfiguration() {
    return info.getSiteConfiguration();
  }

  public synchronized ServerConfigurationFactory getServerConfFactory() {
    if (serverConfFactory == null) {
      serverConfFactory = new ServerConfigurationFactory(this, info.getSiteConfiguration());
    }
    return serverConfFactory;
  }

  @Override
  public AccumuloConfiguration getConfiguration() {
    if (systemConfig == null) {
      ZooCache propCache = new ZooCache(getZooKeepers(), getZooKeepersSessionTimeOut());
      systemConfig = new ZooConfiguration(this, propCache, getSiteConfiguration());
    }
    return systemConfig;
  }

  public TableConfiguration getTableConfiguration(TableId id) {
    return getServerConfFactory().getTableConfiguration(id);
  }

  public NamespaceConfiguration getNamespaceConfiguration(NamespaceId namespaceId) {
    return getServerConfFactory().getNamespaceConfiguration(namespaceId);
  }

  public DefaultConfiguration getDefaultConfiguration() {
    if (defaultConfig == null) {
      defaultConfig = DefaultConfiguration.getInstance();
    }
    return defaultConfig;
  }

  /**
   * A "client-side" assertion for servers to validate that they are logged in as the expected user,
   * per the configuration, before performing any RPC
   */
  // Should be private, but package-protected so EasyMock will work
  void enforceKerberosLogin() {
    final AccumuloConfiguration conf = getServerConfFactory().getSiteConfiguration();
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
    AccumuloConfiguration conf = getServerConfFactory().getSiteConfiguration();
    if (!conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      return null;
    }
    return new SaslServerConnectionParams(conf, getCredentials().getToken(), secretManager);
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

  public void setSecretManager(AuthenticationTokenSecretManager secretManager) {
    this.secretManager = secretManager;
  }

  public AuthenticationTokenSecretManager getSecretManager() {
    return secretManager;
  }

  public synchronized TableManager getTableManager() {
    if (tableManager == null) {
      tableManager = new TableManager(this);
    }
    return tableManager;
  }

  public synchronized UniqueNameAllocator getUniqueNameAllocator() {
    if (nameAllocator == null) {
      nameAllocator = new UniqueNameAllocator(this);
    }
    return nameAllocator;
  }

  public CryptoService getLogCryptoService() {
    if (walCryptoService == null) {
      throw new CryptoService.CryptoException("Crypto service not initialized.");
    }
    return walCryptoService;
  }

  @Override
  public Ample getAmple() {
    return new ServerAmpleImpl(this);
  }

}
