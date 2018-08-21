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
package org.apache.accumulo.core.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

/**
 * This class represents any essential configuration and credentials needed to initiate RPC
 * operations throughout the code. It is intended to represent a shared object that contains these
 * things from when the client was first constructed. It is not public API, and is only an internal
 * representation of the context in which a client is executing RPCs. If additional parameters are
 * added to the public API that need to be used in the internals of Accumulo, they should be added
 * to this object for later retrieval, rather than as a separate parameter. Any state in this object
 * should be available at the time of its construction.
 */
public class ClientContext {

  private static final Logger log = LoggerFactory.getLogger(ClientContext.class);

  private ClientInfo info;
  private String instanceId = null;
  private final ZooCache zooCache;

  private Credentials creds;
  private BatchWriterConfig batchWriterConfig;
  private AccumuloConfiguration serverConf;
  protected Connector conn;

  // These fields are very frequently accessed (each time a connection is created) and expensive to
  // compute, so cache them.
  private Supplier<Long> timeoutSupplier;
  private Supplier<SaslConnectionParams> saslSupplier;
  private Supplier<SslConnectionParams> sslSupplier;
  private TCredentials rpcCreds;

  private static <T> Supplier<T> memoizeWithExpiration(Supplier<T> s) {
    // This insanity exists to make modernizer plugin happy. We are living in the future now.
    return () -> Suppliers.memoizeWithExpiration(s::get, 100, TimeUnit.MILLISECONDS).get();
  }

  public ClientContext(ClientInfo info) {
    this(info, ClientConfConverter.toAccumuloConf(info.getProperties()));
  }

  public ClientContext(ClientInfo info, AccumuloConfiguration serverConf) {
    this.info = info;
    zooCache = new ZooCacheFactory().getZooCache(info.getZooKeepers(),
        info.getZooKeepersSessionTimeOut());
    this.serverConf = serverConf;
    timeoutSupplier = memoizeWithExpiration(
        () -> getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
    sslSupplier = memoizeWithExpiration(() -> SslConnectionParams.forClient(getConfiguration()));
    saslSupplier = memoizeWithExpiration(
        () -> SaslConnectionParams.from(getConfiguration(), getCredentials().getToken()));
  }

  /**
   * Retrieve the instance used to construct this context
   *
   * @deprecated since 2.0.0
   */
  @Deprecated
  public org.apache.accumulo.core.client.Instance getDeprecatedInstance() {
    final ClientContext context = this;
    return new org.apache.accumulo.core.client.Instance() {
      @Override
      public String getRootTabletLocation() {
        return context.getRootTabletLocation();
      }

      @Override
      public List<String> getMasterLocations() {
        return context.getMasterLocations();
      }

      @Override
      public String getInstanceID() {
        return context.getInstanceID();
      }

      @Override
      public String getInstanceName() {
        return context.getInstanceName();
      }

      @Override
      public String getZooKeepers() {
        return context.getZooKeepers();
      }

      @Override
      public int getZooKeepersSessionTimeOut() {
        return context.getZooKeepersSessionTimeOut();
      }

      @Override
      public Connector getConnector(String principal, AuthenticationToken token)
          throws AccumuloException, AccumuloSecurityException {
        return context.getConnector().changeUser(principal, token);
      }
    };
  }

  public ClientInfo getClientInfo() {
    return info;
  }

  /**
   * Retrieve the credentials used to construct this context
   */
  public synchronized Credentials getCredentials() {
    if (creds == null) {
      creds = new Credentials(info.getPrincipal(), info.getAuthenticationToken());
    }
    return creds;
  }

  public String getPrincipal() {
    return getCredentials().getPrincipal();
  }

  public AuthenticationToken getAuthenticationToken() {
    return getCredentials().getToken();
  }

  public Properties getProperties() {
    return info.getProperties();
  }

  /**
   * Update the credentials in the current context after changing the current user's password or
   * other auth token
   */
  public synchronized void setCredentials(Credentials newCredentials) {
    checkArgument(newCredentials != null, "newCredentials is null");
    creds = newCredentials;
    rpcCreds = null;
  }

  /**
   * Retrieve the configuration used to construct this context
   */
  public AccumuloConfiguration getConfiguration() {
    return serverConf;
  }

  /**
   * Retrieve the universal RPC client timeout from the configuration
   */
  public long getClientTimeoutInMillis() {
    return timeoutSupplier.get();
  }

  /**
   * Retrieve SSL/TLS configuration to initiate an RPC connection to a server
   */
  public SslConnectionParams getClientSslParams() {
    return sslSupplier.get();
  }

  /**
   * Retrieve SASL configuration to initiate an RPC connection to a server
   */
  public SaslConnectionParams getSaslParams() {
    return saslSupplier.get();
  }

  /**
   * Retrieve a connector
   */
  public synchronized Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    if (conn == null) {
      conn = new ConnectorImpl(this);
    }
    return conn;
  }

  public BatchWriterConfig getBatchWriterConfig() {
    if (batchWriterConfig == null) {
      batchWriterConfig = ClientInfoFactory.getBatchWriterConfig(getClientInfo());
    }
    return batchWriterConfig;
  }

  /**
   * Serialize the credentials just before initiating the RPC call
   */
  public synchronized TCredentials rpcCreds() {
    if (getCredentials().getToken().isDestroyed()) {
      rpcCreds = null;
    }

    if (rpcCreds == null) {
      rpcCreds = getCredentials().toThrift(getInstanceID());
    }

    return rpcCreds;
  }

  /**
   * Returns the location of the tablet server that is serving the root tablet.
   *
   * @return location in "hostname:port" form
   */
  public String getRootTabletLocation() {
    String zRootLocPath = getZooKeeperRoot() + RootTable.ZROOT_TABLET_LOCATION;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up root tablet location in zookeeper.",
          Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = zooCache.get(zRootLocPath);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found root tablet at {} in {}", Thread.currentThread().getId(),
          (loc == null ? "null" : new String(loc, UTF_8)),
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    if (loc == null) {
      return null;
    }

    return new String(loc, UTF_8).split("\\|")[0];
  }

  /**
   * Returns the location(s) of the accumulo master and any redundant servers.
   *
   * @return a list of locations in "hostname:port" form
   */
  public List<String> getMasterLocations() {
    String masterLocPath = getZooKeeperRoot() + Constants.ZMASTER_LOCK;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up master location in zookeeper.", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = ZooUtil.getLockData(zooCache, masterLocPath);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found master at {} in {}", Thread.currentThread().getId(),
          (loc == null ? "null" : new String(loc, UTF_8)),
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    if (loc == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(new String(loc, UTF_8));
  }

  /**
   * Returns a unique string that identifies this instance of accumulo.
   *
   * @return a UUID
   */
  public String getInstanceID() {
    final String instanceName = info.getInstanceName();
    if (instanceId == null) {
      // want the instance id to be stable for the life of this instance object,
      // so only get it once
      String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
      byte[] iidb = zooCache.get(instanceNamePath);
      if (iidb == null) {
        throw new RuntimeException(
            "Instance name " + instanceName + " does not exist in zookeeper. "
                + "Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
      }
      instanceId = new String(iidb, UTF_8);
    }

    if (zooCache.get(Constants.ZROOT + "/" + instanceId) == null) {
      if (instanceName == null)
        throw new RuntimeException("Instance id " + instanceId + " does not exist in zookeeper");
      throw new RuntimeException("Instance id " + instanceId + " pointed to by the name "
          + instanceName + " does not exist in zookeeper");
    }

    return instanceId;
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
    return info.getInstanceName();
  }

  /**
   * Returns a comma-separated list of zookeeper servers the instance is using.
   *
   * @return the zookeeper servers this instance is using in "hostname:port" form
   */
  public String getZooKeepers() {
    return info.getZooKeepers();
  }

  /**
   * Returns the zookeeper connection timeout.
   *
   * @return the configured timeout to connect to zookeeper
   */
  public int getZooKeepersSessionTimeOut() {
    return info.getZooKeepersSessionTimeOut();
  }

  public ZooCache getZooCache() {
    return zooCache;
  }
}
