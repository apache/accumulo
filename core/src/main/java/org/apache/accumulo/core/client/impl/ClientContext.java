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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.security.thrift.TCredentials;

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

  private ClientInfo info;
  private Instance inst;
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
    return () -> Suppliers.memoizeWithExpiration(() -> s.get(), 100, TimeUnit.MILLISECONDS).get();
  }

  public ClientContext(ClientInfo info) {
    this(info, ClientInfoFactory.getInstance(info), ClientInfoFactory.getCredentials(info),
        ClientConfConverter.toAccumuloConf(info.getProperties()));
  }

  /**
   * Instantiate a client context from an existing {@link AccumuloConfiguration}. This is primarily
   * intended for subclasses and testing.
   */
  public ClientContext(Instance instance, Credentials credentials,
      AccumuloConfiguration serverConf) {
    this(null, instance, credentials, serverConf);
  }

  public ClientContext(ClientInfo info, Instance instance, Credentials credentials,
      AccumuloConfiguration serverConf) {
    this.info = info;
    inst = instance;
    creds = credentials;
    this.serverConf = serverConf;
    saslSupplier = () -> {
      // Use the clientProps if we have it
      if (info != null) {
        if (!ClientProperty.SASL_ENABLED.getBoolean(info.getProperties())) {
          return null;
        }
        return new SaslConnectionParams(info.getProperties(), getCredentials().getToken());
      }
      AccumuloConfiguration conf = getConfiguration();
      if (!conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        return null;
      }
      return new SaslConnectionParams(conf, getCredentials().getToken());
    };

    timeoutSupplier = memoizeWithExpiration(
        () -> getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
    sslSupplier = memoizeWithExpiration(() -> SslConnectionParams.forClient(getConfiguration()));
    saslSupplier = memoizeWithExpiration(saslSupplier);
  }

  /**
   * Retrieve the instance used to construct this context
   */
  public Instance getInstance() {
    return inst;
  }

  public ClientInfo getClientInfo() {
    if (info == null) {
      info = new ClientInfoImpl(ClientConfConverter.toProperties(serverConf, inst, creds));
    }
    return info;
  }

  /**
   * Retrieve the credentials used to construct this context
   */
  public synchronized Credentials getCredentials() {
    return creds;
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
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    // avoid making more connectors than necessary
    if (conn == null) {
      if (getInstance() instanceof ZooKeeperInstance) {
        // reuse existing context
        conn = new ConnectorImpl(this);
      } else {
        Credentials c = getCredentials();
        conn = getInstance().getConnector(c.getPrincipal(), c.getToken());
      }
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
      rpcCreds = getCredentials().toThrift(getInstance());
    }

    return rpcCreds;
  }

  /**
   * Returns the location of the tablet server that is serving the root tablet.
   *
   * @return location in "hostname:port" form
   */
  public String getRootTabletLocation() {
    return inst.getRootTabletLocation();
  }

  /**
   * Returns the location(s) of the accumulo master and any redundant servers.
   *
   * @return a list of locations in "hostname:port" form
   */
  public List<String> getMasterLocations() {
    return inst.getMasterLocations();
  }

  /**
   * Returns a unique string that identifies this instance of accumulo.
   *
   * @return a UUID
   */
  public String getInstanceID() {
    return inst.getInstanceID();
  }

  /**
   * Returns the instance name given at system initialization time.
   *
   * @return current instance name
   */
  public String getInstanceName() {
    return inst.getInstanceName();
  }

  /**
   * Returns a comma-separated list of zookeeper servers the instance is using.
   *
   * @return the zookeeper servers this instance is using in "hostname:port" form
   */
  public String getZooKeepers() {
    return inst.getZooKeepers();
  }

  /**
   * Returns the zookeeper connection timeout.
   *
   * @return the configured timeout to connect to zookeeper
   */
  public int getZooKeepersSessionTimeOut() {
    return inst.getZooKeepersSessionTimeOut();
  }
}
