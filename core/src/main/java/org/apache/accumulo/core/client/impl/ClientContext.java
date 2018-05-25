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
import static java.util.Objects.requireNonNull;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConnectionInfo;
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

  protected final Instance inst;
  private Credentials creds;
  private Properties clientProps;
  private BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
  private final AccumuloConfiguration rpcConf;
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

  public ClientContext(ConnectionInfo connectionInfo) {
    this(ConnectionInfoFactory.getInstance(connectionInfo),
        ConnectionInfoFactory.getCredentials(connectionInfo), connectionInfo.getProperties(),
        ConnectionInfoFactory.getBatchWriterConfig(connectionInfo));
  }

  public ClientContext(Instance instance, Credentials credentials, Properties clientProps) {
    this(instance, credentials, clientProps, new BatchWriterConfig());
  }

  public ClientContext(Instance instance, Credentials credentials, Properties clientProps,
      BatchWriterConfig batchWriterConfig) {
    this(instance, credentials,
        ClientConfConverter.toAccumuloConf(requireNonNull(clientProps, "clientProps is null")));
    this.clientProps = clientProps;
    this.batchWriterConfig = batchWriterConfig;
  }

  /**
   * Instantiate a client context from an existing {@link AccumuloConfiguration}. This is primarily
   * intended for subclasses and testing.
   */
  public ClientContext(Instance instance, Credentials credentials,
      AccumuloConfiguration serverConf) {
    inst = requireNonNull(instance, "instance is null");
    creds = requireNonNull(credentials, "credentials is null");
    rpcConf = requireNonNull(serverConf, "serverConf is null");
    clientProps = null;

    saslSupplier = () -> {
      // Use the clientProps if we have it
      if (null != clientProps) {
        if (!ClientProperty.SASL_ENABLED.getBoolean(clientProps)) {
          return null;
        }
        return new SaslConnectionParams(clientProps, getCredentials().getToken());
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

  public ConnectionInfo getConnectionInfo() {
    return new ConnectionInfoImpl(clientProps, creds.getToken());
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
    return rpcConf;
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
}
