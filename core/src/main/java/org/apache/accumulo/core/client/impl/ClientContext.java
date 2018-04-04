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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ConnectionInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.CredentialProviderFactoryShim;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

/**
 * This class represents any essential configuration and credentials needed to initiate RPC operations throughout the code. It is intended to represent a shared
 * object that contains these things from when the client was first constructed. It is not public API, and is only an internal representation of the context in
 * which a client is executing RPCs. If additional parameters are added to the public API that need to be used in the internals of Accumulo, they should be
 * added to this object for later retrieval, rather than as a separate parameter. Any state in this object should be available at the time of its construction.
 */
public class ClientContext {

  private static final Logger log = LoggerFactory.getLogger(ClientContext.class);

  protected final Instance inst;
  private Credentials creds;
  private ClientConfiguration clientConf;
  private BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
  private final AccumuloConfiguration rpcConf;
  protected Connector conn;

  // These fields are very frequently accessed (each time a connection is created) and expensive to compute, so cache them.
  private Supplier<Long> timeoutSupplier;
  private Supplier<SaslConnectionParams> saslSupplier;
  private Supplier<SslConnectionParams> sslSupplier;
  private TCredentials rpcCreds;

  private static <T> Supplier<T> memoizeWithExpiration(Supplier<T> s) {
    //This insanity exists to make modernizer plugin happy.  We are living in the future now.
    return () -> Suppliers.memoizeWithExpiration(() -> s.get(), 100, TimeUnit.MILLISECONDS).get();
  }

  public ClientContext(ConnectionInfo connectionInfo) {
    this(ConnectionInfoFactory.getInstance(connectionInfo), ConnectionInfoFactory.getCredentials(connectionInfo), ConnectionInfoFactory
        .getClientConfiguration(connectionInfo), ConnectionInfoFactory.getBatchWriterConfig(connectionInfo));
  }

  /**
   * Instantiate a client context
   */
  public ClientContext(Instance instance, Credentials credentials, ClientConfiguration clientConf) {
    this(instance, credentials, clientConf, new BatchWriterConfig());
  }

  public ClientContext(Instance instance, Credentials credentials, ClientConfiguration clientConf, BatchWriterConfig batchWriterConfig) {
    this(instance, credentials, convertClientConfig(requireNonNull(clientConf, "clientConf is null")));
    this.clientConf = clientConf;
    this.batchWriterConfig = batchWriterConfig;
  }

  /**
   * Instantiate a client context from an existing {@link AccumuloConfiguration}. This is primarily intended for subclasses and testing.
   */
  public ClientContext(Instance instance, Credentials credentials, AccumuloConfiguration serverConf) {
    inst = requireNonNull(instance, "instance is null");
    creds = requireNonNull(credentials, "credentials is null");
    rpcConf = requireNonNull(serverConf, "serverConf is null");
    clientConf = null;

    saslSupplier = new Supplier<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams get() {
        // Use the clientConf if we have it
        if (null != clientConf) {
          if (!clientConf.hasSasl()) {
            return null;
          }
          return new SaslConnectionParams(clientConf, getCredentials().getToken());
        }
        AccumuloConfiguration conf = getConfiguration();
        if (!conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
          return null;
        }
        return new SaslConnectionParams(conf, getCredentials().getToken());
      }
    };

    timeoutSupplier = memoizeWithExpiration(() -> getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
    sslSupplier = memoizeWithExpiration(() -> SslConnectionParams.forClient(getConfiguration()));
    saslSupplier = memoizeWithExpiration(saslSupplier);

  }

  /**
   * Retrieve the instance used to construct this context
   */
  public Instance getInstance() {
    return inst;
  }

  /**
   * Retrieve the credentials used to construct this context
   */
  public synchronized Credentials getCredentials() {
    return creds;
  }

  /**
   * Update the credentials in the current context after changing the current user's password or other auth token
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

  /**
   * A utility method for converting client configuration to a standard configuration object for use internally.
   *
   * @param config
   *          the original {@link ClientConfiguration}
   * @return the client configuration presented in the form of an {@link AccumuloConfiguration}
   */
  public static AccumuloConfiguration convertClientConfig(final ClientConfiguration config) {

    final AccumuloConfiguration defaults = DefaultConfiguration.getInstance();

    return new AccumuloConfiguration() {

      @Override
      public String get(Property property) {
        final String key = property.getKey();

        // Attempt to load sensitive properties from a CredentialProvider, if configured
        if (property.isSensitive()) {
          org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
          if (null != hadoopConf) {
            try {
              char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf, key);
              if (null != value) {
                log.trace("Loaded sensitive value for {} from CredentialProvider", key);
                return new String(value);
              } else {
                log.trace("Tried to load sensitive value for {} from CredentialProvider, but none was found", key);
              }
            } catch (IOException e) {
              log.warn("Failed to extract sensitive property ({}) from Hadoop CredentialProvider, falling back to base AccumuloConfiguration", key, e);
            }
          }
        }

        if (config.containsKey(key))
          return config.getString(key);
        else {
          // Reconstitute the server kerberos property from the client config
          if (Property.GENERAL_KERBEROS_PRINCIPAL == property) {
            if (config.containsKey(ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY.getKey())) {
              // Avoid providing a realm since we don't know what it is...
              return config.getString(ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY.getKey()) + "/_HOST@" + SaslConnectionParams.getDefaultRealm();
            }
          }
          return defaults.get(property);
        }
      }

      @Override
      public void getProperties(Map<String,String> props, Predicate<String> filter) {
        defaults.getProperties(props, filter);

        Iterator<String> keyIter = config.getKeys();
        while (keyIter.hasNext()) {
          String key = keyIter.next().toString();
          if (filter.test(key))
            props.put(key, config.getString(key));
        }

        // Two client props that don't exist on the server config. Client doesn't need to know about the Kerberos instance from the principle, but servers do
        // Automatically reconstruct the server property when converting a client config.
        if (props.containsKey(ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY.getKey())) {
          final String serverPrimary = props.remove(ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY.getKey());
          if (filter.test(Property.GENERAL_KERBEROS_PRINCIPAL.getKey())) {
            // Use the _HOST expansion. It should be unnecessary in "client land".
            props.put(Property.GENERAL_KERBEROS_PRINCIPAL.getKey(), serverPrimary + "/_HOST@" + SaslConnectionParams.getDefaultRealm());
          }
        }

        // Attempt to load sensitive properties from a CredentialProvider, if configured
        org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
        if (null != hadoopConf) {
          try {
            for (String key : CredentialProviderFactoryShim.getKeys(hadoopConf)) {
              if (!Property.isValidPropertyKey(key) || !Property.isSensitive(key)) {
                continue;
              }

              if (filter.test(key)) {
                char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf, key);
                if (null != value) {
                  props.put(key, new String(value));
                }
              }
            }
          } catch (IOException e) {
            log.warn("Failed to extract sensitive properties from Hadoop CredentialProvider, falling back to accumulo-site.xml", e);
          }
        }
      }

      private org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        String credProviderPaths = config.getString(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
        if (null != credProviderPaths && !credProviderPaths.isEmpty()) {
          org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
          hadoopConf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, credProviderPaths);
          return hadoopConf;
        }

        log.trace("Did not find credential provider configuration in ClientConfiguration");

        return null;
      }
    };

  }

}
