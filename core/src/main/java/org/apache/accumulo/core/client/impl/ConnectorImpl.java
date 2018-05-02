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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.ConnectionInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.CredentialProviderToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Tracer;

public class ConnectorImpl extends Connector {
  private static final String SYSTEM_TOKEN_NAME = "org.apache.accumulo.server.security."
      + "SystemCredentials$SystemToken";
  private final ClientContext context;
  private SecurityOperations secops = null;
  private TableOperationsImpl tableops = null;
  private NamespaceOperations namespaceops = null;
  private InstanceOperations instanceops = null;
  private ReplicationOperations replicationops = null;

  public ConnectorImpl(final ClientContext context)
      throws AccumuloSecurityException, AccumuloException {
    checkArgument(context != null, "Context is null");
    checkArgument(context.getCredentials() != null, "Credentials are null");
    checkArgument(context.getCredentials().getToken() != null, "Authentication token is null");
    if (context.getCredentials().getToken().isDestroyed())
      throw new AccumuloSecurityException(context.getCredentials().getPrincipal(),
          SecurityErrorCode.TOKEN_EXPIRED);

    this.context = context;

    // Skip fail fast for system services; string literal for class name, to avoid dependency on
    // server jar
    final String tokenClassName = context.getCredentials().getToken().getClass().getName();
    if (!SYSTEM_TOKEN_NAME.equals(tokenClassName)) {
      ServerClient.executeVoid(context, iface -> {
        if (!iface.authenticate(Tracer.traceInfo(), context.rpcCreds()))
          throw new AccumuloSecurityException("Authentication failed, access denied",
              SecurityErrorCode.BAD_CREDENTIALS);
      });
    }

    this.tableops = new TableOperationsImpl(context);
    this.namespaceops = new NamespaceOperationsImpl(context, tableops);
  }

  private Table.ID getTableId(String tableName) throws TableNotFoundException {
    Table.ID tableId = Tables.getTableId(context.getInstance(), tableName);
    if (Tables.getTableState(context.getInstance(), tableId) == TableState.OFFLINE)
      throw new TableOfflineException(context.getInstance(), tableId.canonicalID());
    return tableId;
  }

  @Override
  public Instance getInstance() {
    return context.getInstance();
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchReader(context, getTableId(tableName), authorizations,
        numQueryThreads);
  }

  @Deprecated
  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, long maxMemory, long maxLatency, int maxWriteThreads)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchDeleter(context, getTableId(tableName), authorizations,
        numQueryThreads, new BatchWriterConfig().setMaxMemory(maxMemory)
            .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, BatchWriterConfig config) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchDeleter(context, getTableId(tableName), authorizations,
        numQueryThreads, config.merge(context.getBatchWriterConfig()));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    return createBatchDeleter(tableName, authorizations, numQueryThreads, new BatchWriterConfig());
  }

  @Deprecated
  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    return new BatchWriterImpl(context, getTableId(tableName),
        new BatchWriterConfig().setMaxMemory(maxMemory)
            .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    // we used to allow null inputs for bw config
    if (config == null) {
      config = new BatchWriterConfig();
    }
    return new BatchWriterImpl(context, getTableId(tableName),
        config.merge(context.getBatchWriterConfig()));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName) throws TableNotFoundException {
    return createBatchWriter(tableName, new BatchWriterConfig());
  }

  @Deprecated
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency,
      int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(context, new BatchWriterConfig().setMaxMemory(maxMemory)
        .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    return new MultiTableBatchWriterImpl(context, config.merge(context.getBatchWriterConfig()));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter() {
    return createMultiTableBatchWriter(new BatchWriterConfig());
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config)
      throws TableNotFoundException {
    return new ConditionalWriterImpl(context, getTableId(tableName), config);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new ScannerImpl(context, getTableId(tableName), authorizations);
  }

  @Override
  public String whoami() {
    return context.getCredentials().getPrincipal();
  }

  @Override
  public synchronized TableOperations tableOperations() {
    return tableops;
  }

  @Override
  public synchronized NamespaceOperations namespaceOperations() {
    return namespaceops;
  }

  @Override
  public synchronized SecurityOperations securityOperations() {
    if (secops == null)
      secops = new SecurityOperationsImpl(context);

    return secops;
  }

  @Override
  public synchronized InstanceOperations instanceOperations() {
    if (instanceops == null)
      instanceops = new InstanceOperationsImpl(context);

    return instanceops;
  }

  @Override
  public synchronized ReplicationOperations replicationOperations() {
    if (null == replicationops) {
      replicationops = new ReplicationOperationsImpl(context);
    }

    return replicationops;
  }

  @Override
  public ConnectionInfo info() {
    return this.context.getConnectionInfo();
  }

  private static void setAuthenticationProperties(Properties props, String principal, AuthenticationToken token) {
    props.setProperty(ClientProperty.AUTH_USERNAME.getKey(), principal);
    if (token instanceof CredentialProviderToken) {
      props.setProperty(ClientProperty.AUTH_METHOD.getKey(), "provider");
      CredentialProviderToken cpt = (CredentialProviderToken) token;
      props.setProperty(ClientProperty.AUTH_PROVIDER_NAME.getKey(), cpt.getName());
      props.setProperty(ClientProperty.AUTH_PROVIDER_URLS.getKey(), cpt.getCredentialProviders());
    } else if (token instanceof PasswordToken) {
      props.setProperty(ClientProperty.AUTH_METHOD.getKey(), "password");
    } else if (token instanceof KerberosToken) {
      props.setProperty(ClientProperty.AUTH_METHOD.getKey(), "kerberos");
      props.setProperty(ClientProperty.AUTH_KERBEROS_KEYTAB_PATH.getKey(),
          ((KerberosToken) token).getKeytab().getAbsolutePath());
    } else {
      props.setProperty(ClientProperty.AUTH_METHOD.getKey(), "unknown");
    }
  }

  public static class ConnectorBuilderImpl
      implements InstanceArgs, PropertyOptions, ConnectionInfoOptions, AuthenticationArgs,
      ConnectionOptions, SslOptions, SaslOptions, ConnectorFactory, FromOptions {

    private Properties properties = new Properties();
    private AuthenticationToken token = null;

    @Override
    public Connector build() throws AccumuloException, AccumuloSecurityException {
      return ConnectionInfoFactory.getConnector(new ConnectionInfoImpl(properties, token));
    }

    @Override
    public ConnectionInfo info() {
      return new ConnectionInfoImpl(properties, token);
    }

    @Override
    public AuthenticationArgs forInstance(String instanceName, String zookeepers) {
      setProperty(ClientProperty.INSTANCE_NAME, instanceName);
      setProperty(ClientProperty.INSTANCE_ZOOKEEPERS, zookeepers);
      return this;
    }

    @Override
    public SslOptions withTruststore(String path) {
      setProperty(ClientProperty.SSL_TRUSTSTORE_PATH, path);
      return this;
    }

    @Override
    public SslOptions withTruststore(String path, String password, String type) {
      setProperty(ClientProperty.SSL_TRUSTSTORE_PATH, path);
      setProperty(ClientProperty.SSL_TRUSTSTORE_PASSWORD, password);
      setProperty(ClientProperty.SSL_TRUSTSTORE_TYPE, type);
      return this;
    }

    @Override
    public SslOptions withKeystore(String path) {
      setProperty(ClientProperty.SSL_KEYSTORE_PATH, path);
      return this;
    }

    @Override
    public SslOptions withKeystore(String path, String password, String type) {
      setProperty(ClientProperty.SSL_KEYSTORE_PATH, path);
      setProperty(ClientProperty.SSL_KEYSTORE_PASSWORD, password);
      setProperty(ClientProperty.SSL_KEYSTORE_TYPE, type);
      return this;
    }

    @Override
    public SslOptions useJsse() {
      setProperty(ClientProperty.SSL_USE_JSSE, "true");
      return this;
    }

    @Override
    public ConnectionOptions withZkTimeout(int timeout) {
      setProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT_SEC, Integer.toString(timeout));
      return this;
    }

    @Override
    public SslOptions withSsl() {
      setProperty(ClientProperty.SSL_ENABLED, "true");
      return this;
    }

    @Override
    public SaslOptions withSasl() {
      setProperty(ClientProperty.SASL_ENABLED, "true");
      return this;
    }

    @Override
    public ConnectionOptions withBatchWriterConfig(BatchWriterConfig batchWriterConfig) {
      setProperty(ClientProperty.BATCH_WRITER_MAX_MEMORY_BYTES, batchWriterConfig.getMaxMemory());
      setProperty(ClientProperty.BATCH_WRITER_MAX_LATENCY_SEC,
          batchWriterConfig.getMaxLatency(TimeUnit.SECONDS));
      setProperty(ClientProperty.BATCH_WRITER_MAX_TIMEOUT_SEC,
          batchWriterConfig.getTimeout(TimeUnit.SECONDS));
      setProperty(ClientProperty.BATCH_WRITER_MAX_WRITE_THREADS,
          batchWriterConfig.getMaxWriteThreads());
      setProperty(ClientProperty.BATCH_WRITER_DURABILITY,
          batchWriterConfig.getDurability().toString());
      return this;
    }

    @Override
    public SaslOptions withPrimary(String kerberosServerPrimary) {
      setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY, kerberosServerPrimary);
      return this;
    }

    @Override
    public SaslOptions withQop(String qualityOfProtection) {
      setProperty(ClientProperty.SASL_QOP, qualityOfProtection);
      return this;
    }

    @Override
    public ConnectorFactory usingProperties(String configFile) {
      Properties properties = new Properties();
      try (InputStream is = new FileInputStream(configFile)) {
        properties.load(is);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      return usingProperties(properties);
    }

    @Override
    public ConnectorFactory usingProperties(Properties properties) {
      this.properties = properties;
      String authMethod = ClientProperty.AUTH_METHOD.getValue(properties).toLowerCase();
      switch (authMethod) {
        case "password":
          String password = ClientProperty.AUTH_PASSWORD.getValue(properties);
          Objects.nonNull(password);
          this.token = new PasswordToken(password);
          this.properties.remove(ClientProperty.AUTH_PASSWORD);
          break;
        case "kerberos":
          String principal = ClientProperty.AUTH_USERNAME.getValue(properties);
          String keytabPath = ClientProperty.AUTH_KERBEROS_KEYTAB_PATH.getValue(properties);
          Objects.nonNull(principal);
          Objects.nonNull(keytabPath);
          try {
            this.token = new KerberosToken(principal, new File(keytabPath));
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
          break;
        case "provider":
          String name = ClientProperty.AUTH_PROVIDER_NAME.getValue(properties);
          String providerUrls = ClientProperty.AUTH_PROVIDER_URLS.getValue(properties);
          try {
            this.token = new CredentialProviderToken(name, providerUrls);
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
          break;
        default:
          throw new IllegalArgumentException(
              "An authentication method (password, kerberos, etc) must be set");
      }
      return this;
    }

    @Override
    public ConnectionOptions usingPassword(String username, CharSequence password) {
      setProperty(ClientProperty.AUTH_METHOD, "password");
      setProperty(ClientProperty.AUTH_USERNAME, username);
      this.token = new PasswordToken(password);
      return this;
    }

    @Override
    public ConnectionOptions usingKerberos(String principal, String keyTabFile) {
      setProperty(ClientProperty.AUTH_METHOD, "kerberos");
      setProperty(ClientProperty.AUTH_USERNAME, principal);
      setProperty(ClientProperty.AUTH_KERBEROS_KEYTAB_PATH, keyTabFile);
      try {
        this.token = new KerberosToken(principal, new File(keyTabFile));
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      return this;
    }

    @Override
    public ConnectionOptions usingProvider(String username, String name, String providerUrls) {
      setProperty(ClientProperty.AUTH_METHOD, "provider");
      setProperty(ClientProperty.AUTH_USERNAME, username);
      setProperty(ClientProperty.AUTH_PROVIDER_NAME, name);
      setProperty(ClientProperty.AUTH_PROVIDER_URLS, providerUrls);
      try {
        this.token = new CredentialProviderToken(name, providerUrls);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      return this;
    }

    @Override
    public ConnectionOptions usingToken(String principal, AuthenticationToken token) {
      this.token = token;
      setAuthenticationProperties(this.properties, principal, token);
      return this;
    }

    @Override
    public FromOptions usingConnectionInfo(ConnectionInfo connectionInfo) {
      this.properties = connectionInfo.getProperties();
      this.token = connectionInfo.getAuthenticationToken();
      return this;
    }

    public void setProperty(ClientProperty property, String value) {
      properties.setProperty(property.getKey(), value);
    }

    public void setProperty(ClientProperty property, Long value) {
      setProperty(property, Long.toString(value));
    }

    public void setProperty(ClientProperty property, Integer value) {
      setProperty(property, Integer.toString(value));
    }
  }
}
