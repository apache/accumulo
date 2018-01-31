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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Tracer;

public class ConnectorImpl extends Connector {
  private static final String SYSTEM_TOKEN_NAME = "org.apache.accumulo.server.security.SystemCredentials$SystemToken";
  private final ClientContext context;
  private SecurityOperations secops = null;
  private TableOperationsImpl tableops = null;
  private NamespaceOperations namespaceops = null;
  private InstanceOperations instanceops = null;
  private ReplicationOperations replicationops = null;

  public ConnectorImpl(final ClientContext context) throws AccumuloSecurityException, AccumuloException {
    checkArgument(context != null, "Context is null");
    checkArgument(context.getCredentials() != null, "Credentials are null");
    checkArgument(context.getCredentials().getToken() != null, "Authentication token is null");
    if (context.getCredentials().getToken().isDestroyed())
      throw new AccumuloSecurityException(context.getCredentials().getPrincipal(), SecurityErrorCode.TOKEN_EXPIRED);

    this.context = context;

    // Skip fail fast for system services; string literal for class name, to avoid dependency on server jar
    final String tokenClassName = context.getCredentials().getToken().getClass().getName();
    if (!SYSTEM_TOKEN_NAME.equals(tokenClassName)) {
      ServerClient.executeVoid(context, new ClientExec<ClientService.Client>() {
        @Override
        public void execute(ClientService.Client iface) throws Exception {
          if (!iface.authenticate(Tracer.traceInfo(), context.rpcCreds()))
            throw new AccumuloSecurityException("Authentication failed, access denied", SecurityErrorCode.BAD_CREDENTIALS);
        }
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
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchReader(context, getTableId(tableName), authorizations, numQueryThreads);
  }

  @Deprecated
  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchDeleter(context, getTableId(tableName), authorizations, numQueryThreads, new BatchWriterConfig().setMaxMemory(maxMemory)
        .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchDeleter(context, getTableId(tableName), authorizations, numQueryThreads, config.merge(context.getBatchWriterConfig()));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
    return createBatchDeleter(tableName, authorizations, numQueryThreads, new BatchWriterConfig());
  }

  @Deprecated
  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    return new BatchWriterImpl(context, getTableId(tableName), new BatchWriterConfig().setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
        .setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    // we used to allow null inputs for bw config
    if (config == null) {
      config = new BatchWriterConfig();
    }
    return new BatchWriterImpl(context, getTableId(tableName), config.merge(context.getBatchWriterConfig()));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName) throws TableNotFoundException {
    return createBatchWriter(tableName, new BatchWriterConfig());
  }

  @Deprecated
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(context, new BatchWriterConfig().setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
        .setMaxWriteThreads(maxWriteThreads));
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
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) throws TableNotFoundException {
    return new ConditionalWriterImpl(context, getTableId(tableName), config);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
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

  public static class ConnectorBuilderImpl implements InstanceArgs, PropertyOptions, ConnectionInfoOptions, AuthenticationArgs, ConnectionOptions, SslOptions,
      SaslOptions, ConnectorFactory {

    private ConnectionInfoImpl info = new ConnectionInfoImpl();

    @Override
    public Connector build() throws AccumuloException, AccumuloSecurityException {
      return info.getConnector();
    }

    @Override
    public ConnectionInfo info() {
      return info;
    }

    @Override
    public AuthenticationArgs forInstance(String instanceName, String zookeepers) {
      info.setProperty(ClientProperty.INSTANCE_NAME, instanceName);
      info.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS, zookeepers);
      return this;
    }

    @Override
    public SslOptions withTruststore(String path) {
      info.setProperty(ClientProperty.SSL_TRUSTSTORE_PATH, path);
      return this;
    }

    @Override
    public SslOptions withTruststore(String path, String password, String type) {
      info.setProperty(ClientProperty.SSL_TRUSTSTORE_PATH, path);
      info.setProperty(ClientProperty.SSL_TRUSTSTORE_PASSWORD, password);
      info.setProperty(ClientProperty.SSL_TRUSTSTORE_TYPE, type);
      return this;
    }

    @Override
    public SslOptions withKeystore(String path) {
      info.setProperty(ClientProperty.SSL_KEYSTORE_PATH, path);
      return this;
    }

    @Override
    public SslOptions withKeystore(String path, String password, String type) {
      info.setProperty(ClientProperty.SSL_KEYSTORE_PATH, path);
      info.setProperty(ClientProperty.SSL_KEYSTORE_PASSWORD, password);
      info.setProperty(ClientProperty.SSL_KEYSTORE_TYPE, type);
      return this;
    }

    @Override
    public SslOptions useJsse() {
      info.setProperty(ClientProperty.SSL_USE_JSSE, "true");
      return this;
    }

    @Override
    public ConnectionOptions withZkTimeout(int timeout) {
      info.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT_SEC, Integer.toString(timeout));
      return this;
    }

    @Override
    public SslOptions withSsl() {
      info.setProperty(ClientProperty.SSL_ENABLED, "true");
      return this;
    }

    @Override
    public SaslOptions withSasl() {
      info.setProperty(ClientProperty.SASL_ENABLED, "true");
      return this;
    }

    @Override
    public ConnectionOptions withBatchWriterConfig(BatchWriterConfig batchWriterConfig) {
      info.setProperty(ClientProperty.BATCH_WRITER_MAX_MEMORY_BYTES, batchWriterConfig.getMaxMemory());
      info.setProperty(ClientProperty.BATCH_WRITER_MAX_LATENCY_SEC, batchWriterConfig.getMaxLatency(TimeUnit.SECONDS));
      info.setProperty(ClientProperty.BATCH_WRITER_MAX_TIMEOUT_SEC, batchWriterConfig.getTimeout(TimeUnit.SECONDS));
      info.setProperty(ClientProperty.BATCH_WRITER_MAX_WRITE_THREADS, batchWriterConfig.getMaxWriteThreads());
      info.setProperty(ClientProperty.BATCH_WRITER_DURABILITY, batchWriterConfig.getDurability().toString());
      return this;
    }

    @Override
    public SaslOptions withPrimary(String kerberosServerPrimary) {
      info.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY, kerberosServerPrimary);
      return this;
    }

    @Override
    public SaslOptions withQop(String qualityOfProtection) {
      info.setProperty(ClientProperty.SASL_QOP, qualityOfProtection);
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
      info = new ConnectionInfoImpl(properties);
      return this;
    }

    @Override
    public ConnectionOptions usingBasicCredentials(String username, CharSequence password) {
      info.setProperty(ClientProperty.AUTH_TYPE, "basic");
      info.setProperty(ClientProperty.AUTH_BASIC_USERNAME, username);
      info.setProperty(ClientProperty.AUTH_BASIC_PASSWORD, password.toString());
      return this;
    }

    @Override
    public ConnectionOptions usingKerberosCredentials(String principal, String keyTabFile) {
      info.setProperty(ClientProperty.AUTH_TYPE, "kerberos");
      info.setProperty(ClientProperty.AUTH_KERBEROS_PRINCIPAL, principal);
      info.setProperty(ClientProperty.AUTH_KERBEROS_KEYTAB_PATH, keyTabFile);
      return this;
    }

    @Override
    public ConnectionOptions usingCredentials(String principal, AuthenticationToken token) {
      if (token instanceof PasswordToken) {
        return usingBasicCredentials(principal, new String(((PasswordToken) token).getPassword()));
      } else if (token instanceof KerberosToken) {
        return usingKerberosCredentials(principal, ((KerberosToken) token).getKeytab().getAbsolutePath());
      } else {
        throw new IllegalArgumentException("Unknown authentication token type");
      }
    }

    @Override
    public ConnectorFactory usingConnectionInfo(ConnectionInfo connectionInfo) {
      this.info = (ConnectionInfoImpl) connectionInfo;
      return this;
    }
  }
}
