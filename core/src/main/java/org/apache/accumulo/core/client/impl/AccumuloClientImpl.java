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

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
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
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.core.trace.Tracer;

public class AccumuloClientImpl implements AccumuloClient {
  private static final String SYSTEM_TOKEN_NAME = "org.apache.accumulo.server.security."
      + "SystemCredentials$SystemToken";
  final ClientContext context;
  private final String instanceID;
  private SecurityOperations secops = null;
  private TableOperationsImpl tableops = null;
  private NamespaceOperations namespaceops = null;
  private InstanceOperations instanceops = null;
  private ReplicationOperations replicationops = null;
  private final SingletonReservation singletonReservation;
  private volatile boolean closed = false;
  private final boolean autoClose;

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("This client was closed.");
    }
  }

  public AccumuloClientImpl(SingletonReservation reservation, final ClientContext context,
      boolean autoClose) throws AccumuloSecurityException, AccumuloException {
    checkArgument(context != null, "Context is null");
    checkArgument(context.getCredentials() != null, "Credentials are null");
    checkArgument(context.getCredentials().getToken() != null, "Authentication token is null");
    if (context.getCredentials().getToken().isDestroyed())
      throw new AccumuloSecurityException(context.getCredentials().getPrincipal(),
          SecurityErrorCode.TOKEN_EXPIRED);

    this.singletonReservation = Objects.requireNonNull(reservation);
    this.context = context;
    instanceID = context.getInstanceID();

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
    this.autoClose = autoClose;
  }

  Table.ID getTableId(String tableName) throws TableNotFoundException {
    Table.ID tableId = Tables.getTableId(context, tableName);
    if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
      throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));
    return tableId;
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    ensureOpen();
    return new TabletServerBatchReader(context, getTableId(tableName), authorizations,
        numQueryThreads);
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException {
    Integer numQueryThreads = ClientProperty.BATCH_SCANNER_NUM_QUERY_THREADS
        .getInteger(context.getClientInfo().getProperties());
    Objects.requireNonNull(numQueryThreads);
    ensureOpen();
    return createBatchScanner(tableName, authorizations, numQueryThreads);
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, BatchWriterConfig config) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    ensureOpen();
    return new TabletServerBatchDeleter(context, getTableId(tableName), authorizations,
        numQueryThreads, config.merge(context.getBatchWriterConfig()));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    ensureOpen();
    return createBatchDeleter(tableName, authorizations, numQueryThreads, new BatchWriterConfig());
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    ensureOpen();
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

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    ensureOpen();
    return new MultiTableBatchWriterImpl(context, config.merge(context.getBatchWriterConfig()));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter() {
    return createMultiTableBatchWriter(new BatchWriterConfig());
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config)
      throws TableNotFoundException {
    ensureOpen();
    return new ConditionalWriterImpl(context, getTableId(tableName), config);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    ensureOpen();
    Scanner scanner = new ScannerImpl(context, getTableId(tableName), authorizations);
    Integer batchSize = ClientProperty.SCANNER_BATCH_SIZE
        .getInteger(context.getClientInfo().getProperties());
    if (batchSize != null) {
      scanner.setBatchSize(batchSize);
    }
    return scanner;
  }

  @Override
  public String whoami() {
    ensureOpen();
    return context.getCredentials().getPrincipal();
  }

  @Override
  public String getInstanceID() {
    ensureOpen();
    return instanceID;
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
    if (secops == null)
      secops = new SecurityOperationsImpl(context);

    return secops;
  }

  @Override
  public synchronized InstanceOperations instanceOperations() {
    ensureOpen();
    if (instanceops == null)
      instanceops = new InstanceOperationsImpl(context);

    return instanceops;
  }

  @Override
  public synchronized ReplicationOperations replicationOperations() {
    ensureOpen();
    if (null == replicationops) {
      replicationops = new ReplicationOperationsImpl(context);
    }

    return replicationops;
  }

  @Override
  public ClientInfo info() {
    ensureOpen();
    return this.context.getClientInfo();
  }

  @Override
  public AccumuloClient changeUser(String principal, AuthenticationToken token)
      throws AccumuloSecurityException, AccumuloException {
    ensureOpen();
    return Accumulo.newClient().usingClientInfo(info()).usingToken(principal, token).build();
  }

  @Override
  public void close() {
    closed = true;
    try {
      context.close();
    } finally {
      singletonReservation.close();
    }
  }

  @Override
  public void finalize() throws Throwable {
    try {
      if (autoClose)
        close();
    } finally {
      super.finalize();
    }
  }

  public static class AccumuloClientBuilderImpl
      implements InstanceArgs, PropertyOptions, ClientInfoOptions, AuthenticationArgs,
      ConnectionOptions, SslOptions, SaslOptions, AccumuloClientFactory, FromOptions {

    private Properties properties = new Properties();
    private AuthenticationToken token = null;

    private ClientInfo getClientInfo() {
      if (token != null) {
        return new ClientInfoImpl(properties, token);
      }
      return new ClientInfoImpl(properties);
    }

    @Override
    public AccumuloClient build() throws AccumuloException, AccumuloSecurityException {
      SingletonReservation reservation = SingletonManager.getClientReservation();
      try {
        return new AccumuloClientImpl(reservation, new ClientContext(getClientInfo()), true);
      } catch (AccumuloException | AccumuloSecurityException | RuntimeException e) {
        reservation.close();
        throw e;
      }
    }

    @Override
    public ClientInfo info() {
      return getClientInfo();
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
      setProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, Integer.toString(timeout) + "ms");
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
    public ConnectionOptions withBatchScannerQueryThreads(int numQueryThreads) {
      setProperty(ClientProperty.BATCH_SCANNER_NUM_QUERY_THREADS, numQueryThreads);
      return this;
    }

    @Override
    public ConnectionOptions withScannerBatchSize(int batchSize) {
      setProperty(ClientProperty.SCANNER_BATCH_SIZE, batchSize);
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
    public AccumuloClientFactory usingProperties(String configFile) {
      return usingProperties(ClientInfoImpl.toProperties(configFile));
    }

    @Override
    public AccumuloClientFactory usingProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    @Override
    public ConnectionOptions usingPassword(String principal, CharSequence password) {
      setProperty(ClientProperty.AUTH_PRINCIPAL, principal);
      ClientProperty.setPassword(properties, password.toString());
      return this;
    }

    @Override
    public ConnectionOptions usingKerberos(String principal, String keyTabFile) {
      setProperty(ClientProperty.AUTH_PRINCIPAL, principal);
      ClientProperty.setKerberosKeytab(properties, keyTabFile);
      return this;
    }

    @Override
    public ConnectionOptions usingToken(String principal, AuthenticationToken token) {
      setProperty(ClientProperty.AUTH_PRINCIPAL, principal);
      this.token = token;
      return this;
    }

    @Override
    public FromOptions usingClientInfo(ClientInfo clientInfo) {
      this.properties = clientInfo.getProperties();
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
