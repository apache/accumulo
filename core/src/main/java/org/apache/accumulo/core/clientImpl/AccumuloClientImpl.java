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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
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

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("This client was closed.");
    }
  }

  public AccumuloClientImpl(SingletonReservation reservation, final ClientContext context) {
    checkArgument(context != null, "Context is null");
    checkArgument(context.getCredentials() != null, "Credentials are null");
    checkArgument(context.getCredentials().getToken() != null, "Authentication token is null");

    this.singletonReservation = Objects.requireNonNull(reservation);
    this.context = context;
    instanceID = context.getInstanceID();
    this.tableops = new TableOperationsImpl(context);
    this.namespaceops = new NamespaceOperationsImpl(context, tableops);
  }

  Table.ID getTableId(String tableName) throws TableNotFoundException {
    Table.ID tableId = Tables.getTableId(context, tableName);
    if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
      throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));
    return tableId;
  }


  void authenticate() throws AccumuloSecurityException, AccumuloException {
    if (context.getCredentials().getToken().isDestroyed())
      throw new AccumuloSecurityException(context.getCredentials().getPrincipal(),
          SecurityErrorCode.TOKEN_EXPIRED);
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
  public BatchScanner createBatchScanner(String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Authorizations auths = securityOperations().getUserAuthorizations(context.getPrincipal());
    return createBatchScanner(tableName, auths);
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
  public Scanner createScanner(String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Authorizations auths = securityOperations().getUserAuthorizations(context.getPrincipal());
    return createScanner(tableName, auths);
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
  public Properties properties() {
    ensureOpen();
    Properties result = new Properties();
    this.context.getProperties().forEach((key, value) -> {
      if (!key.equals(ClientProperty.AUTH_TOKEN.getKey())) {
        result.setProperty((String) key, (String) value);
      }
    });
    return result;
  }

  public AuthenticationToken token() {
    ensureOpen();
    return this.context.getAuthenticationToken();
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

  public static class ClientBuilderImpl<T>
      implements InstanceArgs<T>, PropertyOptions<T>, AuthenticationArgs<T>, ConnectionOptions<T>,
      SslOptions<T>, SaslOptions<T>, ClientFactory<T>, FromOptions<T> {

    private Properties properties = new Properties();
    private AuthenticationToken token = null;
    private Function<ClientBuilderImpl, T> builderFunction;

    public ClientBuilderImpl(Function<ClientBuilderImpl, T> builderFunction) {
      this.builderFunction = builderFunction;
    }

    private ClientInfo getClientInfo() {
      if (token != null) {
        return new ClientInfoImpl(properties, token);
      }
      return new ClientInfoImpl(properties);
    }

    @Override
    public T build() {
      return builderFunction.apply(this);
    }

    public static AccumuloClient buildClient(ClientBuilderImpl<AccumuloClient> cbi) {
      SingletonReservation reservation = SingletonManager.getClientReservation();
      try {
        // AccumuloClientImpl closes reservation unless a RuntimeException is thrown
        return new AccumuloClientImpl(reservation, new ClientContext(cbi.getClientInfo()));
      } catch (RuntimeException e) {
        reservation.close();
        throw e;
      }
    }

    public static Properties buildProps(ClientBuilderImpl<Properties> cbi) {
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
          batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS));
      ClientProperty.BATCH_WRITER_TIMEOUT_MAX.setTimeInMillis(properties,
          batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS));
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
    public FromOptions<T> from(Properties properties) {
      this.properties = properties;
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
      this.token = token;
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
  }
}
