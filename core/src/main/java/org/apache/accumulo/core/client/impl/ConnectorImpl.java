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

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.trace.instrument.Tracer;

public class ConnectorImpl extends Connector {
  private final Instance instance;
  private final Credentials credentials;
  private SecurityOperations secops = null;
  private TableOperations tableops = null;
  private NamespaceOperations namespaceops = null;
  private InstanceOperations instanceops = null;

  public ConnectorImpl(final Instance instance, Credentials cred) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(instance, cred);
    if (cred.getToken().isDestroyed())
      throw new AccumuloSecurityException(cred.getPrincipal(), SecurityErrorCode.TOKEN_EXPIRED);

    this.instance = instance;

    this.credentials = cred;

    // Skip fail fast for system services; string literal for class name, to avoid
    if (!"org.apache.accumulo.server.security.SystemCredentials$SystemToken".equals(cred.getToken().getClass().getName())) {
      ServerClient.execute(instance, new ClientExec<ClientService.Client>() {
        @Override
        public void execute(ClientService.Client iface) throws Exception {
          if (!iface.authenticate(Tracer.traceInfo(), credentials.toThrift(instance)))
            throw new AccumuloSecurityException("Authentication failed, access denied", SecurityErrorCode.BAD_CREDENTIALS);
        }
      });
    }

    this.tableops = new TableOperationsImpl(instance, credentials);
    this.namespaceops = new NamespaceOperationsImpl(instance, credentials, (TableOperationsImpl) tableops);
  }

  private String getTableId(String tableName) throws TableNotFoundException {
    String tableId = Tables.getTableId(instance, tableName);
    if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
      throw new TableOfflineException(instance, tableId);
    return tableId;
  }

  @Override
  public Instance getInstance() {
    return instance;
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new TabletServerBatchReader(instance, credentials, getTableId(tableName), authorizations, numQueryThreads);
  }

  @Deprecated
  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new TabletServerBatchDeleter(instance, credentials, getTableId(tableName), authorizations, numQueryThreads, new BatchWriterConfig()
        .setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
      throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new TabletServerBatchDeleter(instance, credentials, getTableId(tableName), authorizations, numQueryThreads, config);
  }

  @Deprecated
  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    return new BatchWriterImpl(instance, credentials, getTableId(tableName), new BatchWriterConfig().setMaxMemory(maxMemory)
        .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    return new BatchWriterImpl(instance, credentials, getTableId(tableName), config);
  }

  @Deprecated
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(instance, credentials, new BatchWriterConfig().setMaxMemory(maxMemory)
        .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    return new MultiTableBatchWriterImpl(instance, credentials, config);
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) throws TableNotFoundException {
    return new ConditionalWriterImpl(instance, credentials, getTableId(tableName), config);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new ScannerImpl(instance, credentials, getTableId(tableName), authorizations);
  }

  @Override
  public String whoami() {
    return credentials.getPrincipal();
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
      secops = new SecurityOperationsImpl(instance, credentials);

    return secops;
  }

  @Override
  public synchronized InstanceOperations instanceOperations() {
    if (instanceops == null)
      instanceops = new InstanceOperationsImpl(instance, credentials);

    return instanceops;
  }
}
