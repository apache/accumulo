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
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
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

  public ConnectorImpl(final ClientContext context) throws AccumuloException, AccumuloSecurityException {
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
    return new TabletServerBatchDeleter(context, getTableId(tableName), authorizations, numQueryThreads, config);
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
    return new BatchWriterImpl(context, getTableId(tableName), config);
  }

  @Deprecated
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(context, new BatchWriterConfig().setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
        .setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    return new MultiTableBatchWriterImpl(context, config);
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
}
