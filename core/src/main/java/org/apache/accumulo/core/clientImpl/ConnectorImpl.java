/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

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
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.trace.TraceUtil;

/**
 * This class now delegates to {@link ClientContext}, except for the methods which were not copied
 * over to that.
 */
@Deprecated
public class ConnectorImpl extends org.apache.accumulo.core.client.Connector {

  private static final String SYSTEM_TOKEN_NAME =
      "org.apache.accumulo.server.security.SystemCredentials$SystemToken";
  private final ClientContext context;

  public ConnectorImpl(ClientContext context) throws AccumuloSecurityException, AccumuloException {
    this.context = context;
    SingletonManager.setMode(Mode.CONNECTOR);
    if (context.getCredentials().getToken().isDestroyed())
      throw new AccumuloSecurityException(context.getCredentials().getPrincipal(),
          SecurityErrorCode.TOKEN_EXPIRED);
    // Skip fail fast for system services; string literal for class name, to avoid dependency on
    // server jar
    final String tokenClassName = context.getCredentials().getToken().getClass().getName();
    if (!SYSTEM_TOKEN_NAME.equals(tokenClassName)) {
      ServerClient.executeVoid(context, iface -> {
        if (!iface.authenticate(TraceUtil.traceInfo(), context.rpcCreds()))
          throw new AccumuloSecurityException("Authentication failed, access denied",
              SecurityErrorCode.BAD_CREDENTIALS);
      });
    }
  }

  public ClientContext getAccumuloClient() {
    return context;
  }

  @Override
  @Deprecated
  public org.apache.accumulo.core.client.Instance getInstance() {
    return context.getDeprecatedInstance();
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    return context.createBatchScanner(tableName, authorizations, numQueryThreads);
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, long maxMemory, long maxLatency, int maxWriteThreads)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchDeleter(context, context.getTableId(tableName), authorizations,
        numQueryThreads, new BatchWriterConfig().setMaxMemory(maxMemory)
            .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, BatchWriterConfig config) throws TableNotFoundException {
    return context.createBatchDeleter(tableName, authorizations, numQueryThreads, config);
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    return new BatchWriterImpl(context, context.getTableId(tableName),
        new BatchWriterConfig().setMaxMemory(maxMemory)
            .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config)
      throws TableNotFoundException {
    return context.createBatchWriter(tableName, config);
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency,
      int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(context, new BatchWriterConfig().setMaxMemory(maxMemory)
        .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    return context.createMultiTableBatchWriter(config);
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config)
      throws TableNotFoundException {
    return context.createConditionalWriter(tableName, config);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException {
    return context.createScanner(tableName, authorizations);
  }

  @Override
  public String whoami() {
    return context.whoami();
  }

  @Override
  public TableOperations tableOperations() {
    return context.tableOperations();
  }

  @Override
  public NamespaceOperations namespaceOperations() {
    return context.namespaceOperations();
  }

  @Override
  public SecurityOperations securityOperations() {
    return context.securityOperations();
  }

  @Override
  public InstanceOperations instanceOperations() {
    return context.instanceOperations();
  }

  @Override
  public ReplicationOperations replicationOperations() {
    return context.replicationOperations();
  }

}
