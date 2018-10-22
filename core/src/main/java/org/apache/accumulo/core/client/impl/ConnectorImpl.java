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

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;

/**
 * This class now delegates to {@link AccumuloClientImpl}, except for the methods which were not
 * copied over to that.
 */
@Deprecated
public class ConnectorImpl extends Connector {

  private final AccumuloClientImpl impl;

  public ConnectorImpl(AccumuloClientImpl impl) {
    this.impl = impl;
    SingletonManager.setMode(Mode.CONNECTOR);
  }

  public AccumuloClientImpl getAccumuloClient() {
    return impl;
  }

  @Override
  @Deprecated
  public org.apache.accumulo.core.client.Instance getInstance() {
    return impl.context.getDeprecatedInstance();
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException {
    return impl.createBatchScanner(tableName, authorizations, numQueryThreads);
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, long maxMemory, long maxLatency, int maxWriteThreads)
      throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(authorizations != null, "authorizations is null");
    return new TabletServerBatchDeleter(impl.context, impl.getTableId(tableName), authorizations,
        numQueryThreads, new BatchWriterConfig().setMaxMemory(maxMemory)
            .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, BatchWriterConfig config) throws TableNotFoundException {
    return impl.createBatchDeleter(tableName, authorizations, numQueryThreads, config);
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    return new BatchWriterImpl(impl.context, impl.getTableId(tableName),
        new BatchWriterConfig().setMaxMemory(maxMemory)
            .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config)
      throws TableNotFoundException {
    return impl.createBatchWriter(tableName, config);
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency,
      int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(impl.context,
        new BatchWriterConfig().setMaxMemory(maxMemory)
            .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    return impl.createMultiTableBatchWriter(config);
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config)
      throws TableNotFoundException {
    return impl.createConditionalWriter(tableName, config);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException {
    return impl.createScanner(tableName, authorizations);
  }

  @Override
  public String whoami() {
    return impl.whoami();
  }

  @Override
  public TableOperations tableOperations() {
    return impl.tableOperations();
  }

  @Override
  public NamespaceOperations namespaceOperations() {
    return impl.namespaceOperations();
  }

  @Override
  public SecurityOperations securityOperations() {
    return impl.securityOperations();
  }

  @Override
  public InstanceOperations instanceOperations() {
    return impl.instanceOperations();
  }

  @Override
  public ReplicationOperations replicationOperations() {
    return impl.replicationOperations();
  }

}
