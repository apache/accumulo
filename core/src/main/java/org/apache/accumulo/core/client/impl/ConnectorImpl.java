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

import org.apache.accumulo.cloudtrace.instrument.Tracer;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.InstanceOperationsImpl;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.SecurityOperationsImpl;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TableOperationsImpl;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.tokens.AccumuloToken;
import org.apache.accumulo.core.security.tokens.InstanceTokenWrapper;
import org.apache.accumulo.core.util.ArgumentChecker;

public class ConnectorImpl extends Connector {
  private Instance instance;
  
  // There are places where we need information from the token. But we don't want to convert to a thrift object for every call.
  // So we'll keep both on hand and pass them around.
  private InstanceTokenWrapper token;
  private SecurityOperations secops = null;
  private TableOperations tableops = null;
  private InstanceOperations instanceops = null;
  
  /**
   * 
   * Use {@link Instance#getConnector(String, byte[])}
   * 
   * @param instance
   * @param token
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   * @see Instance#getConnector(String user, byte[] password)
   * @deprecated Not for client use
   */
  @Deprecated
  public ConnectorImpl(Instance instance, final AccumuloToken<?,?> token2) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(instance, token2);
    this.instance = instance;
    
    // copy password so that user can clear it.... in future versions we can clear it...
    
    this.token = new InstanceTokenWrapper(token2, instance.getInstanceID());
    
    // hardcoded string for SYSTEM user since the definition is
    // in server code
    if (!token.getPrincipal().equals("!SYSTEM")) {
      ServerClient.execute(instance, new ClientExec<ClientService.Client>() {
        @Override
        public void execute(ClientService.Client iface) throws Exception {
          iface.authenticateUser(Tracer.traceInfo(), token.toThrift(), token.toThrift().token);
        }
      });
    }
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
    return new TabletServerBatchReader(instance, token, getTableId(tableName), authorizations, numQueryThreads);
  }
  
  @Deprecated
  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new TabletServerBatchDeleter(instance, token, getTableId(tableName), authorizations, numQueryThreads, new BatchWriterConfig()
        .setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }
  
  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
      throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new TabletServerBatchDeleter(instance, token, getTableId(tableName), authorizations, numQueryThreads, config);
  }
  
  @Deprecated
  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    return new BatchWriterImpl(instance, token, getTableId(tableName), new BatchWriterConfig().setMaxMemory(maxMemory)
        .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }
  
  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    return new BatchWriterImpl(instance, token, getTableId(tableName), config);
  }
  
  @Deprecated
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(instance, token, new BatchWriterConfig().setMaxMemory(maxMemory)
        .setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }
  
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    return new MultiTableBatchWriterImpl(instance, token, config);
  }
  
  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new ScannerImpl(instance, token, getTableId(tableName), authorizations);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see accumulo.core.client.Connector#whoami()
   */
  @Override
  public String whoami() {
    return token.getPrincipal();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see accumulo.core.client.Connector#tableOperations()
   */
  @Override
  public synchronized TableOperations tableOperations() {
    if (tableops == null)
      tableops = new TableOperationsImpl(instance, token);
    return tableops;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see accumulo.core.client.Connector#securityOperations()
   */
  @Override
  public synchronized SecurityOperations securityOperations() {
    if (secops == null)
      secops = new SecurityOperationsImpl(instance, token);
    
    return secops;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see accumulo.core.client.Connector#instanceOperations()
   */
  @Override
  public synchronized InstanceOperations instanceOperations() {
    if (instanceops == null)
      instanceops = new InstanceOperationsImpl(instance, token);
    
    return instanceops;
  }
  
}
