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

import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
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
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;

public class ConnectorImpl extends Connector {
  private Instance instance;
  private AuthInfo credentials;
  private SecurityOperations secops = null;
  private TableOperations tableops = null;
  private InstanceOperations instanceops = null;
  
/**
     * 
     * Use {@link Instance#getConnector(String, byte[])}
     * 
     * @param instance
     * @param user
     * @param password
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @see Instance#getConnector(String user, byte[] password)
     * @deprecated Not for client use
     */
  public ConnectorImpl(Instance instance, String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(instance, user, password);
    this.instance = instance;
    
    // copy password so that user can clear it.... in future versions we can clear it...
    byte[] passCopy = new byte[password.length];
    System.arraycopy(password, 0, passCopy, 0, password.length);
    this.credentials = new AuthInfo(user, ByteBuffer.wrap(password), instance.getInstanceID());
    
    // hardcoded string for SYSTEM user since the definition is
    // in server code
    if (!user.equals("!SYSTEM")) {
      ServerClient.execute(instance, new ClientExec<ClientService.Iface>() {
        @Override
        public void execute(ClientService.Iface iface) throws Exception {
          iface.authenticateUser(null, credentials, credentials.user, credentials.password);
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
    return new TabletServerBatchReader(instance, credentials, getTableId(tableName), authorizations, numQueryThreads);
  }
  
  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new TabletServerBatchDeleter(instance, credentials, getTableId(tableName), authorizations, numQueryThreads, maxMemory, maxLatency, maxWriteThreads);
  }
  
  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    return new BatchWriterImpl(instance, credentials, getTableId(tableName), maxMemory, maxLatency, maxWriteThreads);
  }
  
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return new MultiTableBatchWriterImpl(instance, credentials, maxMemory, maxLatency, maxWriteThreads);
  }
  
  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName, authorizations);
    return new ScannerImpl(instance, credentials, getTableId(tableName), authorizations);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see accumulo.core.client.Connector#whoami()
   */
  @Override
  public String whoami() {
    return credentials.user;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see accumulo.core.client.Connector#tableOperations()
   */
  @Override
  public synchronized TableOperations tableOperations() {
    if (tableops == null)
      tableops = new TableOperationsImpl(instance, credentials);
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
      secops = new SecurityOperationsImpl(instance, credentials);
    
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
      instanceops = new InstanceOperationsImpl(instance, credentials);
    
    return instanceops;
  }
  
}
