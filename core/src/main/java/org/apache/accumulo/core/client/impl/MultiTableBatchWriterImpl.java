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

import java.util.HashMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.log4j.Logger;

public class MultiTableBatchWriterImpl implements MultiTableBatchWriter {
  static final Logger log = Logger.getLogger(MultiTableBatchWriterImpl.class);
  private boolean closed;
  
  private class TableBatchWriter implements BatchWriter {
    
    private String table;
    
    TableBatchWriter(String table) {
      this.table = table;
    }
    
    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
      ArgumentChecker.notNull(m);
      bw.addMutation(table, m);
    }
    
    @Override
    public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
      bw.addMutation(table, iterable.iterator());
    }
    
    @Override
    public void close() {
      throw new UnsupportedOperationException("Must close all tables, can not close an individual table");
    }
    
    @Override
    public void flush() {
      throw new UnsupportedOperationException("Must flush all tables, can not flush an individual table");
    }
    
  }
  
  private TabletServerBatchWriter bw;
  private HashMap<String,BatchWriter> tableWriters;
  private Instance instance;
  
  public MultiTableBatchWriterImpl(Instance instance, AuthInfo credentials, long maxMemory, long maxLatency, int maxWriteThreads) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.bw = new TabletServerBatchWriter(instance, credentials, maxMemory, maxLatency, maxWriteThreads);
    tableWriters = new HashMap<String,BatchWriter>();
    this.closed = false;
  }
  
  public boolean isClosed() {
    return this.closed;
  }
  
  public void close() throws MutationsRejectedException {
    bw.close();
    this.closed = true;
  }
  
  /**
   * Warning: do not rely upon finalize to close this class. Finalize is not guaranteed to be called.
   */
  @Override
  protected void finalize() {
    if (!closed) {
      log.warn(MultiTableBatchWriterImpl.class.getSimpleName() + " not shutdown; did you forget to call close()?");
      try {
        close();
      } catch (MutationsRejectedException mre) {
        log.error(MultiTableBatchWriterImpl.class.getSimpleName() + " internal error.", mre);
        throw new RuntimeException("Exception when closing " + MultiTableBatchWriterImpl.class.getSimpleName(), mre);
      }
    }
  }
  
  @Override
  public synchronized BatchWriter getBatchWriter(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    String tableId = Tables.getNameToIdMap(instance).get(tableName);
    if (tableId == null)
      throw new TableNotFoundException(tableId, tableName, null);
    
    if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
      throw new TableOfflineException(instance, tableId);
    
    BatchWriter tbw = tableWriters.get(tableId);
    if (tbw == null) {
      tbw = new TableBatchWriter(tableId);
      tableWriters.put(tableId, tbw);
    }
    return tbw;
  }
  
  @Override
  public void flush() throws MutationsRejectedException {
    bw.flush();
  }
  
}
