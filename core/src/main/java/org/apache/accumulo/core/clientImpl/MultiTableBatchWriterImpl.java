/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

import java.lang.ref.Cleaner.Cleanable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTableBatchWriterImpl implements MultiTableBatchWriter {

  private static final Logger log = LoggerFactory.getLogger(MultiTableBatchWriterImpl.class);

  private class TableBatchWriter implements BatchWriter {

    private final TableId tableId;

    private TableBatchWriter(TableId tableId) {
      this.tableId = tableId;
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
      checkArgument(m != null, "m is null");
      bw.addMutation(tableId, m);
    }

    @Override
    public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
      bw.addMutation(tableId, iterable.iterator());
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException(
          "Must close all tables, can not close an individual table");
    }

    @Override
    public void flush() {
      throw new UnsupportedOperationException(
          "Must flush all tables, can not flush an individual table");
    }
  }

  private final ConcurrentHashMap<TableId,BatchWriter> tableWriters = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ClientContext context;
  private final TabletServerBatchWriter bw;
  private final Cleanable cleanable;

  MultiTableBatchWriterImpl(ClientContext context, BatchWriterConfig config) {
    checkArgument(context != null, "context is null");
    checkArgument(config != null, "config is null");
    this.context = context;
    this.bw = new TabletServerBatchWriter(context, config);
    this.cleanable = CleanerUtil.unclosed(this, MultiTableBatchWriter.class, closed, log, bw);
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() throws MutationsRejectedException {
    if (closed.compareAndSet(false, true)) {
      // deregister cleanable, but it won't run because it checks
      // the value of closed first, which is now true
      cleanable.clean();
      bw.close();
    }
  }

  @Override
  public BatchWriter getBatchWriter(String tableName) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");

    TableId tableId = context.getTableId(tableName);

    BatchWriter tbw = tableWriters.get(tableId);
    if (tbw == null) {
      tbw = new TableBatchWriter(tableId);
      BatchWriter current = tableWriters.putIfAbsent(tableId, tbw);
      // return the current one if another thread created one first
      return current != null ? current : tbw;
    } else {
      return tbw;
    }
  }

  @Override
  public void flush() throws MutationsRejectedException {
    bw.flush();
  }

}
