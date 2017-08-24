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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class MultiTableBatchWriterImpl implements MultiTableBatchWriter {
  public static final long DEFAULT_CACHE_TIME = 200;
  public static final TimeUnit DEFAULT_CACHE_TIME_UNIT = TimeUnit.MILLISECONDS;

  private static final Logger log = LoggerFactory.getLogger(MultiTableBatchWriterImpl.class);
  private AtomicBoolean closed;
  private AtomicLong cacheLastState;

  private class TableBatchWriter implements BatchWriter {

    private Table.ID tableId;

    TableBatchWriter(Table.ID tableId) {
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
      throw new UnsupportedOperationException("Must close all tables, can not close an individual table");
    }

    @Override
    public void flush() {
      throw new UnsupportedOperationException("Must flush all tables, can not flush an individual table");
    }

  }

  /**
   * CacheLoader which will look up the internal table ID for a given table name.
   */
  private class TableNameToIdLoader extends CacheLoader<String,Table.ID> {

    @Override
    public Table.ID load(String tableName) throws Exception {
      Instance instance = context.getInstance();
      Table.ID tableId = Tables.getTableId(instance, tableName);

      if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
        throw new TableOfflineException(instance, tableId.canonicalID());

      return tableId;
    }

  }

  private TabletServerBatchWriter bw;
  private ConcurrentHashMap<Table.ID,BatchWriter> tableWriters;
  private final ClientContext context;
  private final LoadingCache<String,Table.ID> nameToIdCache;

  public MultiTableBatchWriterImpl(ClientContext context, BatchWriterConfig config) {
    this(context, config, DEFAULT_CACHE_TIME, DEFAULT_CACHE_TIME_UNIT);
  }

  public MultiTableBatchWriterImpl(ClientContext context, BatchWriterConfig config, long cacheTime, TimeUnit cacheTimeUnit) {
    checkArgument(context != null, "context is null");
    checkArgument(config != null, "config is null");
    checkArgument(cacheTimeUnit != null, "cacheTimeUnit is null");
    this.context = context;
    this.bw = new TabletServerBatchWriter(context, config);
    tableWriters = new ConcurrentHashMap<>();
    this.closed = new AtomicBoolean(false);
    this.cacheLastState = new AtomicLong(0);

    // Potentially up to ~500k used to cache names to IDs with "segments" of (maybe) ~1000 entries
    nameToIdCache = CacheBuilder.newBuilder().expireAfterWrite(cacheTime, cacheTimeUnit).concurrencyLevel(10).maximumSize(10000).initialCapacity(20)
        .build(new TableNameToIdLoader());
  }

  @Override
  public boolean isClosed() {
    return this.closed.get();
  }

  @Override
  public void close() throws MutationsRejectedException {
    this.closed.set(true);
    bw.close();
  }

  // WARNING: do not rely upon finalize to close this class. Finalize is not guaranteed to be called.
  @Override
  protected void finalize() {
    if (!closed.get()) {
      log.warn("{} not shutdown; did you forget to call close()?", MultiTableBatchWriterImpl.class.getSimpleName());
      try {
        close();
      } catch (MutationsRejectedException mre) {
        log.error(MultiTableBatchWriterImpl.class.getSimpleName() + " internal error.", mre);
        throw new RuntimeException("Exception when closing " + MultiTableBatchWriterImpl.class.getSimpleName(), mre);
      }
    }
  }

  /**
   * Returns the table ID for the given table name.
   *
   * @param tableName
   *          The name of the table which to find the ID for
   * @return The table ID, or null if the table name doesn't exist
   */
  private Table.ID getId(String tableName) throws TableNotFoundException {
    try {
      return nameToIdCache.get(tableName);
    } catch (UncheckedExecutionException e) {
      Throwable cause = e.getCause();

      log.error("Unexpected exception when fetching table id for {}", tableName);

      if (null == cause) {
        throw new RuntimeException(e);
      } else if (cause instanceof TableNotFoundException) {
        throw (TableNotFoundException) cause;
      } else if (cause instanceof TableOfflineException) {
        throw (TableOfflineException) cause;
      }

      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();

      log.error("Unexpected exception when fetching table id for {}", tableName);

      if (null == cause) {
        throw new RuntimeException(e);
      } else if (cause instanceof TableNotFoundException) {
        throw (TableNotFoundException) cause;
      } else if (cause instanceof TableOfflineException) {
        throw (TableOfflineException) cause;
      }

      throw new RuntimeException(e);
    }
  }

  @Override
  public BatchWriter getBatchWriter(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");

    while (true) {
      long cacheResetCount = Tables.getCacheResetCount();

      // cacheResetCount could change after this point in time, but I think thats ok because just want to ensure this methods sees changes
      // made before it was called.

      long internalResetCount = cacheLastState.get();

      if (cacheResetCount > internalResetCount) {
        if (!cacheLastState.compareAndSet(internalResetCount, cacheResetCount)) {
          continue; // concurrent operation, lets not possibly move cacheLastState backwards in the case where a thread pauses for along time
        }

        nameToIdCache.invalidateAll();
        break;
      }

      break;
    }

    Table.ID tableId = getId(tableName);

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
