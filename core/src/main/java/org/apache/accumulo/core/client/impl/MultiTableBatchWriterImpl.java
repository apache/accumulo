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
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class MultiTableBatchWriterImpl implements MultiTableBatchWriter {
  public static final long DEFAULT_CACHE_TIME = 200;
  public static final TimeUnit DEFAULT_CACHE_TIME_UNIT = TimeUnit.MILLISECONDS;

  static final Logger log = Logger.getLogger(MultiTableBatchWriterImpl.class);
  private AtomicBoolean closed;
  private AtomicLong cacheLastState;

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

  /**
   * CacheLoader which will look up the internal table ID for a given table name.
   */
  private class TableNameToIdLoader extends CacheLoader<String,String> {

    @Override
    public String load(String tableName) throws Exception {
      String tableId = Tables.getNameToIdMap(instance).get(tableName);

      if (tableId == null)
        throw new TableNotFoundException(null, tableName, null);

      if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
        throw new TableOfflineException(instance, tableId);

      return tableId;
    }

  }

  private TabletServerBatchWriter bw;
  private ConcurrentHashMap<String,BatchWriter> tableWriters;
  private Instance instance;
  private final LoadingCache<String,String> nameToIdCache;

  public MultiTableBatchWriterImpl(Instance instance, Credentials credentials, BatchWriterConfig config) {
    this(instance, credentials, config, DEFAULT_CACHE_TIME, DEFAULT_CACHE_TIME_UNIT);
  }

  public MultiTableBatchWriterImpl(Instance instance, Credentials credentials, BatchWriterConfig config, long cacheTime, TimeUnit cacheTimeUnit) {
    ArgumentChecker.notNull(instance, credentials, config, cacheTimeUnit);
    this.instance = instance;
    this.bw = new TabletServerBatchWriter(instance, credentials, config);
    tableWriters = new ConcurrentHashMap<String,BatchWriter>();
    this.closed = new AtomicBoolean(false);
    this.cacheLastState = new AtomicLong(0);

    // Potentially up to ~500k used to cache names to IDs with "segments" of (maybe) ~1000 entries
    nameToIdCache = CacheBuilder.newBuilder().expireAfterWrite(cacheTime, cacheTimeUnit).concurrencyLevel(10).maximumSize(10000).initialCapacity(20)
        .build(new TableNameToIdLoader());
  }

  public boolean isClosed() {
    return this.closed.get();
  }

  public void close() throws MutationsRejectedException {
    this.closed.set(true);
    bw.close();
  }

  /**
   * Warning: do not rely upon finalize to close this class. Finalize is not guaranteed to be called.
   */
  @Override
  protected void finalize() {
    if (!closed.get()) {
      log.warn(MultiTableBatchWriterImpl.class.getSimpleName() + " not shutdown; did you forget to call close()?");
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
  private String getId(String tableName) throws TableNotFoundException {
    try {
      return nameToIdCache.get(tableName);
    } catch (UncheckedExecutionException e) {
      Throwable cause = e.getCause();

      log.error("Unexpected exception when fetching table id for " + tableName);

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

      log.error("Unexpected exception when fetching table id for " + tableName);

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
    ArgumentChecker.notNull(tableName);

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

    String tableId = getId(tableName);

    synchronized (tableWriters) {
      BatchWriter tbw = tableWriters.get(tableId);
      if (tbw == null) {
        tbw = new TableBatchWriter(tableId);
        tableWriters.put(tableId, tbw);
      }
      return tbw;
    }
  }

  @Override
  public void flush() throws MutationsRejectedException {
    bw.flush();
  }

}
