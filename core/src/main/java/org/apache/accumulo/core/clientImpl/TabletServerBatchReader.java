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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletServerBatchReader extends ScannerOptions implements BatchScanner {
  private static final Logger log = LoggerFactory.getLogger(TabletServerBatchReader.class);
  private static final AtomicInteger nextBatchReaderInstance = new AtomicInteger(1);

  private final int batchReaderInstance = nextBatchReaderInstance.getAndIncrement();
  private final TableId tableId;
  private final String tableName;
  private final int numThreads;
  private final ThreadPoolExecutor queryThreadPool;
  private final ClientContext context;
  private final Authorizations authorizations;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Cleanable cleanable;

  private ArrayList<Range> ranges = null;

  public TabletServerBatchReader(ClientContext context, TableId tableId, String tableName,
      Authorizations authorizations, int numQueryThreads) {
    this(context, BatchScanner.class, tableId, tableName, authorizations, numQueryThreads);
  }

  protected TabletServerBatchReader(ClientContext context, Class<?> scopeClass, TableId tableId,
      String tableName, Authorizations authorizations, int numQueryThreads) {
    checkArgument(context != null, "context is null");
    checkArgument(tableId != null, "tableId is null");
    checkArgument(authorizations != null, "authorizations is null");
    this.context = context;
    this.authorizations = authorizations;
    this.tableId = tableId;
    this.tableName = tableName;
    this.numThreads = numQueryThreads;

    queryThreadPool =
        context.threadPools().getPoolBuilder("batch scanner " + batchReaderInstance + "-")
            .numCoreThreads(numQueryThreads).build();
    // Call shutdown on this thread pool in case the caller does not call close().
    cleanable = CleanerUtil.shutdownThreadPoolExecutor(queryThreadPool, closed, log);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // Shutdown the pool
      queryThreadPool.shutdownNow();
      // deregister the cleaner, will not call shutdownNow() because closed is now true
      cleanable.clean();
    }
  }

  @Override
  public Authorizations getAuthorizations() {
    return authorizations;
  }

  @Override
  public void setRanges(Collection<Range> ranges) {
    if (ranges == null || ranges.isEmpty()) {
      throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
    }

    if (closed.get()) {
      throw new IllegalStateException("batch reader closed");
    }

    this.ranges = new ArrayList<>(ranges);
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    if (ranges == null) {
      throw new IllegalStateException("ranges not set");
    }

    if (closed.get()) {
      throw new IllegalStateException("batch reader closed");
    }

    return new TabletServerBatchReaderIterator(context, tableId, tableName, authorizations, ranges,
        numThreads, queryThreadPool, this, retryTimeout);
  }
}
