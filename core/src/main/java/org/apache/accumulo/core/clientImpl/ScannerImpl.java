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
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

/**
 * provides scanner functionality
 *
 * "Clients can iterate over multiple column families, and there are several mechanisms for limiting
 * the rows, columns, and timestamps traversed by a scan. For example, we could restrict [a] scan
 * ... to only produce anchors whose columns match [a] regular expression ..., or to only produce
 * anchors whose timestamps fall within ten days of the current time."
 *
 */
public class ScannerImpl extends ScannerOptions implements Scanner {

  // keep a list of columns over which to scan
  // keep track of the last thing read
  // hopefully, we can track all the state in the scanner on the client
  // and just query for the next highest row from the tablet server

  private final ClientContext context;
  private Authorizations authorizations;
  private TableId tableId;

  private int size;

  private Range range;
  private boolean isolated = false;
  private long readaheadThreshold = Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD;

  boolean closed = false;

  private static final int MAX_ENTRIES = 16;

  private long iterCount = 0;

  // Create an LRU map of iterators that tracks the MAX_ENTRIES most recently used iterators. An LRU
  // map is used to support the use case of a long lived scanner that constantly creates iterators
  // and does not read all of the data. For this case do not want iterator tracking to consume too
  // much memory. Also it would be best to avoid an RPC storm of close methods for thousands
  // sessions that may have timed out.
  private Map<ScannerIterator,Long> iters = new LinkedHashMap<>(MAX_ENTRIES + 1, .75F, true) {
    private static final long serialVersionUID = 1L;

    // This method is called just after a new entry has been added
    @Override
    public boolean removeEldestEntry(Map.Entry<ScannerIterator,Long> eldest) {
      return size() > MAX_ENTRIES;
    }
  };

  /**
   * This is used for ScannerIterators to report their activity back to the scanner that created
   * them.
   */
  class Reporter {

    void readBatch(ScannerIterator iter) {
      synchronized (ScannerImpl.this) {
        // This iter just had some activity, so access it in map so it becomes the most recently
        // used.
        iters.get(iter);
      }
    }

    void finished(ScannerIterator iter) {
      synchronized (ScannerImpl.this) {
        iters.remove(iter);
      }
    }
  }

  private synchronized void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Scanner is closed");
    }
  }

  public ScannerImpl(ClientContext context, TableId tableId, Authorizations authorizations) {
    checkArgument(context != null, "context is null");
    checkArgument(tableId != null, "tableId is null");
    checkArgument(authorizations != null, "authorizations is null");
    this.context = context;
    this.tableId = tableId;
    this.range = new Range((Key) null, (Key) null);
    this.authorizations = authorizations;

    this.size = Constants.SCAN_BATCH_SIZE;
  }

  public ClientContext getClientContext() {
    ensureOpen();
    return context;
  }

  public TableId getTableId() {
    ensureOpen();
    return tableId;
  }

  @Override
  public synchronized void setRange(Range range) {
    ensureOpen();
    checkArgument(range != null, "range is null");
    this.range = range;
  }

  @Override
  public synchronized Range getRange() {
    ensureOpen();
    return range;
  }

  @Override
  public synchronized void setBatchSize(int size) {
    ensureOpen();
    if (size > 0) {
      this.size = size;
    } else {
      throw new IllegalArgumentException("size must be greater than zero");
    }
  }

  @Override
  public synchronized int getBatchSize() {
    ensureOpen();
    return size;
  }

  @Override
  public synchronized Iterator<Entry<Key,Value>> iterator() {
    ensureOpen();
    ScannerIterator iter = new ScannerIterator(context, tableId, authorizations, range, size,
        getTimeout(SECONDS), this, isolated, readaheadThreshold, new Reporter());

    iters.put(iter, iterCount++);

    return iter;
  }

  @Override
  public Authorizations getAuthorizations() {
    ensureOpen();
    return authorizations;
  }

  @Override
  public synchronized void enableIsolation() {
    ensureOpen();
    this.isolated = true;
  }

  @Override
  public synchronized void disableIsolation() {
    ensureOpen();
    this.isolated = false;
  }

  @Override
  public synchronized void setReadaheadThreshold(long batches) {
    ensureOpen();
    if (batches < 0) {
      throw new IllegalArgumentException(
          "Number of batches before read-ahead must be non-negative");
    }

    readaheadThreshold = batches;
  }

  @Override
  public synchronized long getReadaheadThreshold() {
    ensureOpen();
    return readaheadThreshold;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      iters.forEach((iter, v) -> iter.close());
      iters.clear();
    }

    closed = true;
  }
}
