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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
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
  private Table.ID tableId;

  private int size;

  private Range range;
  private boolean isolated = false;
  private long readaheadThreshold = Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD;

  boolean closed = false;

  private Set<ScannerIterator> iters = Collections.synchronizedSet(new HashSet<>());

  private synchronized void ensureOpen() {
    if (closed)
      throw new IllegalArgumentException("Scanner is closed");
  }

  public ScannerImpl(ClientContext context, Table.ID tableId, Authorizations authorizations) {
    checkArgument(context != null, "context is null");
    checkArgument(tableId != null, "tableId is null");
    checkArgument(authorizations != null, "authorizations is null");
    this.context = context;
    this.tableId = tableId;
    this.range = new Range((Key) null, (Key) null);
    this.authorizations = authorizations;

    this.size = Constants.SCAN_BATCH_SIZE;
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
    if (size > 0)
      this.size = size;
    else
      throw new IllegalArgumentException("size must be greater than zero");
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
        getTimeout(TimeUnit.SECONDS), this, isolated, readaheadThreshold, iters);

    iters.add(iter);

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
      iters.forEach(ScannerIterator::close);
      iters.clear();
    }

    closed = true;
  }
}
