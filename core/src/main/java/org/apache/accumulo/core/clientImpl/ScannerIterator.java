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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanState;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ScannerIterator implements Iterator<Entry<Key,Value>> {

  // scanner options
  private long timeOut;

  // scanner state
  private Iterator<KeyValue> iter;
  private final ScanState scanState;

  private ScannerOptions options;

  private Future<List<KeyValue>> readAheadOperation;

  private boolean finished = false;

  private long batchCount = 0;
  private long readaheadThreshold;

  private ScannerImpl.Reporter reporter;

  private final ClientContext context;

  private AtomicBoolean closed = new AtomicBoolean(false);

  ScannerIterator(ClientContext context, TableId tableId, Authorizations authorizations,
      Range range, int size, long timeOut, ScannerOptions options, boolean isolated,
      long readaheadThreshold, ScannerImpl.Reporter reporter) {
    this.context = context;
    this.timeOut = timeOut;
    this.readaheadThreshold = readaheadThreshold;

    this.options = new ScannerOptions(options);

    this.reporter = reporter;

    if (!this.options.fetchedColumns.isEmpty()) {
      range = range.bound(this.options.fetchedColumns.first(), this.options.fetchedColumns.last());
    }

    scanState = new ScanState(context, tableId, authorizations, new Range(range),
        options.fetchedColumns, size, options.serverSideIteratorList,
        options.serverSideIteratorOptions, isolated, readaheadThreshold,
        options.getSamplerConfiguration(), options.batchTimeOut, options.classLoaderContext,
        options.executionHints, options.getConsistencyLevel() == ConsistencyLevel.EVENTUAL);

    // If we want to start readahead immediately, don't wait for hasNext to be called
    if (readaheadThreshold == 0L) {
      initiateReadAhead();
    }
    iter = null;
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }

    if (iter != null && iter.hasNext()) {
      return true;
    }

    iter = getNextBatch().iterator();
    if (!iter.hasNext()) {
      finished = true;
      reporter.finished(this);
      return false;
    }

    return true;
  }

  @Override
  public Entry<Key,Value> next() {
    if (hasNext()) {
      return iter.next();
    }
    throw new NoSuchElementException();
  }

  void close() {
    // run actual close operation in the background so this does not block.
    context.executeCleanupTask(() -> {
      synchronized (scanState) {
        // this is synchronized so its mutually exclusive with readBatch()
        try {
          closed.set(true);
          ThriftScanner.close(scanState);
        } catch (Exception e) {
          LoggerFactory.getLogger(ScannerIterator.class)
              .debug("Exception when closing scan session", e);
        }
      }
    });
  }

  private void initiateReadAhead() {
    Preconditions.checkState(readAheadOperation == null);
    readAheadOperation = context.submitScannerReadAheadTask(this::readBatch);
  }

  private List<KeyValue> readBatch() throws Exception {

    List<KeyValue> batch;

    do {
      synchronized (scanState) {
        // this is synchronized so its mutually exclusive with closing
        Preconditions.checkState(!closed.get(), "Scanner was closed");
        batch = ThriftScanner.scan(scanState.context, scanState, timeOut);
      }
    } while (batch != null && batch.isEmpty());

    if (batch != null) {
      reporter.readBatch(this);
    }

    return batch == null ? Collections.emptyList() : batch;
  }

  private List<KeyValue> getNextBatch() {

    List<KeyValue> nextBatch;

    try {
      if (readAheadOperation == null) {
        // no read ahead run, fetch the next batch right now
        nextBatch = readBatch();
      } else {
        nextBatch = readAheadOperation.get();
        readAheadOperation = null;
      }
    } catch (ExecutionException ee) {
      wrapExecutionException(ee);
      throw new RuntimeException(ee);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (!nextBatch.isEmpty()) {
      batchCount++;

      if (batchCount > readaheadThreshold) {
        // start a thread to read the next batch
        initiateReadAhead();
      }
    }

    return nextBatch;
  }

  private void wrapExecutionException(ExecutionException ee) {
    // Need preserve the type of exception that was the cause because some code depends on it.
    // However the cause is an exception that occurred in a background thread, so throwing it would
    // lose the stack trace for the user thread calling the scanner. Wrapping the exception with the
    // same type preserves the type and stack traces (foreground and background thread traces) that
    // are critical for debugging.
    if (ee.getCause() instanceof IsolationException) {
      throw new IsolationException(ee);
    }
    if (ee.getCause() instanceof TableDeletedException) {
      TableDeletedException cause = (TableDeletedException) ee.getCause();
      throw new TableDeletedException(cause.getTableId(), cause);
    }
    if (ee.getCause() instanceof TableOfflineException) {
      throw new TableOfflineException(ee);
    }
    if (ee.getCause() instanceof SampleNotPresentException) {
      throw new SampleNotPresentException(ee.getCause().getMessage(), ee);
    }
  }

}
