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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scanner {
  private static final Logger log = LoggerFactory.getLogger(Scanner.class);

  private final TabletBase tablet;
  private final ScanParameters scanParams;
  private Range range;
  private SortedKeyValueIterator<Key,Value> isolatedIter;
  private ScanDataSource isolatedDataSource;
  private boolean sawException = false;
  private boolean scanClosed = false;
  /**
   * A fair semaphore of one is used since explicitly know the access pattern will be one thread to
   * read and another to call close if the session becomes idle. Since we're explicitly preventing
   * re-entrance, we're currently using a Semaphore. If at any point we decide read needs to be
   * re-entrant, we can switch to a Reentrant lock.
   */
  private Semaphore scannerSemaphore;

  private AtomicBoolean interruptFlag;

  Scanner(TabletBase tablet, Range range, ScanParameters scanParams, AtomicBoolean interruptFlag) {
    this.tablet = tablet;
    this.range = range;
    this.scanParams = scanParams;
    this.scannerSemaphore = new Semaphore(1, true);
    this.interruptFlag = interruptFlag;
  }

  public ScanBatch read() throws IOException, TabletClosedException {

    ScanDataSource dataSource = null;

    Batch results = null;

    try {

      try {
        scannerSemaphore.acquire();
      } catch (InterruptedException e) {
        sawException = true;
      }

      // sawException may have occurred within close, so we cannot assume that an interrupted
      // exception was its cause
      if (sawException) {
        throw new IllegalStateException("Tried to use scanner after exception occurred.");
      }

      if (scanClosed) {
        throw new IllegalStateException("Tried to use scanner after it was closed.");
      }

      if (scanParams.isIsolated()) {
        if (isolatedDataSource == null) {
          isolatedDataSource = tablet.createDataSource(scanParams, true, interruptFlag);
        }
        dataSource = isolatedDataSource;
      } else {
        dataSource = tablet.createDataSource(scanParams, true, interruptFlag);
      }

      SortedKeyValueIterator<Key,Value> iter;

      if (scanParams.isIsolated()) {
        if (isolatedIter == null) {
          isolatedIter = new SourceSwitchingIterator(dataSource, true);
        } else {
          isolatedDataSource.reattachFileManager();
        }
        iter = isolatedIter;
      } else {
        iter = new SourceSwitchingIterator(dataSource, false);
      }

      results = tablet.nextBatch(iter, range, scanParams);

      if (results.getResults() == null) {
        range = null;
        return new ScanBatch(new ArrayList<>(), false);
      } else if (results.getContinueKey() == null) {
        return new ScanBatch(results.getResults(), false);
      } else {
        range = new Range(results.getContinueKey(), !results.isSkipContinueKey(), range.getEndKey(),
            range.isEndKeyInclusive());
        return new ScanBatch(results.getResults(), true);
      }

    } catch (IterationInterruptedException iie) {
      sawException = true;
      if (tablet.isClosed()) {
        throw new TabletClosedException(iie);
      } else {
        throw iie;
      }
    } catch (IOException ioe) {
      if (ShutdownUtil.wasCausedByHadoopShutdown(ioe)) {
        log.debug("IOException while shutdown in progress ", ioe);
        throw new TabletClosedException(ioe); // this was possibly caused by Hadoop shutdown hook,
                                              // so make the client retry
      }

      sawException = true;
      dataSource.close(true);
      throw ioe;
    } catch (RuntimeException re) {
      if (ShutdownUtil.wasCausedByHadoopShutdown(re)) {
        log.debug("RuntimeException while shutdown in progress ", re);
        throw new TabletClosedException(re); // this was possibly caused by Hadoop shutdown hook, so
                                             // make the client retry
      }
      sawException = true;
      throw re;
    } finally {
      // code in finally block because always want
      // to return mapfiles, even when exception is thrown
      if (dataSource != null && !scanParams.isIsolated()) {
        dataSource.close(false);
      } else if (dataSource != null) {
        dataSource.detachFileManager();
      }

      if (results != null && results.getResults() != null) {
        tablet.updateQueryStats(results.getResults().size(), results.getNumBytes());
      }

      scannerSemaphore.release();
    }
  }

  // close and read are synchronized because can not call close on the data source while it is in
  // use
  // this could lead to the case where file iterators that are in use by a thread are returned
  // to the pool... this would be bad
  public boolean close() {
    interruptFlag.set(true);

    boolean obtainedLock = false;
    try {
      obtainedLock = scannerSemaphore.tryAcquire(10, TimeUnit.MILLISECONDS);
      if (!obtainedLock) {
        return false;
      }

      scanClosed = true;
      if (isolatedDataSource != null) {
        isolatedDataSource.close(false);
      }
    } catch (InterruptedException e) {
      return false;
    } finally {
      if (obtainedLock) {
        scannerSemaphore.release();
      }
    }
    return true;
  }
}
