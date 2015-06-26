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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scanner {
  private static final Logger log = LoggerFactory.getLogger(Scanner.class);

  private final Tablet tablet;
  private final ScanOptions options;
  private Range range;
  private SortedKeyValueIterator<Key,Value> isolatedIter;
  private ScanDataSource isolatedDataSource;
  private boolean sawException = false;
  private boolean scanClosed = false;

  Scanner(Tablet tablet, Range range, ScanOptions options) {
    this.tablet = tablet;
    this.range = range;
    this.options = options;
  }

  public synchronized ScanBatch read() throws IOException, TabletClosedException {

    if (sawException)
      throw new IllegalStateException("Tried to use scanner after exception occurred.");

    if (scanClosed)
      throw new IllegalStateException("Tried to use scanner after it was closed.");

    Batch results = null;

    ScanDataSource dataSource;

    if (options.isIsolated()) {
      if (isolatedDataSource == null)
        isolatedDataSource = new ScanDataSource(tablet, options);
      dataSource = isolatedDataSource;
    } else {
      dataSource = new ScanDataSource(tablet, options);
    }

    try {

      SortedKeyValueIterator<Key,Value> iter;

      if (options.isIsolated()) {
        if (isolatedIter == null)
          isolatedIter = new SourceSwitchingIterator(dataSource, true);
        else
          isolatedDataSource.reattachFileManager();
        iter = isolatedIter;
      } else {
        iter = new SourceSwitchingIterator(dataSource, false);
      }

      results = tablet.nextBatch(iter, range, options.getNum(), options.getColumnSet(), options.getBatchTimeOut());

      if (results.getResults() == null) {
        range = null;
        return new ScanBatch(new ArrayList<KVEntry>(), false);
      } else if (results.getContinueKey() == null) {
        return new ScanBatch(results.getResults(), false);
      } else {
        range = new Range(results.getContinueKey(), !results.isSkipContinueKey(), range.getEndKey(), range.isEndKeyInclusive());
        return new ScanBatch(results.getResults(), true);
      }

    } catch (IterationInterruptedException iie) {
      sawException = true;
      if (tablet.isClosed())
        throw new TabletClosedException(iie);
      else
        throw iie;
    } catch (IOException ioe) {
      if (tablet.shutdownInProgress()) {
        log.debug("IOException while shutdown in progress ", ioe);
        throw new TabletClosedException(ioe); // assume IOException was caused by execution of HDFS shutdown hook
      }

      sawException = true;
      dataSource.close(true);
      throw ioe;
    } catch (RuntimeException re) {
      sawException = true;
      throw re;
    } finally {
      // code in finally block because always want
      // to return mapfiles, even when exception is thrown
      if (!options.isIsolated()) {
        dataSource.close(false);
      } else {
        dataSource.detachFileManager();
      }

      if (results != null && results.getResults() != null)
        tablet.updateQueryStats(results.getResults().size(), results.getNumBytes());
    }
  }

  // close and read are synchronized because can not call close on the data source while it is in use
  // this could lead to the case where file iterators that are in use by a thread are returned
  // to the pool... this would be bad
  public void close() {
    options.getInterruptFlag().set(true);
    synchronized (this) {
      scanClosed = true;
      if (isolatedDataSource != null)
        isolatedDataSource.close(false);
    }
  }
}
