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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.ThriftScanner.ScanState;
import org.apache.accumulo.core.client.impl.ThriftScanner.ScanTimedOutException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScannerIterator implements Iterator<Entry<Key,Value>> {

  private static final Logger log = LoggerFactory.getLogger(ScannerIterator.class);

  // scanner options
  private int timeOut;

  // scanner state
  private Iterator<KeyValue> iter;
  private ScanState scanState;

  private ScannerOptions options;

  private ArrayBlockingQueue<Object> synchQ;

  private boolean finished = false;

  private boolean readaheadInProgress = false;
  private long batchCount = 0;
  private long readaheadThreshold;

  private static final List<KeyValue> EMPTY_LIST = Collections.emptyList();

  private static ThreadPoolExecutor readaheadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 3l, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new NamingThreadFactory("Accumulo scanner read ahead thread"));

  private class Reader implements Runnable {

    @Override
    public void run() {

      try {
        while (true) {
          List<KeyValue> currentBatch = ThriftScanner.scan(scanState.context, scanState, timeOut);

          if (currentBatch == null) {
            synchQ.add(EMPTY_LIST);
            return;
          }

          if (currentBatch.size() == 0)
            continue;

          synchQ.add(currentBatch);
          return;
        }
      } catch (IsolationException | ScanTimedOutException | AccumuloException | AccumuloSecurityException | TableDeletedException | TableOfflineException
          | SampleNotPresentException e) {
        log.trace("{}", e.getMessage(), e);
        synchQ.add(e);
      } catch (TableNotFoundException e) {
        log.warn("{}", e.getMessage(), e);
        synchQ.add(e);
      } catch (Exception e) {
        log.error("{}", e.getMessage(), e);
        synchQ.add(e);
      }
    }

  }

  ScannerIterator(ClientContext context, Table.ID tableId, Authorizations authorizations, Range range, int size, int timeOut, ScannerOptions options,
      boolean isolated, long readaheadThreshold) {
    this.timeOut = timeOut;
    this.readaheadThreshold = readaheadThreshold;

    this.options = new ScannerOptions(options);

    synchQ = new ArrayBlockingQueue<>(1);

    if (this.options.fetchedColumns.size() > 0) {
      range = range.bound(this.options.fetchedColumns.first(), this.options.fetchedColumns.last());
    }

    scanState = new ScanState(context, tableId, authorizations, new Range(range), options.fetchedColumns, size, options.serverSideIteratorList,
        options.serverSideIteratorOptions, isolated, readaheadThreshold, options.getSamplerConfiguration(), options.batchTimeOut, options.classLoaderContext);

    // If we want to start readahead immediately, don't wait for hasNext to be called
    if (0l == readaheadThreshold) {
      initiateReadAhead();
    }
    iter = null;
  }

  private void initiateReadAhead() {
    readaheadInProgress = true;
    readaheadPool.execute(new Reader());
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean hasNext() {
    if (finished)
      return false;

    if (iter != null && iter.hasNext()) {
      return true;
    }

    // this is done in order to find see if there is another batch to get

    try {
      if (!readaheadInProgress) {
        // no read ahead run, fetch the next batch right now
        new Reader().run();
      }

      Object obj = synchQ.take();

      if (obj instanceof Exception) {
        finished = true;
        if (obj instanceof RuntimeException)
          throw (RuntimeException) obj;
        else
          throw new RuntimeException((Exception) obj);
      }

      List<KeyValue> currentBatch = (List<KeyValue>) obj;

      if (currentBatch.size() == 0) {
        currentBatch = null;
        finished = true;
        return false;
      }
      iter = currentBatch.iterator();
      batchCount++;

      if (batchCount > readaheadThreshold) {
        // start a thread to read the next batch
        initiateReadAhead();
      }

    } catch (InterruptedException e1) {
      throw new RuntimeException(e1);
    }

    return true;
  }

  @Override
  public Entry<Key,Value> next() {
    if (hasNext())
      return iter.next();
    throw new NoSuchElementException();
  }

  // just here to satisfy the interface
  // could make this actually delete things from the database
  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove is not supported in Scanner");
  }

}
