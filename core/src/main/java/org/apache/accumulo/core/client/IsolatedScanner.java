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
package org.apache.accumulo.core.client;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.impl.IsolationException;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * A scanner that presents a row isolated view of an accumulo table. Rows are buffered in memory on the client side. If you think your rows may not fit into
 * memory, then you can provide an alternative row buffer factory to the constructor. This would allow rows to be buffered to disk for example.
 *
 */

public class IsolatedScanner extends ScannerOptions implements Scanner {

  private static class RowBufferingIterator implements Iterator<Entry<Key,Value>> {

    private Iterator<Entry<Key,Value>> source;
    private RowBuffer buffer;
    private Entry<Key,Value> nextRowStart;
    private Iterator<Entry<Key,Value>> rowIter;
    private ByteSequence lastRow = null;
    private long timeout;

    private final Scanner scanner;
    private ScannerOptions opts;
    private Range range;
    private int batchSize;
    private long readaheadThreshold;

    private void readRow() {

      ByteSequence row = null;

      while (true) {
        buffer.clear();

        try {
          if (nextRowStart != null) {
            buffer.add(nextRowStart);
            row = nextRowStart.getKey().getRowData();
            nextRowStart = null;
          } else if (source.hasNext()) {
            Entry<Key,Value> entry = source.next();
            buffer.add(entry);
            row = entry.getKey().getRowData();
          }

          while (source.hasNext()) {
            Entry<Key,Value> entry = source.next();

            if (entry.getKey().getRowData().equals(row)) {
              buffer.add(entry);
            } else {
              nextRowStart = entry;
              break;
            }
          }

          lastRow = row;
          rowIter = buffer.iterator();
          // System.out.println("lastRow <- "+lastRow + " "+buffer);
          return;
        } catch (IsolationException ie) {
          Range seekRange = null;

          nextRowStart = null;

          if (lastRow == null)
            seekRange = range;
          else {
            Text lastRowText = new Text();
            lastRowText.set(lastRow.getBackingArray(), lastRow.offset(), lastRow.length());
            Key startKey = new Key(lastRowText).followingKey(PartialKey.ROW);
            if (!range.afterEndKey(startKey)) {
              seekRange = new Range(startKey, true, range.getEndKey(), range.isEndKeyInclusive());
            }
            // System.out.println(seekRange);
          }

          if (seekRange == null) {
            buffer.clear();
            rowIter = buffer.iterator();
            return;
          }

          // wait a moment before retrying
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

          source = newIterator(seekRange);
        }
      }
    }

    private Iterator<Entry<Key,Value>> newIterator(Range r) {
      synchronized (scanner) {
        scanner.enableIsolation();
        scanner.setBatchSize(batchSize);
        scanner.setTimeout(timeout, TimeUnit.MILLISECONDS);
        scanner.setRange(r);
        scanner.setReadaheadThreshold(readaheadThreshold);
        setOptions((ScannerOptions) scanner, opts);

        return scanner.iterator();
        // return new FaultyIterator(scanner.iterator());
      }
    }

    public RowBufferingIterator(Scanner scanner, ScannerOptions opts, Range range, long timeout, int batchSize, long readaheadThreshold,
        RowBufferFactory bufferFactory) {
      this.scanner = scanner;
      this.opts = new ScannerOptions(opts);
      this.range = range;
      this.timeout = timeout;
      this.batchSize = batchSize;
      this.readaheadThreshold = readaheadThreshold;

      buffer = bufferFactory.newBuffer();

      this.source = newIterator(range);

      readRow();
    }

    @Override
    public boolean hasNext() {
      return rowIter.hasNext();
    }

    @Override
    public Entry<Key,Value> next() {
      Entry<Key,Value> next = rowIter.next();
      if (!rowIter.hasNext()) {
        readRow();
      }

      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  public static interface RowBufferFactory {
    RowBuffer newBuffer();
  }

  public static interface RowBuffer extends Iterable<Entry<Key,Value>> {
    void add(Entry<Key,Value> entry);

    @Override
    Iterator<Entry<Key,Value>> iterator();

    void clear();
  }

  public static class MemoryRowBufferFactory implements RowBufferFactory {

    @Override
    public RowBuffer newBuffer() {
      return new MemoryRowBuffer();
    }
  }

  public static class MemoryRowBuffer implements RowBuffer {

    private ArrayList<Entry<Key,Value>> buffer = new ArrayList<>();

    @Override
    public void add(Entry<Key,Value> entry) {
      buffer.add(entry);
    }

    @Override
    public Iterator<Entry<Key,Value>> iterator() {
      return buffer.iterator();
    }

    @Override
    public void clear() {
      buffer.clear();
    }

  }

  private Scanner scanner;
  private Range range;
  private int batchSize;
  private long readaheadThreshold;
  private RowBufferFactory bufferFactory;

  public IsolatedScanner(Scanner scanner) {
    this(scanner, new MemoryRowBufferFactory());
  }

  public IsolatedScanner(Scanner scanner, RowBufferFactory bufferFactory) {
    this.scanner = scanner;
    this.range = scanner.getRange();
    this.timeOut = scanner.getTimeout(TimeUnit.MILLISECONDS);
    this.batchTimeOut = scanner.getBatchTimeout(TimeUnit.MILLISECONDS);
    this.batchSize = scanner.getBatchSize();
    this.readaheadThreshold = scanner.getReadaheadThreshold();
    this.bufferFactory = bufferFactory;
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    return new RowBufferingIterator(scanner, this, range, timeOut, batchSize, readaheadThreshold, bufferFactory);
  }

  @Deprecated
  @Override
  public void setTimeOut(int timeOut) {
    if (timeOut == Integer.MAX_VALUE)
      setTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    else
      setTimeout(timeOut, TimeUnit.SECONDS);
  }

  @Deprecated
  @Override
  public int getTimeOut() {
    long timeout = getTimeout(TimeUnit.SECONDS);
    if (timeout >= Integer.MAX_VALUE)
      return Integer.MAX_VALUE;
    return (int) timeout;
  }

  @Override
  public void setRange(Range range) {
    this.range = range;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public void enableIsolation() {
    // aye aye captain, already done sir
  }

  @Override
  public void disableIsolation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getReadaheadThreshold() {
    return readaheadThreshold;
  }

  @Override
  public void setReadaheadThreshold(long batches) {
    if (0 > batches) {
      throw new IllegalArgumentException("Number of batches before read-ahead must be non-negative");
    }

    this.readaheadThreshold = batches;
  }
}
