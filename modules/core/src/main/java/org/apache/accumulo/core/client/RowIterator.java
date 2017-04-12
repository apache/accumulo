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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

/**
 * Group Key/Value pairs into Iterators over rows. Suggested usage:
 *
 * <pre>
 * RowIterator rowIterator = new RowIterator(connector.createScanner(tableName, authorizations));
 * </pre>
 */
public class RowIterator implements Iterator<Iterator<Entry<Key,Value>>> {

  /**
   * Iterate over entries in a single row.
   */
  private static class SingleRowIter implements Iterator<Entry<Key,Value>> {
    private PeekingIterator<Entry<Key,Value>> source;
    private Text currentRow = null;
    private long count = 0;
    private boolean disabled = false;

    /**
     * SingleRowIter must be passed a PeekingIterator so that it can peek at the next entry to see if it belongs in the current row or not.
     */
    public SingleRowIter(PeekingIterator<Entry<Key,Value>> source) {
      this.source = source;
      if (source.hasNext())
        currentRow = source.peek().getKey().getRow();
    }

    @Override
    public boolean hasNext() {
      if (disabled)
        throw new IllegalStateException("SingleRowIter no longer valid");
      return currentRow != null;
    }

    @Override
    public Entry<Key,Value> next() {
      if (disabled)
        throw new IllegalStateException("SingleRowIter no longer valid");
      return _next();
    }

    private Entry<Key,Value> _next() {
      if (currentRow == null)
        throw new NoSuchElementException();
      count++;
      Entry<Key,Value> kv = source.next();
      if (!source.hasNext() || !source.peek().getKey().getRow().equals(currentRow)) {
        currentRow = null;
      }
      return kv;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Get a count of entries read from the row (only equals the number of entries in the row when the row has been read fully).
     */
    public long getCount() {
      return count;
    }

    /**
     * Consume the rest of the row. Disables the iterator from future use.
     */
    public void consume() {
      disabled = true;
      while (currentRow != null)
        _next();
    }
  }

  private final PeekingIterator<Entry<Key,Value>> iter;
  private long count = 0;
  private SingleRowIter lastIter = null;

  /**
   * Create an iterator from an (ordered) sequence of KeyValue pairs.
   */
  public RowIterator(Iterator<Entry<Key,Value>> iterator) {
    this.iter = new PeekingIterator<>(iterator);
  }

  /**
   * Create an iterator from an Iterable.
   */
  public RowIterator(Iterable<Entry<Key,Value>> iterable) {
    this(iterable.iterator());
  }

  /**
   * Returns true if there is at least one more row to get.
   *
   * If the last row hasn't been fully read, this method will read through the end of the last row so it can determine if the underlying iterator has a next
   * row. The last row is disabled from future use.
   */
  @Override
  public boolean hasNext() {
    if (lastIter != null) {
      lastIter.consume();
      count += lastIter.getCount();
      lastIter = null;
    }
    return iter.hasNext();
  }

  /**
   * Fetch the next row.
   */
  @Override
  public Iterator<Entry<Key,Value>> next() {
    if (!hasNext())
      throw new NoSuchElementException();
    return lastIter = new SingleRowIter(iter);
  }

  /**
   * Unsupported.
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get a count of the total number of entries in all rows read so far.
   */
  public long getKVCount() {
    return count;
  }
}
