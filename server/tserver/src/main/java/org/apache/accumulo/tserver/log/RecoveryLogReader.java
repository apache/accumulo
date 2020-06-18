/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.log;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A class which reads sorted recovery logs produced from a single WAL.
 *
 * Presently only supports next() and seek() and works on all the Map directories within a
 * directory. The primary purpose of this class is to merge the results of multiple Reduce jobs that
 * result in Map output files.
 */
public class RecoveryLogReader implements CloseableIterator<Entry<LogFileKey,LogFileValue>> {

  /**
   * Group together the next key/value from a Reader with the Reader
   */
  private static class Index implements Comparable<Index> {
    Reader reader;
    WritableComparable<?> key;
    Writable value;
    boolean cached = false;

    private static Object create(java.lang.Class<?> klass) {
      try {
        return klass.getConstructor().newInstance();
      } catch (Throwable t) {
        throw new RuntimeException("Unable to construct objects to use for comparison");
      }
    }

    public Index(Reader reader) {
      this.reader = reader;
      key = (WritableComparable<?>) create(reader.getKeyClass());
      value = (Writable) create(reader.getValueClass());
    }

    private void cache() throws IOException {
      if (!cached && reader.next(key, value)) {
        cached = true;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj || (obj != null && obj instanceof Index && compareTo((Index) obj) == 0);
    }

    @Override
    public int compareTo(Index o) {
      try {
        cache();
        o.cache();
        // no more data: always goes to the end
        if (!cached)
          return 1;
        if (!o.cached)
          return -1;
        @SuppressWarnings({"unchecked", "rawtypes"})
        int result = ((WritableComparable) key).compareTo(o.key);
        return result;
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }
  }

  private PriorityQueue<Index> heap = new PriorityQueue<>();
  private Iterator<Entry<LogFileKey,LogFileValue>> iter;

  public RecoveryLogReader(VolumeManager fs, Path directory) throws IOException {
    this(fs, directory, null, null);
  }

  public RecoveryLogReader(VolumeManager fs, Path directory, LogFileKey start, LogFileKey end)
      throws IOException {
    boolean foundFinish = false;
    for (FileStatus child : fs.listStatus(directory)) {
      if (child.getPath().getName().startsWith("_"))
        continue;
      if (SortedLogState.isFinished(child.getPath().getName())) {
        foundFinish = true;
        continue;
      }
      if (SortedLogState.FAILED.getMarker().equals(child.getPath().getName())) {
        continue;
      }
      FileSystem ns = fs.getFileSystemByPath(child.getPath());
      heap.add(new Index(new Reader(ns.makeQualified(child.getPath()), ns.getConf())));
    }
    if (!foundFinish)
      throw new IOException(
          "Sort \"" + SortedLogState.FINISHED.getMarker() + "\" flag not found in " + directory);

    iter = new SortCheckIterator(new RangeIterator(start, end));
  }

  private static void copy(Writable src, Writable dest) throws IOException {
    // not exactly efficient...
    DataOutputBuffer output = new DataOutputBuffer();
    src.write(output);
    DataInputBuffer input = new DataInputBuffer();
    input.reset(output.getData(), output.getLength());
    dest.readFields(input);
  }

  @VisibleForTesting
  synchronized boolean next(WritableComparable<?> key, Writable val) throws IOException {
    Index elt = heap.remove();
    try {
      elt.cache();
      if (elt.cached) {
        copy(elt.key, key);
        copy(elt.value, val);
        elt.cached = false;
      } else {
        return false;
      }
    } finally {
      heap.add(elt);
    }
    return true;
  }

  @VisibleForTesting
  synchronized boolean seek(WritableComparable<?> key) throws IOException {
    PriorityQueue<Index> reheap = new PriorityQueue<>(heap.size());
    boolean result = false;
    for (Index index : heap) {
      try {
        WritableComparable<?> found = index.reader.getClosest(key, index.value, true);
        if (found != null && found.equals(key)) {
          result = true;
        }
      } catch (EOFException ex) {
        // thrown if key is beyond all data in the map
      }
      index.cached = false;
      reheap.add(index);
    }
    heap = reheap;
    return result;
  }

  @Override
  public void close() throws IOException {
    IOException problem = null;
    for (Index index : heap) {
      try {
        index.reader.close();
      } catch (IOException ex) {
        problem = ex;
      }
    }
    if (problem != null)
      throw problem;
    heap = null;
  }

  /**
   * Ensures source iterator provides data in sorted order
   */
  @VisibleForTesting
  static class SortCheckIterator implements Iterator<Entry<LogFileKey,LogFileValue>> {

    private PeekingIterator<Entry<LogFileKey,LogFileValue>> source;

    SortCheckIterator(Iterator<Entry<LogFileKey,LogFileValue>> source) {
      this.source = Iterators.peekingIterator(source);

    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public Entry<LogFileKey,LogFileValue> next() {
      Entry<LogFileKey,LogFileValue> next = source.next();
      if (source.hasNext()) {
        Preconditions.checkState(next.getKey().compareTo(source.peek().getKey()) <= 0,
            "Keys not in order %s %s", next.getKey(), source.peek().getKey());
      }
      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  private class RangeIterator implements Iterator<Entry<LogFileKey,LogFileValue>> {

    private LogFileKey key = new LogFileKey();
    private LogFileValue value = new LogFileValue();
    private boolean hasNext;
    private LogFileKey end;

    private boolean next(LogFileKey key, LogFileValue value) throws IOException {
      try {
        return RecoveryLogReader.this.next(key, value);
      } catch (EOFException e) {
        return false;
      }
    }

    RangeIterator(LogFileKey start, LogFileKey end) throws IOException {
      this.end = end;

      if (start != null) {
        hasNext = next(key, value);

        if (hasNext && key.event != LogEvents.OPEN) {
          throw new IllegalStateException("First log entry value is not OPEN");
        }

        seek(start);
      }

      hasNext = next(key, value);

      if (hasNext && start != null && key.compareTo(start) < 0) {
        throw new IllegalStateException("First key is less than start " + key + " " + start);
      }

      if (hasNext && end != null && key.compareTo(end) > 0) {
        hasNext = false;
      }
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public Entry<LogFileKey,LogFileValue> next() {
      Preconditions.checkState(hasNext);
      Entry<LogFileKey,LogFileValue> entry = new AbstractMap.SimpleImmutableEntry<>(key, value);

      key = new LogFileKey();
      value = new LogFileValue();
      try {
        hasNext = next(key, value);
        if (hasNext && end != null && key.compareTo(end) > 0) {
          hasNext = false;
        }
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }

      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Entry<LogFileKey,LogFileValue> next() {
    return iter.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

}
