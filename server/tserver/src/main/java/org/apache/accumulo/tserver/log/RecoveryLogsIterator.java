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
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

/**
 * Iterates over multiple recovery logs merging them into a single sorted stream.
 */
public class RecoveryLogsIterator
    implements Iterator<Entry<LogFileKey,LogFileValue>>, AutoCloseable {

  private List<MultiReader> readers;
  private UnmodifiableIterator<Entry<LogFileKey,LogFileValue>> iter;

  private static class MultiReaderIterator implements Iterator<Entry<LogFileKey,LogFileValue>> {

    private MultiReader reader;
    private LogFileKey key = new LogFileKey();
    private LogFileValue value = new LogFileValue();
    private boolean hasNext;
    private LogFileKey end;

    MultiReaderIterator(MultiReader reader, LogFileKey start, LogFileKey end) throws IOException {
      this.reader = reader;
      this.end = end;

      reader.seek(start);

      hasNext = reader.next(key, value);

      if (hasNext && key.compareTo(start) < 0) {
        throw new IllegalStateException("First key is less than start " + key + " " + start);
      }

      if (hasNext && key.compareTo(end) > 0) {
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
        hasNext = reader.next(key, value);
        if (hasNext && key.compareTo(end) > 0) {
          hasNext = false;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return entry;
    }
  }

  static class SortCheckIterator implements Iterator<Entry<LogFileKey,LogFileValue>> {

    private PeekingIterator<Entry<LogFileKey,LogFileValue>> source;
    private String sourceName;

    SortCheckIterator(String sourceName, Iterator<Entry<LogFileKey,LogFileValue>> source) {
      this.source = Iterators.peekingIterator(source);
      this.sourceName = sourceName;
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
            "Data source %s keys not in order %s %s", sourceName, next.getKey(),
            source.peek().getKey());
      }
      return next;
    }
  }

  private MultiReader open(VolumeManager fs, Path log) throws IOException {
    MultiReader reader = new MultiReader(fs, log);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    if (!reader.next(key, value)) {
      reader.close();
      return null;
    }
    if (key.event != OPEN) {
      reader.close();
      throw new RuntimeException("First log entry value is not OPEN");
    }

    return reader;
  }

  static LogFileKey maxKey(LogEvents event) {
    LogFileKey key = new LogFileKey();
    key.event = event;
    key.tid = Integer.MAX_VALUE;
    key.seq = Long.MAX_VALUE;
    return key;
  }

  static LogFileKey maxKey(LogEvents event, int tid) {
    LogFileKey key = maxKey(event);
    key.tid = tid;
    return key;
  }

  static LogFileKey minKey(LogEvents event) {
    LogFileKey key = new LogFileKey();
    key.event = event;
    key.tid = 0;
    key.seq = 0;
    return key;
  }

  static LogFileKey minKey(LogEvents event, int tid) {
    LogFileKey key = minKey(event);
    key.tid = tid;
    return key;
  }

  /**
   * Iterates only over keys with the specified event (some events are equivalent for sorting) and
   * tid type.
   */
  RecoveryLogsIterator(VolumeManager fs, List<Path> recoveryLogPaths, LogEvents event, int tid)
      throws IOException {
    this(fs, recoveryLogPaths, minKey(event, tid), maxKey(event, tid));
  }

  /**
   * Iterates only over keys with the specified event (some events are equivalent for sorting).
   */
  RecoveryLogsIterator(VolumeManager fs, List<Path> recoveryLogPaths, LogEvents event)
      throws IOException {
    this(fs, recoveryLogPaths, minKey(event), maxKey(event));
  }

  /**
   * Iterates only over keys between [start,end].
   */
  RecoveryLogsIterator(VolumeManager fs, List<Path> recoveryLogPaths, LogFileKey start,
      LogFileKey end) throws IOException {
    readers = new ArrayList<>(recoveryLogPaths.size());

    ArrayList<Iterator<Entry<LogFileKey,LogFileValue>>> iterators = new ArrayList<>();

    try {
      for (Path log : recoveryLogPaths) {
        MultiReader reader = open(fs, log);
        if (reader != null) {
          readers.add(reader);
          iterators.add(
              new SortCheckIterator(log.getName(), new MultiReaderIterator(reader, start, end)));
        }
      }

      iter = Iterators.mergeSorted(iterators, new Comparator<Entry<LogFileKey,LogFileValue>>() {
        @Override
        public int compare(Entry<LogFileKey,LogFileValue> o1, Entry<LogFileKey,LogFileValue> o2) {
          return o1.getKey().compareTo(o2.getKey());
        }
      });

    } catch (RuntimeException | IOException e) {
      close();
      throw e;
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
  public void close() {
    for (MultiReader reader : readers) {
      try {
        reader.close();
      } catch (IOException e) {
        Log.debug("Failed to close reader", e);
      }
    }
  }
}
