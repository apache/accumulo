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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

/**
 * Iterates over multiple sorted recovery logs merging them into a single sorted stream.
 */
public class RecoveryLogsIterator
    implements Iterator<Entry<LogFileKey,LogFileValue>>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryLogsIterator.class);

  private List<RecoveryLogReader> readers;
  private UnmodifiableIterator<Entry<LogFileKey,LogFileValue>> iter;

  /**
   * Ensures source iterator provides data in sorted order
   */
  // TODO add unit test and move to RecoveryLogReader
  @VisibleForTesting
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

  // TODO get rid of this (push down into iterator in RecoveryLogReader)
  private RecoveryLogReader open(VolumeManager fs, Path log) throws IOException {
    RecoveryLogReader reader = new RecoveryLogReader(fs, log);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    if (!reader.next(key, value)) {
      reader.close();
      return null;
    }
    if (key.event != OPEN) {
      RuntimeException e = new IllegalStateException(
          "First log entry value is not OPEN (" + log + ")");
      try {
        reader.close();
      } catch (Exception e2) {
        e.addSuppressed(e2);
      }
      throw e;
    }

    return reader;
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
        RecoveryLogReader reader = open(fs, log);
        if (reader != null) {
          readers.add(reader);
          iterators.add(new SortCheckIterator(log.getName(), reader.getIterator(start, end)));
        }
      }

      iter = Iterators.mergeSorted(iterators, new Comparator<Entry<LogFileKey,LogFileValue>>() {
        @Override
        public int compare(Entry<LogFileKey,LogFileValue> o1, Entry<LogFileKey,LogFileValue> o2) {
          return o1.getKey().compareTo(o2.getKey());
        }
      });

    } catch (RuntimeException | IOException e) {
      try {
        close();
      } catch (Exception e2) {
        e.addSuppressed(e2);
      }
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
    for (RecoveryLogReader reader : readers) {
      try {
        reader.close();
      } catch (IOException e) {
        LOG.debug("Failed to close reader", e);
      }
    }
  }
}
