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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.accumulo.tserver.log.RecoveryLogsIterator.maxKey;
import static org.apache.accumulo.tserver.log.RecoveryLogsIterator.minKey;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.MUTATION;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * Extract Mutations for a tablet from a set of logs that have been sorted by operation and tablet.
 *
 */
public class SortedLogRecovery {

  private static final Logger log = LoggerFactory.getLogger(SortedLogRecovery.class);

  private VolumeManager fs;

  public SortedLogRecovery(VolumeManager fs) {
    this.fs = fs;
  }

  private int findMaxTabletId(KeyExtent extent, List<Path> recoveryLogs) throws IOException {
    int tid = -1;

    try (RecoveryLogsIterator rli = new RecoveryLogsIterator(fs, recoveryLogs, DEFINE_TABLET)) {
      KeyExtent alternative = extent;
      if (extent.isRootTablet()) {
        alternative = RootTable.OLD_EXTENT;
      }

      while (rli.hasNext()) {
        LogFileKey key = rli.next().getKey();

        checkState(key.event == DEFINE_TABLET); // should only fail if bug elsewhere

        if (key.tablet.equals(extent) || key.tablet.equals(alternative)) {
          checkState(key.tid >= 0, "Tid %s for %s is negative", key.tid, extent);
          checkState(tid == -1 || key.tid >= tid); // should only fail if bug in
                                                   // RecoveryLogsIterator

          if (tid != key.tid) {
            tid = key.tid;
          }
        }
      }
    }
    return tid;
  }

  private String getPathSuffix(String pathString) {
    Path path = new Path(pathString);
    if (path.depth() < 2)
      throw new IllegalArgumentException("Bad path " + pathString);
    return path.getParent().getName() + "/" + path.getName();
  }

  static class DeduplicatingIterator implements Iterator<Entry<LogFileKey,LogFileValue>> {

    private PeekingIterator<Entry<LogFileKey,LogFileValue>> source;

    public DeduplicatingIterator(Iterator<Entry<LogFileKey,LogFileValue>> source) {
      this.source = Iterators.peekingIterator(source);
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public Entry<LogFileKey,LogFileValue> next() {
      Entry<LogFileKey,LogFileValue> next = source.next();

      while (source.hasNext() && next.getKey().compareTo(source.peek().getKey()) == 0) {
        source.next();
      }

      return next;
    }

  }

  private long findLastStartToFinish(List<Path> recoveryLogs, Set<String> tabletFiles, int tid)
      throws IOException {
    HashSet<String> suffixes = new HashSet<>();
    for (String path : tabletFiles)
      suffixes.add(getPathSuffix(path));

    long lastStart = 0;
    long recoverySeq = 0;

    try (RecoveryLogsIterator rli = new RecoveryLogsIterator(fs, recoveryLogs, COMPACTION_START,
        tid)) {

      DeduplicatingIterator ddi = new DeduplicatingIterator(rli);

      String lastStartFile = null;
      LogEvents lastEvent = null;
      boolean firstEventWasFinish = false;
      boolean sawStartFinish = false;

      while (ddi.hasNext()) {
        LogFileKey key = ddi.next().getKey();

        checkState(key.seq >= 0, "Unexpected negative seq %s for tid %s", key.seq, tid);
        checkState(key.tid == tid); // should only fail if bug elsewhere

        if (key.event == COMPACTION_START) {
          checkState(key.seq >= lastStart); // should only fail if bug elsewhere
          lastStart = key.seq;
          lastStartFile = key.filename;
        } else if (key.event == COMPACTION_FINISH) {
          if (lastEvent == null) {
            firstEventWasFinish = true;
          } else if (lastEvent == COMPACTION_FINISH) {
            throw new IllegalStateException(
                "Saw consecutive COMPACTION_FINISH events " + key.tid + " " + key.seq);
          } else {
            if (key.seq <= lastStart) {
              throw new IllegalStateException(
                  "Compaction finish <= start " + lastStart + " " + key.seq);
            }
            recoverySeq = lastStart;
            lastStartFile = null;
            sawStartFinish = true;
          }
        } else {
          throw new IllegalStateException("Non compaction event seen " + key.event);
        }

        lastEvent = key.event;
      }

      if (firstEventWasFinish && !sawStartFinish) {
        throw new IllegalStateException(
            "COMPACTION_FINISH (without preceding COMPACTION_START) is not followed by a successful minor compaction.");
      }

      if (lastStartFile != null && suffixes.contains(getPathSuffix(lastStartFile))) {
        // There was no compaction finish event, however the last compaction start event has a file
        // in the metadata table, so the compaction finished.
        log.debug("Considering compaction start {} {} finished because file {} in metadata table",
            tid, lastStart, getPathSuffix(lastStartFile));
        recoverySeq = lastStart;
      }
    }
    return recoverySeq;
  }

  private void playbackMutations(List<Path> recoveryLogs, MutationReceiver mr, int tid,
      long recoverySeq) throws IOException {
    LogFileKey start = minKey(MUTATION, tid);
    start.seq = recoverySeq;

    LogFileKey end = maxKey(MUTATION, tid);

    try (RecoveryLogsIterator rli = new RecoveryLogsIterator(fs, recoveryLogs, start, end)) {
      while (rli.hasNext()) {
        Entry<LogFileKey,LogFileValue> entry = rli.next();

        checkState(entry.getKey().tid == tid); // should only fail if bug elsewhere
        checkState(entry.getKey().seq >= recoverySeq); // should only fail if bug elsewhere

        if (entry.getKey().event == MUTATION) {
          mr.receive(entry.getValue().mutations.get(0));
        } else if (entry.getKey().event == MANY_MUTATIONS) {
          for (Mutation m : entry.getValue().mutations) {
            mr.receive(m);
          }
        } else {
          throw new IllegalStateException("Non mutation event seen " + entry.getKey().event);
        }
      }
    }
  }

  Collection<String> asNames(List<Path> recoveryLogs) {
    return Collections2.transform(recoveryLogs, new Function<Path,String>() {
      @Override
      public String apply(Path input) {
        return input.getName();
      }
    });
  }

  public void recover(KeyExtent extent, List<Path> recoveryLogs, Set<String> tabletFiles,
      MutationReceiver mr) throws IOException {

    // A tablet may leave a tserver and then come back, in which case it would have a different and
    // higher tablet id. Only want to consider events in the log related to the last time the tablet
    // was loaded.
    int tid = findMaxTabletId(extent, recoveryLogs);

    if (tid == -1) {
      log.info("Tablet {} is not defined in recovery logs {} ", extent, asNames(recoveryLogs));
      return;
    }

    // Find the seq # for the last compaction that started and finished
    long recoverySeq = findLastStartToFinish(recoveryLogs, tabletFiles, tid);

    log.info("Recovering mutations, tablet:{} tid:{} seq:{} logs:{}", extent, tid, recoverySeq,
        asNames(recoveryLogs));

    // Replay all mutations that were written after the last successful compaction started.
    playbackMutations(recoveryLogs, mr, tid, recoverySeq);
  }
}
