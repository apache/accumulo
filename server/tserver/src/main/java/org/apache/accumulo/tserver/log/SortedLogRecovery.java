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

import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.MUTATION;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extract Mutations for a tablet from a set of logs that have been sorted by operation and tablet.
 *
 */
public class SortedLogRecovery {
  private static final Logger log = LoggerFactory.getLogger(SortedLogRecovery.class);

  static class EmptyMapFileException extends Exception {
    private static final long serialVersionUID = 1L;

    public EmptyMapFileException() {
      super();
    }
  }

  static class UnusedException extends Exception {
    private static final long serialVersionUID = 1L;

    public UnusedException() {
      super();
    }
  }

  private VolumeManager fs;

  public SortedLogRecovery(VolumeManager fs) {
    this.fs = fs;
  }

  private enum Status {
    INITIAL, LOOKING_FOR_FINISH, COMPLETE
  }

  private static class LastStartToFinish {
    long lastStart = -1;
    long seq = -1;
    long lastFinish = -1;
    Status compactionStatus = Status.INITIAL;
    String tserverSession = "";

    private void update(long newFinish) {
      this.seq = this.lastStart;
      if (newFinish != -1)
        lastFinish = newFinish;
    }

    private void update(int newStartFile, long newStart) {
      this.lastStart = newStart;
    }

    private void update(String newSession) {
      this.lastStart = -1;
      this.lastFinish = -1;
      this.compactionStatus = Status.INITIAL;
      this.tserverSession = newSession;
    }
  }

  public void recover(KeyExtent extent, List<Path> recoveryLogs, Set<String> tabletFiles, MutationReceiver mr) throws IOException {
    int[] tids = new int[recoveryLogs.size()];
    LastStartToFinish lastStartToFinish = new LastStartToFinish();
    for (int i = 0; i < recoveryLogs.size(); i++) {
      Path logfile = recoveryLogs.get(i);
      log.info("Looking at mutations from {} for {}", logfile, extent);
      MultiReader reader = new MultiReader(fs, logfile);
      try {
        try {
          tids[i] = findLastStartToFinish(reader, i, extent, tabletFiles, lastStartToFinish);
        } catch (EmptyMapFileException ex) {
          log.info("Ignoring empty map file {}", logfile);
          tids[i] = -1;
        } catch (UnusedException ex) {
          log.info("Ignoring log file {} appears to be unused by ", logfile, extent);
          tids[i] = -1;
        }
      } finally {
        try {
          reader.close();
        } catch (IOException ex) {
          log.warn("Ignoring error closing file");
        }
      }

    }

    if (lastStartToFinish.compactionStatus == Status.LOOKING_FOR_FINISH)
      throw new RuntimeException("COMPACTION_FINISH (without preceding COMPACTION_START) not followed by successful minor compaction");

    for (int i = 0; i < recoveryLogs.size(); i++) {
      Path logfile = recoveryLogs.get(i);
      MultiReader reader = new MultiReader(fs, logfile);
      try {
        playbackMutations(reader, tids[i], lastStartToFinish, mr);
      } finally {
        try {
          reader.close();
        } catch (IOException ex) {
          log.warn("Ignoring error closing file");
        }
      }
      log.info("Recovery complete for {} using{} ", extent, logfile);
    }
  }

  private String getPathSuffix(String pathString) {
    Path path = new Path(pathString);
    if (path.depth() < 2)
      throw new IllegalArgumentException("Bad path " + pathString);
    return path.getParent().getName() + "/" + path.getName();
  }

  int findLastStartToFinish(MultiReader reader, int fileno, KeyExtent extent, Set<String> tabletFiles, LastStartToFinish lastStartToFinish) throws IOException,
      EmptyMapFileException, UnusedException {

    HashSet<String> suffixes = new HashSet<>();
    for (String path : tabletFiles)
      suffixes.add(getPathSuffix(path));

    // Scan for tableId for this extent (should always be in the log)
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    int tid = -1;
    if (!reader.next(key, value))
      throw new EmptyMapFileException();
    if (key.event != OPEN)
      throw new RuntimeException("First log entry value is not OPEN");

    if (key.tserverSession.compareTo(lastStartToFinish.tserverSession) != 0) {
      if (lastStartToFinish.compactionStatus == Status.LOOKING_FOR_FINISH)
        throw new RuntimeException("COMPACTION_FINISH (without preceding COMPACTION_START) is not followed by a successful minor compaction.");
      lastStartToFinish.update(key.tserverSession);
    }
    KeyExtent alternative = extent;
    if (extent.isRootTablet()) {
      alternative = RootTable.OLD_EXTENT;
    }

    LogFileKey defineKey = null;

    // find the maximum tablet id... because a tablet may leave a tserver and then come back, in which case it would have a different tablet id
    // for the maximum tablet id, find the minimum sequence #... may be ok to find the max seq, but just want to make the code behave like it used to
    while (reader.next(key, value)) {

      if (key.event != DEFINE_TABLET)
        break;
      if (key.tablet.equals(extent) || key.tablet.equals(alternative)) {
        if (tid != key.tid) {
          tid = key.tid;
          defineKey = key;
          key = new LogFileKey();
        }
      }
    }
    if (tid < 0) {
      throw new UnusedException();
    }

    log.debug("Found tid, seq {} {}", tid, defineKey.seq);

    // Scan start/stop events for this tablet
    key = defineKey;
    key.event = COMPACTION_START;
    reader.seek(key);
    while (reader.next(key, value)) {
      // LogFileEntry.printEntry(entry);
      if (key.tid != tid)
        break;
      if (key.event == COMPACTION_START) {
        if (lastStartToFinish.compactionStatus == Status.INITIAL)
          lastStartToFinish.compactionStatus = Status.COMPLETE;
        if (key.seq <= lastStartToFinish.lastStart)
          throw new RuntimeException("Sequence numbers are not increasing for start/stop events: " + key.seq + " vs " + lastStartToFinish.lastStart);
        lastStartToFinish.update(fileno, key.seq);

        // Tablet server finished the minor compaction, but didn't remove the entry from the METADATA table.
        log.debug("minor compaction into {} finished, but was still in the METADATA", key.filename);
        if (suffixes.contains(getPathSuffix(key.filename)))
          lastStartToFinish.update(-1);
      } else if (key.event == COMPACTION_FINISH) {
        if (key.seq <= lastStartToFinish.lastStart)
          throw new RuntimeException("Sequence numbers are not increasing for start/stop events: " + key.seq + " vs " + lastStartToFinish.lastStart);
        if (lastStartToFinish.compactionStatus == Status.INITIAL)
          lastStartToFinish.compactionStatus = Status.LOOKING_FOR_FINISH;
        else if (lastStartToFinish.lastFinish > lastStartToFinish.lastStart)
          throw new RuntimeException("COMPACTION_FINISH does not have preceding COMPACTION_START event.");
        else
          lastStartToFinish.compactionStatus = Status.COMPLETE;
        lastStartToFinish.update(key.seq);
      } else
        break;
    }
    return tid;
  }

  private void playbackMutations(MultiReader reader, int tid, LastStartToFinish lastStartToFinish, MutationReceiver mr) throws IOException {
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // Playback mutations after the last stop to finish
    log.info("Scanning for mutations starting at sequence number {} for tid {}", lastStartToFinish.seq, tid);
    key.event = MUTATION;
    key.tid = tid;
    // the seq number for the minor compaction start is now the same as the
    // last update made to memory. Scan up to that mutation, but not past it.
    key.seq = lastStartToFinish.seq;
    reader.seek(key);
    while (true) {
      if (!reader.next(key, value))
        break;
      if (key.tid != tid)
        break;
      if (key.event == MUTATION) {
        mr.receive(value.mutations.get(0));
      } else if (key.event == MANY_MUTATIONS) {
        for (Mutation m : value.mutations) {
          mr.receive(m);
        }
      } else {
        throw new RuntimeException("unexpected log key type: " + key.event);
      }
    }
  }
}
