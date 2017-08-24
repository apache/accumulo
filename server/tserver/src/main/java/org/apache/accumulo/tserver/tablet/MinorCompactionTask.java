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

import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.trace.ProbabilitySampler;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MinorCompactionTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(MinorCompactionTask.class);

  private final Tablet tablet;
  private long queued;
  private CommitSession commitSession;
  private DataFileValue stats;
  private FileRef mergeFile;
  private long flushId;
  private MinorCompactionReason mincReason;
  private double tracePercent;

  MinorCompactionTask(Tablet tablet, FileRef mergeFile, CommitSession commitSession, long flushId, MinorCompactionReason mincReason, double tracePercent) {
    this.tablet = tablet;
    queued = System.currentTimeMillis();
    tablet.minorCompactionWaitingToStart();
    this.commitSession = commitSession;
    this.mergeFile = mergeFile;
    this.flushId = flushId;
    this.mincReason = mincReason;
    this.tracePercent = tracePercent;
  }

  @Override
  public void run() {
    tablet.minorCompactionStarted();
    ProbabilitySampler sampler = new ProbabilitySampler(tracePercent);
    Span minorCompaction = Trace.on("minorCompaction", sampler);
    try {
      FileRef newMapfileLocation = tablet.getNextMapFilename(mergeFile == null ? "F" : "M");
      FileRef tmpFileRef = new FileRef(newMapfileLocation.path() + "_tmp");
      Span span = Trace.start("waitForCommits");
      synchronized (tablet) {
        commitSession.waitForCommitsToFinish();
      }
      span.stop();
      span = Trace.start("start");
      while (true) {
        try {
          // the purpose of the minor compaction start event is to keep track of the filename... in the case
          // where the metadata table write for the minor compaction finishes and the process dies before
          // writing the minor compaction finish event, then the start event+filename in metadata table will
          // prevent recovery of duplicate data... the minor compaction start event could be written at any time
          // before the metadata write for the minor compaction
          tablet.getTabletServer().minorCompactionStarted(commitSession, commitSession.getWALogSeq() + 1, newMapfileLocation.path().toString());
          break;
        } catch (IOException e) {
          log.warn("Failed to write to write ahead log {}", e.getMessage(), e);
        }
      }
      span.stop();
      span = Trace.start("compact");
      this.stats = tablet.minorCompact(tablet.getTabletServer().getFileSystem(), tablet.getTabletMemory().getMinCMemTable(), tmpFileRef, newMapfileLocation,
          mergeFile, true, queued, commitSession, flushId, mincReason);
      span.stop();

      minorCompaction.data("extent", tablet.getExtent().toString());
      minorCompaction.data("numEntries", Long.toString(this.stats.getNumEntries()));
      minorCompaction.data("size", Long.toString(this.stats.getSize()));
      minorCompaction.stop();

      if (tablet.needsSplit()) {
        tablet.getTabletServer().executeSplit(tablet);
      } else {
        tablet.initiateMajorCompaction(MajorCompactionReason.NORMAL);
      }
    } catch (Throwable t) {
      log.error("Unknown error during minor compaction for extent: " + tablet.getExtent(), t);
      throw new RuntimeException(t);
    } finally {
      tablet.minorCompactionComplete();
      minorCompaction.stop();
    }
  }
}
