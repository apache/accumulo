/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;

import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

class MinorCompactionTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(MinorCompactionTask.class);

  private final Tablet tablet;
  private long queued;
  private CommitSession commitSession;
  private DataFileValue stats;
  private long flushId;
  private MinorCompactionReason mincReason;

  MinorCompactionTask(Tablet tablet, CommitSession commitSession, long flushId,
      MinorCompactionReason mincReason) {
    this.tablet = tablet;
    queued = System.currentTimeMillis();
    tablet.minorCompactionWaitingToStart();
    this.commitSession = commitSession;
    this.flushId = flushId;
    this.mincReason = mincReason;
  }

  @Override
  public void run() {
    tablet.minorCompactionStarted();
    try {
      Span span = TraceUtil.startSpan(this.getClass(), "minorCompaction");
      try (Scope scope = span.makeCurrent()) {
        Span span2 = TraceUtil.startSpan(this.getClass(), "waitForCommits");
        try (Scope scope2 = span2.makeCurrent()) {
          synchronized (tablet) {
            commitSession.waitForCommitsToFinish();
          }
        } catch (Exception e) {
          TraceUtil.setException(span2, e, true);
          throw e;
        } finally {
          span2.end();
        }
        TabletFile newFile = null;
        TabletFile tmpFile = null;
        Span span3 = TraceUtil.startSpan(this.getClass(), "start");
        try (Scope scope3 = span3.makeCurrent()) {
          while (true) {
            try {
              if (newFile == null) {
                newFile = tablet.getNextMapFilename("F");
                tmpFile = new TabletFile(new Path(newFile.getPathStr() + "_tmp"));
              }
              /*
               * the purpose of the minor compaction start event is to keep track of the filename...
               * in the case where the metadata table write for the minor compaction finishes and
               * the process dies before writing the minor compaction finish event, then the start
               * event+filename in metadata table will prevent recovery of duplicate data... the
               * minor compaction start event could be written at any time before the metadata write
               * for the minor compaction
               */
              tablet.getTabletServer().minorCompactionStarted(commitSession,
                  commitSession.getWALogSeq() + 1, newFile.getMetaInsert());
              break;
            } catch (IOException e) {
              // An IOException could have occurred while creating the new file
              if (newFile == null) {
                log.warn("Failed to create new file for minor compaction {}", e.getMessage(), e);
              } else {
                log.warn("Failed to write to write ahead log {}", e.getMessage(), e);
              }

            }
          }
        } catch (Exception e) {
          TraceUtil.setException(span3, e, true);
          throw e;
        } finally {
          span3.end();
        }
        Span span4 = TraceUtil.startSpan(this.getClass(), "compact");
        try (Scope scope4 = span4.makeCurrent()) {
          this.stats = tablet.minorCompact(tablet.getTabletMemory().getMinCMemTable(), tmpFile,
              newFile, queued, commitSession, flushId, mincReason);
        } catch (Exception e) {
          TraceUtil.setException(span4, e, true);
          throw e;
        } finally {
          span4.end();
        }

        span.setAttribute("extent", tablet.getExtent().toString());
        span.setAttribute("numEntries", Long.toString(this.stats.getNumEntries()));
        span.setAttribute("size", Long.toString(this.stats.getSize()));
      } catch (Exception e) {
        TraceUtil.setException(span, e, true);
        throw e;
      } finally {
        span.end();
      }

      if (tablet.needsSplit()) {
        tablet.getTabletServer().executeSplit(tablet);
      }
    } catch (Exception e) {
      log.error("Unknown error during minor compaction for extent: {}", tablet.getExtent(), e);
      throw e;
    } finally {
      tablet.minorCompactionComplete();
    }
  }
}
