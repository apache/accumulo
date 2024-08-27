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
package org.apache.accumulo.core.logging;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.UUID;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;
import com.google.common.net.HostAndPort;

/**
 * This class contains source for logs messages about a tablets internal state, like its location,
 * set of files, metadata.
 *
 * @see org.apache.accumulo.core.logging
 */
public class TabletLogger {

  private static final String PREFIX = Logging.PREFIX + "tablet.";

  private static final Logger locLog = LoggerFactory.getLogger(PREFIX + "location");
  private static final Logger fileLog = LoggerFactory.getLogger(PREFIX + "files");
  private static final Logger recoveryLog = LoggerFactory.getLogger(PREFIX + "recovery");
  private static final Logger walsLog = LoggerFactory.getLogger(PREFIX + "walogs");

  /**
   * A decision was made to assign a tablet to a tablet server process. Accumulo will stick to this
   * decision until the tablet server loads the tablet or dies.
   */
  public static void assigned(KeyExtent extent, TServerInstance server) {
    locLog.debug("Assigned {} to {}", extent, server);
  }

  /**
   * A tablet server has received an assignment message from manager and queued the tablet for
   * loading.
   */
  public static void loading(KeyExtent extent, TServerInstance server) {
    locLog.debug("Loading {} on {}", extent, server);
  }

  public static void suspended(KeyExtent extent, HostAndPort server, SteadyTime time,
      int numWalogs) {
    locLog.debug("Suspended {} to {} at {} ms with {} walogs", extent, server, time, numWalogs);
  }

  public static void unsuspended(KeyExtent extent) {
    locLog.debug("Unsuspended " + extent);
  }

  public static void loaded(KeyExtent extent, TServerInstance server) {
    locLog.debug("Loaded {} on {}", extent, server);
  }

  public static void unassigned(KeyExtent extent, int logCount) {
    locLog.debug("Unassigned {} with {} walogs", extent, logCount);
  }

  public static void split(KeyExtent parent, SortedSet<Text> splits) {
    locLog.debug("Split {} into {} tablets", parent, splits.size() + 1);
    locLog.trace("Split {} into {}", parent, splits);
  }

  /**
   * Called when a tablet's current assignment state does not match the goal state.
   */
  public static void missassigned(KeyExtent extent, String goalState, String currentState,
      TServerInstance future, TServerInstance current, int walogs) {
    // usually this is only called when the states are not equal, but for the root tablet this
    // method is currently always called
    if (!goalState.equals(currentState)) {
      locLog.trace("Miss-assigned {} goal:{} current:{} future:{} location:{} walogs:{}", extent,
          goalState, currentState, future, current, walogs);
    }
  }

  private static String getSize(Collection<CompactableFile> files) {
    long sum = files.stream().mapToLong(CompactableFile::getEstimatedSize).sum();
    return FileUtils.byteCountToDisplaySize(sum);
  }

  /**
   * Lazily converts TableFile to file names. The lazy part is really important because when it is
   * not called with log.isDebugEnabled().
   */
  private static Collection<String> asMinimalString(Collection<CompactableFile> files) {
    return Collections2.transform(files,
        cf -> CompactableFileImpl.toStoredTabletFile(cf).toMinimalString());
  }

  public static void selected(FateId fateId, KeyExtent extent,
      Collection<StoredTabletFile> inputs) {
    fileLog.trace("Selected files {} {} {}", extent, fateId,
        Collections2.transform(inputs, StoredTabletFile::toMinimalString));
  }

  public static void compacting(TabletMetadata tabletMetadata, ExternalCompactionId cid,
      String compactorAddress, CompactionJob job) {
    if (fileLog.isDebugEnabled()) {
      if (job.getKind() == CompactionKind.USER) {
        var fateId = tabletMetadata.getSelectedFiles().getFateId();
        fileLog.debug(
            "Compacting {} driver:{} id:{} group:{} compactor:{} priority:{} size:{} kind:{} files:{}",
            tabletMetadata.getExtent(), fateId, cid, job.getGroup(), compactorAddress,
            job.getPriority(), getSize(job.getFiles()), job.getKind(),
            asMinimalString(job.getFiles()));
      } else {
        fileLog.debug(
            "Compacting {} id:{} group:{} compactor:{} priority:{} size:{} kind:{} files:{}",
            tabletMetadata.getExtent(), cid, job.getGroup(), compactorAddress, job.getPriority(),
            getSize(job.getFiles()), job.getKind(), asMinimalString(job.getFiles()));
      }
    }
  }

  public static void compacted(KeyExtent extent, ExternalCompactionId ecid, CompactionKind kind,
      Collection<StoredTabletFile> inputs, Optional<ReferencedTabletFile> output) {
    var transformed = Collections2.transform(inputs, StoredTabletFile::toMinimalString);
    fileLog.debug("{} compacted {} for {} created {} from {}", ecid, extent, kind,
        output.map(f -> f + "").orElse("no output"), transformed);
  }

  public static void flushed(KeyExtent extent, Optional<StoredTabletFile> newDatafile) {
    if (newDatafile.isPresent()) {
      fileLog.debug("Flushed {} created {} from [memory]", extent, newDatafile.orElseThrow());
    } else {
      fileLog.debug("Flushed {} from [memory] but no file was written.", extent);
    }
  }

  public static void bulkImported(KeyExtent extent, TabletFile file) {
    fileLog.debug("Imported {} {}  ", extent, file);
  }

  public static void recovering(KeyExtent extent, Collection<LogEntry> logEntries) {
    if (recoveryLog.isDebugEnabled()) {
      List<UUID> logIds = logEntries.stream().map(LogEntry::getUniqueID).collect(toList());
      recoveryLog.debug("For {} recovering data from walogs: {}", extent, logIds);
    }
  }

  public static void recovered(KeyExtent extent, Collection<LogEntry> logEntries, long numMutation,
      long numEntries) {
    recoveryLog.info("For {} recovered {} mutations creating {} entries from {} walogs", extent,
        numMutation, numEntries, logEntries.size());
  }

  public static boolean isWalRefLoggingEnabled() {
    return walsLog.isTraceEnabled();
  }

  /**
   * Called when the set of write ahead logs a tablet currently has unflushed data in changes.
   */
  public static void walRefsChanged(KeyExtent extent, Collection<String> refsSupplier) {
    walsLog.trace("{} has unflushed data in wals: {} ", extent, refsSupplier);
  }

}
