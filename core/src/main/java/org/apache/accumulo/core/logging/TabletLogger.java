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
package org.apache.accumulo.core.logging;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static void assigned(KeyExtent extent, Ample.TServer server) {
    locLog.debug("Assigned {} to {}", extent, server);
  }

  /**
   * A tablet server has received an assignment message from master and queued the tablet for
   * loading.
   */
  public static void loading(KeyExtent extent, Ample.TServer server) {
    locLog.debug("Loading {} on {}", extent, server);
  }

  public static void suspended(KeyExtent extent, HostAndPort server, long time, TimeUnit timeUnit,
      int numWalogs) {
    locLog.debug("Suspended {} to {} at {} ms with {} walogs", extent, server,
        timeUnit.toMillis(time), numWalogs);
  }

  public static void unsuspended(KeyExtent extent) {
    locLog.debug("Unsuspended " + extent);
  }

  public static void loaded(KeyExtent extent, Ample.TServer server) {
    locLog.debug("Loaded {} on {}", extent, server);
  }

  public static void unassigned(KeyExtent extent, int logCount) {
    locLog.debug("Unassigned {} with {} walogs", extent, logCount);
  }

  public static void split(KeyExtent parent, KeyExtent lowChild, KeyExtent highChild,
      Ample.TServer server) {
    locLog.debug("Split {} into {} and {} on {}", parent, lowChild, highChild, server);
  }

  /**
   * Called when a tablet's current assignment state does not match the goal state.
   */
  public static void missassigned(KeyExtent extent, String goalState, String currentState,
      Ample.TServer future, Ample.TServer current, int walogs) {
    // usually this is only called when the states are not equal, but for the root tablet this
    // method is currently always called
    if (!goalState.equals(currentState)) {
      locLog.trace("Miss-assigned {} goal:{} current:{} future:{} location:{} walogs:{}", extent,
          goalState, currentState, future, current, walogs);
    }
  }

  public static void compacted(KeyExtent extent, Collection<? extends TabletFile> inputs,
      TabletFile output) {
    fileLog.debug("Compacted {} created {} from {}", extent, output, inputs);
  }

  public static void flushed(KeyExtent extent, TabletFile absMergeFile, TabletFile newDatafile) {
    if (absMergeFile == null)
      fileLog.debug("Flushed {} created {} from [memory]", extent, newDatafile);
    else
      fileLog.debug("Flushed {} created {} from [memory,{}]", extent, newDatafile, absMergeFile);
  }

  public static void bulkImported(KeyExtent extent, TabletFile file) {
    fileLog.debug("Imported {} {}  ", extent, file);
  }

  public static void recovering(KeyExtent extent, List<LogEntry> logEntries) {
    if (recoveryLog.isDebugEnabled()) {
      List<String> logIds = logEntries.stream().map(LogEntry::getUniqueID).collect(toList());
      recoveryLog.debug("For {} recovering data from walogs: {}", extent, logIds);
    }
  }

  public static void recovered(KeyExtent extent, List<LogEntry> logEntries, long numMutation,
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
