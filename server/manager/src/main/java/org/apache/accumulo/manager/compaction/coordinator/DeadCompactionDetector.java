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
package org.apache.accumulo.manager.compaction.coordinator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
import org.apache.accumulo.server.util.FindCompactionTmpFiles.DeleteStats;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadCompactionDetector {

  private static final Logger log = LoggerFactory.getLogger(DeadCompactionDetector.class);

  private final ServerContext context;
  private final CompactionCoordinator coordinator;
  private final ScheduledThreadPoolExecutor schedExecutor;
  private final ConcurrentHashMap<ExternalCompactionId,Long> deadCompactions;
  private final Set<TableId> tablesWithUnreferencedTmpFiles = new HashSet<>();

  public DeadCompactionDetector(ServerContext context, CompactionCoordinator coordinator,
      ScheduledThreadPoolExecutor stpe) {
    this.context = context;
    this.coordinator = coordinator;
    this.schedExecutor = stpe;
    this.deadCompactions = new ConcurrentHashMap<>();
  }

  public void addTableId(TableId tableWithUnreferencedTmpFiles) {
    synchronized (tablesWithUnreferencedTmpFiles) {
      tablesWithUnreferencedTmpFiles.add(tableWithUnreferencedTmpFiles);
    }
  }

  private void detectDeadCompactions() {

    // The order of obtaining information is very important to avoid race conditions.

    log.debug("Starting to look for dead compactions");

    Map<ExternalCompactionId,KeyExtent> tabletCompactions = new HashMap<>();

    // find what external compactions tablets think are running
    context.getAmple().readTablets().forLevel(DataLevel.USER)
        .fetch(ColumnType.ECOMP, ColumnType.PREV_ROW).build().forEach(tm -> {
          tm.getExternalCompactions().keySet().forEach(ecid -> {
            tabletCompactions.put(ecid, tm.getExtent());
          });
        });

    if (tabletCompactions.isEmpty()) {
      // Clear out dead compactions, tservers don't think anything is running
      log.trace("Clearing the dead compaction map, no tablets have compactions running");
      this.deadCompactions.clear();
      // no need to look for dead compactions when tablets don't have anything recorded as running
    } else {
      if (log.isTraceEnabled()) {
        tabletCompactions.forEach((ecid, extent) -> log.trace("Saw {} for {}", ecid, extent));
      }

      // Remove from the dead map any compactions that the Tablet's
      // do not think are running any more.
      this.deadCompactions.keySet().retainAll(tabletCompactions.keySet());

      // Determine what compactions are currently running and remove those.
      //
      // In order for this overall algorithm to be correct and avoid race conditions, the compactor
      // must return ids covering the time period from before reservation until after commit. If the
      // ids do not cover this time period then legitimate running compactions could be canceled.
      Collection<ExternalCompactionId> running =
          ExternalCompactionUtil.getCompactionIdsRunningOnCompactors(context);

      running.forEach((ecid) -> {
        if (tabletCompactions.remove(ecid) != null) {
          log.debug("Ignoring compaction {} that is running on a compactor", ecid);
        }
        if (this.deadCompactions.remove(ecid) != null) {
          log.debug("Removed {} from the dead compaction map, it's running on a compactor", ecid);
        }
      });

      tabletCompactions.forEach((ecid, extent) -> {
        log.info("Possible dead compaction detected {} {}", ecid, extent);
        this.deadCompactions.merge(ecid, 1L, Long::sum);
      });

      // Everything left in tabletCompactions is no longer running anywhere and should be failed.
      // Its possible that a compaction committed while going through the steps above, if so then
      // that is ok and marking it failed will end up being a no-op.
      Set<ExternalCompactionId> toFail =
          this.deadCompactions.entrySet().stream().filter(e -> e.getValue() > 2)
              .map(e -> e.getKey()).collect(Collectors.toCollection(TreeSet::new));
      tabletCompactions.keySet().retainAll(toFail);
      tabletCompactions.forEach((eci, v) -> {
        log.warn("Compaction {} believed to be dead, failing it.", eci);
      });
      coordinator.compactionFailed(tabletCompactions);
      this.deadCompactions.keySet().removeAll(toFail);
    }

    // Find and delete any known tables that have unreferenced
    // compaction tmp files.
    if (!tablesWithUnreferencedTmpFiles.isEmpty()) {

      Set<TableId> copy = new HashSet<>();
      synchronized (tablesWithUnreferencedTmpFiles) {
        copy.addAll(tablesWithUnreferencedTmpFiles);
        tablesWithUnreferencedTmpFiles.clear();
      }

      log.debug("Tables that may have unreferenced compaction tmp files: {}", copy);
      for (TableId tid : copy) {
        try {
          final Set<Path> matches = FindCompactionTmpFiles.findTempFiles(context, tid.canonical());
          log.debug("Found the following compaction tmp files for table {}:", tid);
          matches.forEach(p -> log.debug("{}", p));

          if (!matches.isEmpty()) {
            log.debug("Deleting compaction tmp files for table {}...", tid);
            DeleteStats stats = FindCompactionTmpFiles.deleteTempFiles(context, matches);
            log.debug(
                "Deletion of compaction tmp files for table {} complete. Success:{}, Failure:{}, Error:{}",
                tid, stats.success, stats.failure, stats.error);
          }
        } catch (InterruptedException e) {
          log.error("Interrupted while finding compaction tmp files for table: {}", tid.canonical(),
              e);
        }
      }
    }

  }

  public void start() {
    long interval = this.context.getConfiguration()
        .getTimeInMillis(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL);

    ThreadPools.watchCriticalScheduledTask(schedExecutor.scheduleWithFixedDelay(() -> {
      try {
        detectDeadCompactions();
      } catch (RuntimeException e) {
        log.warn("Failed to look for dead compactions", e);
      }
    }, 0, interval, TimeUnit.MILLISECONDS));
  }
}
