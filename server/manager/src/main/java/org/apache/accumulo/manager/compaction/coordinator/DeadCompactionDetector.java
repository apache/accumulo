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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateClient;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metadata.schema.filters.HasExternalCompactionsFilter;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.tableOps.FateEnv;
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
  private final Function<FateInstanceType,FateClient<FateEnv>> fateClients;

  public DeadCompactionDetector(ServerContext context, CompactionCoordinator coordinator,
      ScheduledThreadPoolExecutor stpe,
      Function<FateInstanceType,FateClient<FateEnv>> fateClients) {
    this.context = context;
    this.coordinator = coordinator;
    this.schedExecutor = stpe;
    this.deadCompactions = new ConcurrentHashMap<>();
    this.fateClients = fateClients;
  }

  public void addTableId(TableId tableWithUnreferencedTmpFiles) {
    synchronized (tablesWithUnreferencedTmpFiles) {
      tablesWithUnreferencedTmpFiles.add(tableWithUnreferencedTmpFiles);
    }
  }

  private void detectDeadCompactions() {

    /*
     * The order of obtaining information is very important to avoid race conditions. This algorithm
     * ask compactors for information about what they are running. Compactors do the following.
     *
     * 1. Generate a compaction UUID.
     *
     * 2. Set the UUID as what they are currently working. This is reported to any other process
     * that ask, like this dead compaction detection code.
     *
     * 3. Request work from the coordinator under the UUID. The coordinator will use this UUID to
     * create a compaction entry in the metadata table.
     *
     * 4. Run the compaction
     *
     * 5. Ask the coordinator to commit the compaction. The coordinator will seed the fate operation
     * that commits the compaction.
     *
     * 6. Clear the UUID they are currently working on.
     *
     * Given the fact that compactors report they are running a UUID until after its been seeded in
     * fate, we can deduce the following for compactions that succeed.
     *
     * - There is time range from T1 to T2 where only the compactor will report a UUID.
     *
     * - There is a time range T2 to T3 where compactor and fate will report a UUID.
     *
     * - There is a time range T3 to T4 where only fate will report a UUID
     *
     * - After time T4 the compaction is complete and nothing will report the UUID
     *
     * This algorithm does the following.
     *
     * 1. Scan the metadata table looking for compaction UUIDs
     *
     * 2. Ask compactors what they are running
     *
     * 3. Ask Fate what compactions its committing.
     *
     * 4. Consider anything it saw in the metadata table that compactors or fate did not report as a
     * possible dead compaction.
     *
     * When we see a compaction id in the metadata table, then we know we are already at time
     * greater than T1 because the compactor generates and advertises ids prior to placing them in
     * the metadata table.
     *
     * If this process ask a compactor if it's running a compaction uuid and it says yes, then that
     * implies we are in the time range T1 to T3.
     *
     * If this process ask a compactor if it's running a compaction uuid and it says no, then that
     * implies we are in the time range >T3 defined above. So if the compaction is still active then
     * it will be reported by fate. If the time is >T4, then the compaction is finished and not
     * dead.
     *
     * If a time gap existed between when a compactor reported and when fate reported, then it could
     * result in false positives for dead compaction detection. If fate was queried before
     * compactors, then it could result in false positives. If compactors were queried before the
     * metadata table, then it could cause false positives.
     */
    log.trace("Starting to look for dead compactions, deadCompactions.size():{}",
        deadCompactions.size());

    Map<ExternalCompactionId,KeyExtent> tabletCompactions = new HashMap<>();

    // find what external compactions tablets think are running
    try (Stream<TabletMetadata> tabletsMetadata = Stream
        // Listing the data levels vs using DataLevel.values() prevents unexpected
        // behavior if a new DataLevel is added
        .of(DataLevel.ROOT, DataLevel.METADATA, DataLevel.USER)
        .map(dataLevel -> context.getAmple().readTablets().forLevel(dataLevel)
            .filter(new HasExternalCompactionsFilter()).fetch(ColumnType.ECOMP, ColumnType.PREV_ROW)
            .build())
        .flatMap(TabletsMetadata::stream)) {
      tabletsMetadata.forEach(tm -> {
        tm.getExternalCompactions().keySet().forEach(ecid -> {
          tabletCompactions.put(ecid, tm.getExtent());
        });
      });
    }

    if (tabletCompactions.isEmpty()) {
      // Clear out dead compactions, tservers don't think anything is running
      log.trace("Clearing the dead compaction map, no tablets have compactions running");
      this.deadCompactions.clear();
      // no need to look for dead compactions when tablets don't have anything recorded as running
    } else {
      log.trace("Read {} tablet compactions into memory from metadata table",
          tabletCompactions.size());
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

      running.forEach(ecid -> {
        if (tabletCompactions.remove(ecid) != null) {
          log.debug("Ignoring compaction {} that is running on a compactor", ecid);
        }
        if (this.deadCompactions.remove(ecid) != null) {
          log.debug("Removed {} from the dead compaction map, it's running on a compactor", ecid);
        }
      });

      if (!tabletCompactions.isEmpty()) {
        // look for any compactions committing in fate and remove those
        try (Stream<FateKey> keyStream = Arrays.stream(FateInstanceType.values()).map(fateClients)
            .flatMap(fateClient -> fateClient.list(FateKey.FateKeyType.COMPACTION_COMMIT))) {
          keyStream.map(fateKey -> fateKey.getCompactionId().orElseThrow()).forEach(ecid -> {
            if (tabletCompactions.remove(ecid) != null) {
              log.debug("Ignoring compaction {} that is committing in a fate", ecid);
            }
            if (this.deadCompactions.remove(ecid) != null) {
              log.debug("Removed {} from the dead compaction map, it's committing in fate", ecid);
            }
          });
        }
      }

      log.trace("deadCompactions.size() after removals {}", deadCompactions.size());
      tabletCompactions.forEach((ecid, extent) -> {
        log.info("Possible dead compaction detected {} {}", ecid, extent);
        this.deadCompactions.merge(ecid, 1L, Long::sum);
      });
      log.trace("deadCompactions.size() after additions {}", deadCompactions.size());

      // Everything left in tabletCompactions is no longer running anywhere and should be failed.
      // Its possible that a compaction committed while going through the steps above, if so then
      // that is ok and marking it failed will end up being a no-op.
      Set<ExternalCompactionId> toFail =
          this.deadCompactions.entrySet().stream().filter(e -> e.getValue() > 2)
              .map(e -> e.getKey()).collect(Collectors.toCollection(TreeSet::new));
      tabletCompactions.keySet().retainAll(toFail);
      tabletCompactions.forEach((ecid, extent) -> {
        log.warn("Compaction believed to be dead, failing it: id: {}, extent: {}", ecid, extent);
      });
      coordinator.compactionsFailed(tabletCompactions);
      this.deadCompactions.keySet().removeAll(toFail);
    }

    // Find and delete compaction tmp files that are unreferenced
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
