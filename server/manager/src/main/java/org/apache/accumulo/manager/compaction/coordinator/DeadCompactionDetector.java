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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateClient;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.metadata.schema.Ample;
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
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadCompactionDetector {

  private static final Logger log = LoggerFactory.getLogger(DeadCompactionDetector.class);

  private final ServerContext context;
  private final ScheduledThreadPoolExecutor schedExecutor;
  private final Function<FateInstanceType,FateClient<FateEnv>> fateClients;

  public DeadCompactionDetector(ServerContext context, ScheduledThreadPoolExecutor stpe,
      Function<FateInstanceType,FateClient<FateEnv>> fateClients) {
    this.context = context;
    this.schedExecutor = stpe;
    this.fateClients = fateClients;
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
    log.trace("Starting to look for dead compactions");

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

    // Get the list of compaction entries that were removed from the metadata table by a split or
    // merge operation. Must get this data before getting the running set of compactions.
    List<Ample.OrphanedCompaction> orphanedCompactions;
    try (Stream<Ample.OrphanedCompaction> listing =
        context.getAmple().orphanedCompactions().list()) {
      orphanedCompactions = listing.collect(Collectors.toCollection(ArrayList::new));
    }

    // Must get the set of running compactions after reading compaction ids from the metadata table
    Set<ExternalCompactionId> running = null;
    if (!orphanedCompactions.isEmpty() || !tabletCompactions.isEmpty()) {
      running = ExternalCompactionUtil.getCompactionIdsRunningOnCompactors(context);
    }

    // Delete any tmp files related to compaction metadata entries that were removed by split or
    // merge and are no longer running.
    if (!orphanedCompactions.isEmpty()) {
      var runningSet = Objects.requireNonNull(running);
      orphanedCompactions.removeIf(rc -> runningSet.contains(rc.id()));
      Set<Path> tmpFilesToDelete = new HashSet<>();
      orphanedCompactions.forEach(rc -> {
        log.trace("attempting to find tmp files for removed compaction {}", rc);
        FindCompactionTmpFiles.findTmpFiles(context, rc.table(), rc.dir(), Set.of(rc.id()),
            tmpFilesToDelete::add);
      });
      FindCompactionTmpFiles.deleteTempFiles(context, tmpFilesToDelete);
      context.getAmple().orphanedCompactions().delete(orphanedCompactions);
    }

    if (!tabletCompactions.isEmpty()) {
      log.trace("Read {} tablet compactions into memory from metadata table",
          tabletCompactions.size());
      if (log.isTraceEnabled()) {
        tabletCompactions.forEach((ecid, extent) -> log.trace("Saw {} for {}", ecid, extent));
      }

      // Determine what compactions are currently running and remove those.
      //
      // In order for this overall algorithm to be correct and avoid race conditions, the compactor
      // must return ids covering the time period from before reservation until after commit. If the
      // ids do not cover this time period then legitimate running compactions could be canceled.
      running.forEach(ecid -> {
        if (tabletCompactions.remove(ecid) != null) {
          log.trace("Ignoring compaction {} that is running on a compactor", ecid);
        }
      });

      if (!tabletCompactions.isEmpty()) {
        // look for any compactions committing in fate and remove those
        try (Stream<FateKey> keyStream = Arrays.stream(FateInstanceType.values()).map(fateClients)
            .flatMap(fateClient -> fateClient.list(FateKey.FateKeyType.COMPACTION_COMMIT))) {
          keyStream.map(fateKey -> fateKey.getCompactionId().orElseThrow()).forEach(ecid -> {
            if (tabletCompactions.remove(ecid) != null) {
              log.trace("Ignoring compaction {} that is committing in a fate", ecid);
            }
          });
        }
      }

      // Everything left in tabletCompactions is no longer running anywhere and should be failed.
      // Its possible that a compaction committed while going through the steps above, if so then
      // that is ok and marking it failed will end up being a no-op.
      tabletCompactions.forEach((ecid, extent) -> {
        log.info("Compaction believed to be dead, failing it: id: {}, extent: {}", ecid, extent);
      });
      CompactionCoordinator.compactionsFailed(context, tabletCompactions);
      log.trace("Finished looking for dead compactions");
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
