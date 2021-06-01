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
package org.apache.accumulo.coordinator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.compaction.thrift.UnknownCompactionIdException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadCompactionDetector {

  private static final Logger log = LoggerFactory.getLogger(DeadCompactionDetector.class);

  private final ServerContext context;
  private final CompactionCoordinator coordinator;
  private ScheduledThreadPoolExecutor schedExecutor;

  public DeadCompactionDetector(ServerContext context, CompactionCoordinator coordinator,
      ScheduledThreadPoolExecutor stpe) {
    this.context = context;
    this.coordinator = coordinator;
    this.schedExecutor = stpe;
  }

  private void detectDeadCompactions() {

    // The order of obtaining information is very important to avoid race conditions.

    log.trace("Starting to look for dead compactions");

    Map<ExternalCompactionId,KeyExtent> tabletCompactions = new HashMap<>();

    // find what external compactions tablets think are running
    context.getAmple().readTablets().forLevel(DataLevel.USER)
        .fetch(ColumnType.ECOMP, ColumnType.PREV_ROW).build().forEach(tm -> {
          tm.getExternalCompactions().keySet().forEach(ecid -> {
            tabletCompactions.put(ecid, tm.getExtent());
          });
        });

    if (tabletCompactions.isEmpty()) {
      // no need to look for dead compactions when tablets don't have anything recorded as running
      return;
    }

    if (log.isTraceEnabled()) {
      tabletCompactions.forEach((ecid, extent) -> log.trace("Saw {} for {}", ecid, extent));
    }

    // Determine what compactions are currently running and remove those.
    //
    // In order for this overall algorithm to be correct and avoid race conditions, the compactor
    // must return ids covering the time period from before reservation until after commit. If the
    // ids do not cover this time period then legitimate running compactions could be canceled.
    Collection<ExternalCompactionId> running =
        ExternalCompactionUtil.getCompactionIdsRunningOnCompactors(context);

    running.forEach((ecid) -> {
      if (tabletCompactions.remove(ecid) != null) {
        log.trace("Removed {} running on a compactor", ecid);
      }
    });

    // Determine which compactions are currently committing and remove those
    context.getAmple().getExternalCompactionFinalStates()
        .map(ecfs -> ecfs.getExternalCompactionId()).forEach(tabletCompactions::remove);

    tabletCompactions
        .forEach((ecid, extent) -> log.debug("Detected dead compaction {} {}", ecid, extent));

    // Everything left in tabletCompactions is no longer running anywhere and should be failed.
    // Its possible that a compaction committed while going through the steps above, if so then
    // that is ok and marking it failed will end up being a no-op.
    try {
      coordinator.compactionFailed(tabletCompactions);
    } catch (UnknownCompactionIdException e) {
      // One or more Ids was not in the Running compaction list. This is ok to ignore.
    }
  }

  public void start() {
    long interval = this.context.getConfiguration()
        .getTimeInMillis(Property.COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL);

    schedExecutor.scheduleWithFixedDelay(() -> {
      try {
        detectDeadCompactions();
      } catch (RuntimeException e) {
        log.warn("Failed to look for dead compactions", e);
      }
    }, 0, interval, TimeUnit.MILLISECONDS);
  }
}
