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
package org.apache.accumulo.tserver.metrics;

import java.util.Collection;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServerResourceManager.AssignmentWatcher;
import org.apache.accumulo.tserver.tablet.Tablet;

/**
 * Wrapper around extracting metrics from a TabletServer instance
 */
public class TabletServerMetricsUtil {

  private final TabletServer tserver;

  public TabletServerMetricsUtil(TabletServer tserver) {
    this.tserver = tserver;
  }

  public long getLongTabletAssignments() {
    return AssignmentWatcher.getLongAssignments();
  }

  interface TabletToLong {
    long apply(Tablet t);
  }

  private long sum(TableId tableId, TabletToLong tabletFunction) {
    Collection<Tablet> tablets = tableId == null ? tserver.getOnlineTablets().values()
        : tserver.getOnlineTablets(tableId).values();

    long result = 0;
    for (Tablet tablet : tablets) {
      result += tabletFunction.apply(tablet);
    }
    return result;
  }

  public long getEntries(TableId tableId) {
    return sum(tableId, Tablet::getNumEntries);
  }

  public long getEntriesInMemory(TableId tableId) {
    return sum(tableId, Tablet::getNumEntriesInMemory);
  }

  public double getIngestCount(TableId tableId) {
    return sum(tableId, Tablet::totalIngest);
  }

  public double getIngestByteCount(TableId tableId) {
    return sum(tableId, Tablet::totalIngestBytes);
  }

  public int getMinorCompactions(TableId tableId) {
    return (int) sum(tableId, tablet -> tablet.isMinorCompactionRunning() ? 1 : 0);
  }

  public int getMinorCompactionsQueued(TableId tableId) {
    return (int) sum(tableId, tablet -> tablet.isMinorCompactionQueued() ? 1 : 0);
  }

  public int getOnDemandOnlineCount(TableId tableId) {
    return (int) sum(tableId, tablet -> tablet.isOnDemand() ? 1 : 0);
  }

  public int getOnDemandUnloadedLowMem() {
    return tserver.getOnDemandOnlineUnloadedForLowMemory();
  }

  public int getOnlineCount() {
    return tserver.getOnlineTablets().size();
  }

  public int getOpeningCount() {
    return tserver.getOpeningCount();
  }

  public int getUnopenedCount() {
    return tserver.getUnopenedCount();
  }

  public long getTotalMinorCompactions() {
    return tserver.getTotalMinorCompactions();
  }

  public double getHoldTime() {
    return tserver.getHoldTimeMillis() / 1000.;
  }

  public double getAverageFilesPerTablet(TableId tableId) {
    Collection<Tablet> tablets = tableId == null ? tserver.getOnlineTablets().values()
        : tserver.getOnlineTablets(tableId).values();
    int count = 0;
    long result = 0;
    for (Tablet tablet : tablets) {
      result += tablet.getDatafiles().size();
      count++;
    }
    if (count == 0) {
      return 0;
    }
    return result / (double) count;
  }
}
