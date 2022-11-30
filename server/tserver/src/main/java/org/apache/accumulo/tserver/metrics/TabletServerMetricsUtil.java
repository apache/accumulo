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

import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServerResourceManager.AssignmentWatcher;
import org.apache.accumulo.tserver.tablet.Tablet;

/**
 * Wrapper around extracting metrics from a TabletServer instance
 *
 * Necessary to support both old custom JMX metrics and Hadoop Metrics2
 */
public class TabletServerMetricsUtil {

  private final TabletServer tserver;

  public TabletServerMetricsUtil(TabletServer tserver) {
    this.tserver = tserver;
  }

  public long getLongTabletAssignments() {
    return AssignmentWatcher.getLongAssignments();
  }

  public long getEntries() {
    long result = 0;
    for (Tablet tablet : tserver.getOnlineTablets().values()) {
      result += tablet.getNumEntries();
    }
    return result;
  }

  public long getEntriesInMemory() {
    long result = 0;
    for (Tablet tablet : tserver.getOnlineTablets().values()) {
      result += tablet.getNumEntriesInMemory();
    }
    return result;
  }

  public double getIngestCount() {
    double result = 0;
    for (Tablet tablet : tserver.getOnlineTablets().values()) {
      result += tablet.totalIngest();
    }
    return result;
  }

  public double getIngestByteCount() {
    double result = 0;
    for (Tablet tablet : tserver.getOnlineTablets().values()) {
      result += tablet.totalIngestBytes();
    }
    return result;
  }

  public int getMajorCompactions() {
    var mgr = tserver.getCompactionManager();
    return mgr == null ? 0 : mgr.getCompactionsRunning();
  }

  public int getMajorCompactionsQueued() {
    var mgr = tserver.getCompactionManager();
    return mgr == null ? 0 : mgr.getCompactionsQueued();
  }

  public int getMinorCompactions() {
    int result = 0;
    for (Tablet tablet : tserver.getOnlineTablets().values()) {
      if (tablet.isMinorCompactionRunning()) {
        result++;
      }
    }
    return result;
  }

  public int getMinorCompactionsQueued() {
    int result = 0;
    for (Tablet tablet : tserver.getOnlineTablets().values()) {
      if (tablet.isMinorCompactionQueued()) {
        result++;
      }
    }
    return result;
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

  public String getName() {
    return tserver.getClientAddressString();
  }

  public long getTotalMinorCompactions() {
    return tserver.getTotalMinorCompactions();
  }

  public double getHoldTime() {
    return tserver.getHoldTimeMillis() / 1000.;
  }

  public double getAverageFilesPerTablet() {
    int count = 0;
    long result = 0;
    for (Tablet tablet : tserver.getOnlineTablets().values()) {
      result += tablet.getDatafiles().size();
      count++;
    }
    if (count == 0) {
      return 0;
    }
    return result / (double) count;
  }
}
