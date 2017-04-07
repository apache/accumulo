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
package org.apache.accumulo.tserver.metrics;

import org.apache.accumulo.tserver.TabletServer;
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

  public long getEntries() {
    long result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      result += tablet.getNumEntries();
    }
    return result;
  }

  public long getEntriesInMemory() {
    long result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      result += tablet.getNumEntriesInMemory();
    }
    return result;
  }

  public double getIngest() {
    double result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      result += tablet.ingestRate();
    }
    return result;
  }

  public int getMajorCompactions() {
    int result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      if (tablet.isMajorCompactionRunning())
        result++;
    }
    return result;
  }

  public int getMajorCompactionsQueued() {
    int result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      if (tablet.isMajorCompactionQueued())
        result++;
    }
    return result;
  }

  public int getMinorCompactions() {
    int result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      if (tablet.isMinorCompactionRunning())
        result++;
    }
    return result;
  }

  public int getMinorCompactionsQueued() {
    int result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      if (tablet.isMinorCompactionQueued())
        result++;
    }
    return result;
  }

  public int getOnlineCount() {
    return tserver.getOnlineTablets().size();
  }

  public int getOpeningCount() {
    return tserver.getOpeningCount();
  }

  public long getQueries() {
    long result = 0;
    for (Tablet tablet : tserver.getOnlineTablets()) {
      result += tablet.totalQueries();
    }
    return result;
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
    for (Tablet tablet : tserver.getOnlineTablets()) {
      result += tablet.getDatafiles().size();
      count++;
    }
    if (count == 0)
      return 0;
    return result / (double) count;
  }
}
