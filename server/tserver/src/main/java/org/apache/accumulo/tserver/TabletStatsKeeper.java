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
package org.apache.accumulo.tserver;

import org.apache.accumulo.core.tabletserver.thrift.ActionStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.util.ActionStatsUpdator;

public class TabletStatsKeeper {

  private ActionStats major = new ActionStats();
  private ActionStats minor = new ActionStats();
  private ActionStats split = new ActionStats();

  public enum Operation {
    MAJOR, SPLIT, MINOR
  }

  private ActionStats[] map = new ActionStats[] {major, split, minor};

  public void updateTime(Operation operation, long queued, long start, long count, boolean failed) {
    try {
      ActionStats data = map[operation.ordinal()];
      if (failed) {
        data.fail++;
        data.status--;
      } else {
        double t = (System.currentTimeMillis() - start) / 1000.0;
        double q = (start - queued) / 1000.0;

        data.status--;
        data.count += count;
        data.num++;
        data.elapsed += t;
        data.queueTime += q;
        data.sumDev += t * t;
        data.queueSumDev += q * q;
        if (data.elapsed < 0 || data.sumDev < 0 || data.queueSumDev < 0 || data.queueTime < 0)
          resetTimes();
      }
    } catch (Exception E) {
      resetTimes();
    }

  }

  public void updateTime(Operation operation, long start, long count, boolean failed) {
    try {
      ActionStats data = map[operation.ordinal()];
      if (failed) {
        data.fail++;
        data.status--;
      } else {
        double t = (System.currentTimeMillis() - start) / 1000.0;

        data.status--;
        data.num++;
        data.elapsed += t;
        data.sumDev += t * t;

        if (data.elapsed < 0 || data.sumDev < 0 || data.queueSumDev < 0 || data.queueTime < 0)
          resetTimes();
      }
    } catch (Exception E) {
      resetTimes();
    }

  }

  public void saveMinorTimes(TabletStatsKeeper t) {
    ActionStatsUpdator.update(minor, t.minor);
  }

  public void saveMajorTimes(TabletStatsKeeper t) {
    ActionStatsUpdator.update(major, t.major);
  }

  public void resetTimes() {
    major = new ActionStats();
    split = new ActionStats();
    minor = new ActionStats();
  }

  public void incrementStatusMinor() {
    minor.status++;
  }

  public void incrementStatusMajor() {
    major.status++;
  }

  public void incrementStatusSplit() {
    split.status++;
  }

  public TabletStats getTabletStats() {
    return new TabletStats(null, major, minor, split, 0, 0, 0, 0);
  }
}
