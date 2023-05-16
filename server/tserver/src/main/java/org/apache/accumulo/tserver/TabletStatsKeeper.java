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
package org.apache.accumulo.tserver;

import org.apache.accumulo.core.tabletserver.thrift.ActionStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.util.ActionStatsUpdator;

public class TabletStatsKeeper {

  // suspect we need more synchronization in this class
  private ActionStats major = new ActionStats();
  private ActionStats minor = new ActionStats();
  private ActionStats split = new ActionStats();

  public enum Operation {
    MAJOR, SPLIT, MINOR
  }

  private ActionStats[] map = {major, split, minor};

  public void updateTime(Operation operation, long queued, long start, long count, boolean failed) {
    try {
      ActionStats data = map[operation.ordinal()];
      if (failed) {
        data.setFail(data.getFail() + 1);
        data.setStatus(data.getStatus() - 1);
      } else {
        double t = (System.currentTimeMillis() - start) / 1000.0;
        double q = (start - queued) / 1000.0;

        data.setStatus(data.getStatus() - 1);
        data.setCount(data.getCount() + count);
        data.setNum(data.getNum() + 1);
        data.setElapsed(data.getElapsed() + t);
        data.setQueueTime(data.getQueueTime() + q);
        data.setSumDev(data.getSumDev() + t * t);
        data.setQueueSumDev(data.getQueueSumDev() + q * q);
        if (data.getElapsed() < 0 || data.getSumDev() < 0 || data.getQueueSumDev() < 0
            || data.getQueueTime() < 0) {
          resetTimes();
        }
      }
    } catch (Exception E) {
      resetTimes();
    }

  }

  public void updateTime(Operation operation, long start, boolean failed) {
    try {
      ActionStats data = map[operation.ordinal()];
      if (failed) {
        data.setFail(data.getFail() + 1);
        data.setStatus(data.getStatus() - 1);
      } else {
        double t = (System.currentTimeMillis() - start) / 1000.0;

        data.setStatus(data.getStatus() - 1);
        data.setNum(data.getNum() + 1);
        data.setElapsed(data.getElapsed() + t);
        data.setSumDev(data.getSumDev() + t * t);

        if (data.getElapsed() < 0 || data.getSumDev() < 0 || data.getQueueSumDev() < 0
            || data.getQueueTime() < 0) {
          resetTimes();
        }
      }
    } catch (Exception E) {
      resetTimes();
    }

  }

  public void saveMajorMinorTimes(TabletStats t) {
    ActionStatsUpdator.update(minor, t.getMinors());
    ActionStatsUpdator.update(major, t.getMajors());
  }

  private void resetTimes() {
    major = new ActionStats();
    split = new ActionStats();
    minor = new ActionStats();
  }

  public void incrementStatusMinor() {
    minor.setStatus(minor.getStatus() + 1);
  }

  public void incrementStatusMajor() {
    major.setStatus(major.getStatus() + 1);
  }

  void incrementStatusSplit() {
    split.setStatus(split.getStatus() + 1);
  }

  public TabletStats getTabletStats() {
    return new TabletStats(null, major, minor, split, 0, 0, 0, 0);
  }
}
