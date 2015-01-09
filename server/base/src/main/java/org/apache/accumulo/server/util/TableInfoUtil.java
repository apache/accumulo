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
package org.apache.accumulo.server.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;

/**
 *
 */
public class TableInfoUtil {

  public static void add(TableInfo total, TableInfo more) {
    if (total.minors == null)
      total.minors = new Compacting();
    if (total.majors == null)
      total.majors = new Compacting();
    if (total.scans == null)
      total.scans = new Compacting();
    if (more.minors != null) {
      total.minors.running += more.minors.running;
      total.minors.queued += more.minors.queued;
    }
    if (more.majors != null) {
      total.majors.running += more.majors.running;
      total.majors.queued += more.majors.queued;
    }
    if (more.scans != null) {
      total.scans.running += more.scans.running;
      total.scans.queued += more.scans.queued;
    }
    total.onlineTablets += more.onlineTablets;
    total.recs += more.recs;
    total.recsInMemory += more.recsInMemory;
    total.tablets += more.tablets;
    total.ingestRate += more.ingestRate;
    total.ingestByteRate += more.ingestByteRate;
    total.queryRate += more.queryRate;
    total.queryByteRate += more.queryByteRate;
    total.scanRate += more.scanRate;
  }

  public static TableInfo summarizeTableStats(TabletServerStatus status) {
    TableInfo summary = new TableInfo();
    summary.majors = new Compacting();
    summary.minors = new Compacting();
    summary.scans = new Compacting();
    for (TableInfo rates : status.tableMap.values()) {
      TableInfoUtil.add(summary, rates);
    }
    return summary;
  }

  public static Map<String,Double> summarizeTableStats(MasterMonitorInfo mmi) {
    Map<String,Double> compactingByTable = new HashMap<String,Double>();
    if (mmi != null && mmi.tServerInfo != null) {
      for (TabletServerStatus status : mmi.tServerInfo) {
        if (status != null && status.tableMap != null) {
          for (String table : status.tableMap.keySet()) {
            Double holdTime = compactingByTable.get(table);
            compactingByTable.put(table, Math.max(holdTime == null ? 0. : holdTime.doubleValue(), status.holdTime));
          }
        }
      }
    }
    return compactingByTable;
  }

}
