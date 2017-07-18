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
package org.apache.accumulo.master.state;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.server.master.state.TabletState;

public class TableStats {
  private Map<Table.ID,TableCounts> last = new HashMap<>();
  private Map<Table.ID,TableCounts> next;
  private long startScan = 0;
  private long endScan = 0;
  private MasterState state;

  public synchronized void begin() {
    next = new HashMap<>();
    startScan = System.currentTimeMillis();
  }

  public synchronized void update(Table.ID tableId, TabletState state) {
    TableCounts counts = next.get(tableId);
    if (counts == null) {
      counts = new TableCounts();
      next.put(tableId, counts);
    }
    counts.counts[state.ordinal()]++;
  }

  public synchronized void end(MasterState state) {
    last = next;
    next = null;
    endScan = System.currentTimeMillis();
    this.state = state;
  }

  public synchronized Map<Table.ID,TableCounts> getLast() {
    return last;
  }

  public synchronized MasterState getLastMasterState() {
    return state;
  }

  public synchronized TableCounts getLast(Table.ID tableId) {
    TableCounts result = last.get(tableId);
    if (result == null)
      return new TableCounts();
    return result;
  }

  public synchronized long getScanTime() {
    if (endScan <= startScan)
      return System.currentTimeMillis() - startScan;
    return endScan - startScan;
  }

  public synchronized long lastScanFinished() {
    return endScan;
  }
}
