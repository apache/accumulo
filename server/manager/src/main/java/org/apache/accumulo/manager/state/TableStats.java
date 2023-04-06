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
package org.apache.accumulo.manager.state;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TabletState;

public class TableStats {
  private Map<TableId,TableCounts> last = new HashMap<>();
  private Map<TableId,TableCounts> next;
  private long startScan = 0;
  private long endScan = 0;
  private ManagerState state;

  public synchronized void begin() {
    next = new HashMap<>();
    startScan = System.currentTimeMillis();
  }

  public synchronized void update(TableId tableId, TabletState state) {
    TableCounts counts = next.get(tableId);
    if (counts == null) {
      counts = new TableCounts();
      next.put(tableId, counts);
    }
    counts.counts[state.ordinal()]++;
  }

  public synchronized void end(ManagerState state) {
    last = next;
    next = null;
    endScan = System.currentTimeMillis();
    this.state = state;
  }

  public synchronized Map<TableId,TableCounts> getLast() {
    return last;
  }

  public synchronized ManagerState getLastManagerState() {
    return state;
  }

  public synchronized TableCounts getLast(TableId tableId) {
    TableCounts result = last.get(tableId);
    if (result == null) {
      return new TableCounts();
    }
    return result;
  }

  public synchronized long getScanTime() {
    if (endScan <= startScan) {
      return System.currentTimeMillis() - startScan;
    }
    return endScan - startScan;
  }

  public synchronized long lastScanFinished() {
    return endScan;
  }
}
