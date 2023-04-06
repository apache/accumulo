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
package org.apache.accumulo.tserver.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LargestFirstMemoryManager attempts to keep memory between 80% and 90% full. It adapts over
 * time the point at which it should start a compaction based on how full memory gets between
 * successive calls. It will also flush idle tablets based on a per-table configurable idle time. It
 * will only attempt to flush tablets up to 20% of all memory. And, as the name of the class would
 * suggest, it flushes the tablet with the highest memory footprint. However, it actually chooses
 * the tablet as a function of its size doubled for every 15 minutes of idle time.
 */
public class LargestFirstMemoryManager {

  private static final Logger log = LoggerFactory.getLogger(LargestFirstMemoryManager.class);
  static final long ZERO_TIME = System.currentTimeMillis();
  private static final int TSERV_MINC_MAXCONCURRENT_NUMWAITING_MULTIPLIER = 2;
  private static final double MAX_FLUSH_AT_ONCE_PERCENT = 0.20;

  private long maxMemory = -1;
  private int maxConcurrentMincs;
  private int numWaitingMultiplier;
  private long prevIngestMemory;
  // The fraction of memory that needs to be used before we begin flushing.
  private double compactionThreshold;
  private long maxObserved;
  private final HashMap<TableId,Long> mincIdleThresholds = new HashMap<>();
  private ServerContext context = null;

  private static class TabletInfo {
    final KeyExtent extent;
    final long memTableSize;
    final long idleTime;
    final long load;

    public TabletInfo(KeyExtent extent, long memTableSize, long idleTime, long load) {
      this.extent = extent;
      this.memTableSize = memTableSize;
      this.idleTime = idleTime;
      this.load = load;
    }
  }

  // A little map that will hold the "largest" N tablets, where largest is a result of the
  // timeMemoryLoad function
  private static class LargestMap {
    final int max;
    final TreeMap<Long,List<TabletInfo>> map = new TreeMap<>();

    LargestMap(int n) {
      max = n;
    }

    public boolean put(Long key, TabletInfo value) {
      if (map.size() == max) {
        if (key.compareTo(map.firstKey()) < 0) {
          return false;
        }
        try {
          add(key, value);
          return true;
        } finally {
          map.remove(map.firstKey());
        }
      } else {
        add(key, value);
        return true;
      }
    }

    private void add(Long key, TabletInfo value) {
      List<TabletInfo> lst = map.get(key);
      if (lst != null) {
        lst.add(value);
      } else {
        lst = new ArrayList<>();
        lst.add(value);
        map.put(key, lst);
      }
    }

    public boolean isEmpty() {
      return map.isEmpty();
    }

    public Entry<Long,List<TabletInfo>> lastEntry() {
      return map.lastEntry();
    }

    public void remove(Long key) {
      map.remove(key);
    }
  }

  public void init(ServerContext context) {
    this.context = context;
    maxMemory = context.getConfiguration().getAsBytes(Property.TSERV_MAXMEM);
    maxConcurrentMincs = context.getConfiguration().getCount(Property.TSERV_MINC_MAXCONCURRENT);
    numWaitingMultiplier = TSERV_MINC_MAXCONCURRENT_NUMWAITING_MULTIPLIER;
  }

  public LargestFirstMemoryManager() {
    prevIngestMemory = 0;
    compactionThreshold = 0.5;
    maxObserved = 0;
  }

  protected long getMinCIdleThreshold(KeyExtent extent) {
    TableId tableId = extent.tableId();
    if (!mincIdleThresholds.containsKey(tableId)) {
      mincIdleThresholds.put(tableId, context.getTableConfiguration(tableId)
          .getTimeInMillis(Property.TABLE_MINC_COMPACT_IDLETIME));
    }
    return mincIdleThresholds.get(tableId);
  }

  protected boolean tableExists(TableId tableId) {
    // make sure that the table still exists by checking if it has a configuration
    return context.getTableConfiguration(tableId) != null;
  }

  protected boolean tableBeingDeleted(TableId tableId) {
    return context.getTableManager().getTableState(tableId) == TableState.DELETING;
  }

  public List<KeyExtent> tabletsToMinorCompact(List<TabletMemoryReport> tablets) {
    if (maxMemory < 0) {
      throw new IllegalStateException(
          "need to initialize " + LargestFirstMemoryManager.class.getName());
    }

    final int maxMinCs = maxConcurrentMincs * numWaitingMultiplier;

    mincIdleThresholds.clear();
    final List<KeyExtent> tabletsToMinorCompact = new ArrayList<>();

    LargestMap largestMemTablets = new LargestMap(maxMinCs);
    final LargestMap largestIdleMemTablets = new LargestMap(maxConcurrentMincs);
    final long now = currentTimeMillis();

    long ingestMemory = 0;
    long compactionMemory = 0;
    int numWaitingMincs = 0;

    // find the largest and most idle tablets
    for (TabletMemoryReport ts : tablets) {
      KeyExtent tablet = ts.getExtent();
      // Make sure that the table still exists
      if (!tableExists(tablet.tableId()) || tableBeingDeleted(tablet.tableId())) {
        log.trace("Ignoring extent for deleted table: {}", tablet);
        continue;
      }

      final long memTabletSize = ts.getMemTableSize();
      final long minorCompactingSize = ts.getMinorCompactingMemTableSize();
      final long idleTime = now - Math.max(ts.getLastCommitTime(), ZERO_TIME);
      final long timeMemoryLoad = timeMemoryLoad(memTabletSize, idleTime);
      ingestMemory += memTabletSize;
      if (minorCompactingSize == 0 && memTabletSize > 0) {
        TabletInfo tabletInfo = new TabletInfo(tablet, memTabletSize, idleTime, timeMemoryLoad);
        try {
          // If the table was deleted, getMinCIdleThreshold will throw an exception
          if (idleTime > getMinCIdleThreshold(tablet)) {
            largestIdleMemTablets.put(timeMemoryLoad, tabletInfo);
          }
        } catch (IllegalArgumentException e) {
          Throwable cause = e.getCause();
          if (cause != null && cause instanceof TableNotFoundException) {
            log.trace("Ignoring extent for deleted table: {}", tablet);

            // The table might have been deleted during the iteration of the tablets
            // We just want to eat this exception, do nothing with this tablet, and continue
            continue;
          }

          throw e;
        }
        // Only place the tablet into largestMemTablets map when the table still exists
        largestMemTablets.put(timeMemoryLoad, tabletInfo);
      }

      compactionMemory += minorCompactingSize;
      if (minorCompactingSize > 0) {
        numWaitingMincs++;
      }
    }

    if (ingestMemory + compactionMemory > maxObserved) {
      maxObserved = ingestMemory + compactionMemory;
    }

    final long memoryChange = ingestMemory - prevIngestMemory;
    prevIngestMemory = ingestMemory;

    boolean startMinC = false;

    if (numWaitingMincs < maxMinCs) {
      // based on previous ingest memory increase, if we think that the next increase will
      // take us over the threshold for non-compacting memory, then start a minor compaction
      // or if the idle time of the chosen tablet is greater than the threshold, start a minor
      // compaction
      if (memoryChange >= 0 && ingestMemory + memoryChange > compactionThreshold * maxMemory) {
        startMinC = true;
      } else if (!largestIdleMemTablets.isEmpty()) {
        startMinC = true;
        // switch largestMemTablets to largestIdleMemTablets
        largestMemTablets = largestIdleMemTablets;
        log.debug("IDLE minor compaction chosen");
      }
    }

    if (startMinC) {
      long toBeCompacted = compactionMemory;
      outer: for (int i = numWaitingMincs;
          i < maxMinCs && !largestMemTablets.isEmpty(); /* empty */) {
        Entry<Long,List<TabletInfo>> lastEntry = largestMemTablets.lastEntry();
        for (TabletInfo largest : lastEntry.getValue()) {
          toBeCompacted += largest.memTableSize;
          tabletsToMinorCompact.add(largest.extent);
          log.debug(String.format("COMPACTING %s  total = %,d ingestMemory = %,d",
              largest.extent.toString(), (ingestMemory + compactionMemory), ingestMemory));
          log.debug(String.format("chosenMem = %,d chosenIT = %.2f load %,d", largest.memTableSize,
              largest.idleTime / 1000.0, largest.load));
          if (toBeCompacted > ingestMemory * MAX_FLUSH_AT_ONCE_PERCENT) {
            break outer;
          }
          i++;
        }
        largestMemTablets.remove(lastEntry.getKey());
      }
    } else if (memoryChange < 0) {
      // before idle mincs, starting a minor compaction meant that memoryChange >= 0.
      // we thought we might want to remove the "else" if that changed,
      // however it seems performing idle compactions shouldn't make the threshold
      // change more often, so it is staying for now.
      // also, now we have the case where memoryChange < 0 due to an idle compaction, yet
      // we are still adjusting the threshold. should this be tracked and prevented?

      // memory change < 0 means a minor compaction occurred
      // we want to see how full the memory got during the compaction
      // (the goal is for it to have between 80% and 90% memory utilization)
      // and adjust the compactionThreshold accordingly

      log.debug(String.format("BEFORE compactionThreshold = %.3f maxObserved = %,d",
          compactionThreshold, maxObserved));
      if (compactionThreshold < 0.82 && maxObserved < 0.8 * maxMemory) {
        // 0.82 * 1.1 is about 0.9, which is our desired max threshold
        compactionThreshold *= 1.1;
      } else if (compactionThreshold > 0.056 && maxObserved > 0.9 * maxMemory) {
        // 0.056 * 0.9 is about 0.05, which is our desired min threshold
        compactionThreshold *= 0.9;
      }
      maxObserved = 0;

      log.debug(String.format("AFTER compactionThreshold = %.3f", compactionThreshold));
    }

    return tabletsToMinorCompact;
  }

  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  // The load function: memory times the idle time, doubling every 15 mins
  static long timeMemoryLoad(long mem, long time) {
    double minutesIdle = time / 60000.0;

    return (long) (mem * Math.pow(2, minutesIdle / 15.0));
  }
}
