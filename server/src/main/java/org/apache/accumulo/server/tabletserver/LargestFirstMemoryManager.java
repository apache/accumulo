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
package org.apache.accumulo.server.tabletserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class LargestFirstMemoryManager implements MemoryManager {
  
  private static final Logger log = Logger.getLogger(LargestFirstMemoryManager.class);
  
  private long maxMemory = -1;
  private int maxConcurrentMincs;
  private int numWaitingMultiplier;
  private long prevIngestMemory;
  private double compactionThreshold;
  private long maxObserved;
  private HashMap<Text,Long> mincIdleThresholds;
  private static final long zerotime = System.currentTimeMillis();
  private ServerConfiguration config = null;
  
  LargestFirstMemoryManager(long maxMemory, int maxConcurrentMincs, int numWaitingMultiplier) {
    this();
    this.maxMemory = maxMemory;
    this.maxConcurrentMincs = maxConcurrentMincs;
    this.numWaitingMultiplier = numWaitingMultiplier;
  }
  
  public void init(ServerConfiguration conf) {
    this.config = conf;
    maxMemory = conf.getConfiguration().getMemoryInBytes(Property.TSERV_MAXMEM);
    maxConcurrentMincs = conf.getConfiguration().getCount(Property.TSERV_MINC_MAXCONCURRENT);
    numWaitingMultiplier = Constants.TSERV_MINC_MAXCONCURRENT_NUMWAITING_MULTIPLIER;
  }
  
  LargestFirstMemoryManager() {
    prevIngestMemory = 0;
    compactionThreshold = 0.5;
    maxObserved = 0;
    mincIdleThresholds = new HashMap<Text,Long>();
  }
  
  @Override
  public MemoryManagementActions getMemoryManagementActions(List<TabletState> tablets) {
    if (maxMemory < 0)
      throw new IllegalStateException("need to initialize Largst");
    mincIdleThresholds.clear();
    long ingestMemory = 0;
    long compactionMemory = 0;
    KeyExtent largestMemTablet = null;
    long largestMemTableLoad = 0;
    KeyExtent largestIdleMemTablet = null;
    long largestIdleMemTableLoad = 0;
    long mts;
    long mcmts;
    int numWaitingMincs = 0;
    long idleTime;
    long tml;
    long ct = System.currentTimeMillis();
    
    long largestMemTableIdleTime = -1, largestMemTableSize = -1;
    long largestIdleMemTableIdleTime = -1, largestIdleMemTableSize = -1;
    
    for (TabletState ts : tablets) {
      mts = ts.getMemTableSize();
      mcmts = ts.getMinorCompactingMemTableSize();
      if (ts.getLastCommitTime() > 0)
        idleTime = ct - ts.getLastCommitTime();
      else
        idleTime = ct - zerotime;
      ingestMemory += mts;
      tml = timeMemoryLoad(mts, idleTime);
      if (mcmts == 0 && mts > 0) {
        if (tml > largestMemTableLoad) {
          largestMemTableLoad = tml;
          largestMemTablet = ts.getExtent();
          largestMemTableSize = mts;
          largestMemTableIdleTime = idleTime;
        }
        Text tableId = ts.getExtent().getTableId();
        if (!mincIdleThresholds.containsKey(tableId))
          mincIdleThresholds.put(tableId, config.getTableConfiguration(tableId.toString()).getTimeInMillis(Property.TABLE_MINC_COMPACT_IDLETIME));
        if (idleTime > mincIdleThresholds.get(tableId) && tml > largestIdleMemTableLoad) {
          largestIdleMemTableLoad = tml;
          largestIdleMemTablet = ts.getExtent();
          largestIdleMemTableSize = mts;
          largestIdleMemTableIdleTime = idleTime;
        }
        // log.debug("extent: "+ts.getExtent()+" idle threshold: "+mincIdleThresholds.get(tableId)+" idle time: "+idleTime+" memtable: "+mts+" compacting: "+mcmts);
      }
      // else {
      // log.debug("skipping extent "+ts.getExtent()+", nothing in memory");
      // }
      
      compactionMemory += mcmts;
      if (mcmts > 0)
        numWaitingMincs++;
    }
    
    if (ingestMemory + compactionMemory > maxObserved) {
      maxObserved = ingestMemory + compactionMemory;
    }
    
    long memoryChange = ingestMemory - prevIngestMemory;
    prevIngestMemory = ingestMemory;
    
    MemoryManagementActions mma = new MemoryManagementActions();
    mma.tabletsToMinorCompact = new ArrayList<KeyExtent>();
    
    boolean startMinC = false;
    
    if (numWaitingMincs < maxConcurrentMincs * numWaitingMultiplier) {
      // based on previous ingest memory increase, if we think that the next increase will
      // take us over the threshold for non-compacting memory, then start a minor compaction
      // or if the idle time of the chosen tablet is greater than the threshold, start a minor compaction
      if (memoryChange >= 0 && ingestMemory + memoryChange > compactionThreshold * maxMemory) {
        startMinC = true;
      } else if (largestIdleMemTablet != null) {
        startMinC = true;
        // switch largestMemTablet to largestIdleMemTablet
        largestMemTablet = largestIdleMemTablet;
        largestMemTableLoad = largestIdleMemTableLoad;
        largestMemTableSize = largestIdleMemTableSize;
        largestMemTableIdleTime = largestIdleMemTableIdleTime;
        log.debug("IDLE minor compaction chosen");
      }
    }
    
    if (startMinC && largestMemTablet != null) {
      mma.tabletsToMinorCompact.add(largestMemTablet);
      log.debug(String.format("COMPACTING %s  total = %,d ingestMemory = %,d", largestMemTablet.toString(), (ingestMemory + compactionMemory), ingestMemory));
      log.debug(String.format("chosenMem = %,d chosenIT = %.2f load %,d", largestMemTableSize, largestMemTableIdleTime / 1000.0, largestMemTableLoad));
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
      
      log.debug(String.format("BEFORE compactionThreshold = %.3f maxObserved = %,d", compactionThreshold, maxObserved));
      
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
    
    return mma;
  }
  
  @Override
  public void tabletClosed(KeyExtent extent) {}
  
  static long timeMemoryLoad(long mem, long time) {
    double minutesIdle = time / 60000.0;
    
    return (long) (mem * Math.pow(2, minutesIdle / 15.0));
  }
  
  public static void main(String[] args) {
    for (int i = 0; i < 62; i++) {
      System.out.printf("%d\t%d\n", i, timeMemoryLoad(1, i * 60000l));
    }
  }
}
