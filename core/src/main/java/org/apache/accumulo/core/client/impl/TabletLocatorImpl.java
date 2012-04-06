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
package org.apache.accumulo.core.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TabletLocatorImpl extends TabletLocator {
  
  private static final Logger log = Logger.getLogger(TabletLocatorImpl.class);
  
  // there seems to be a bug in TreeMap.tailMap related to
  // putting null in the treemap.. therefore instead of
  // putting null, put MAX_TEXT
  static final Text MAX_TEXT = new Text();
  
  private static class EndRowComparator implements Comparator<Text> {
    
    public int compare(Text o1, Text o2) {
      
      int ret;
      
      if (o1 == MAX_TEXT)
        if (o2 == MAX_TEXT)
          ret = 0;
        else
          ret = 1;
      else if (o2 == MAX_TEXT)
        ret = -1;
      else
        ret = o1.compareTo(o2);
      
      return ret;
    }
    
  }
  
  static final EndRowComparator endRowComparator = new EndRowComparator();
  
  protected Text tableId;
  protected TabletLocator parent;
  protected TreeMap<Text,TabletLocation> metaCache = new TreeMap<Text,TabletLocation>(endRowComparator);
  protected TabletLocationObtainer locationObtainer;
  protected Text lastTabletRow;
  
  private TreeSet<KeyExtent> badExtents = new TreeSet<KeyExtent>();
  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private Lock rLock = rwLock.readLock();
  private Lock wLock = rwLock.writeLock();
  
  public static interface TabletLocationObtainer {
    List<TabletLocation> lookupTablet(TabletLocation src, Text row, Text stopRow, TabletLocator parent) throws AccumuloSecurityException, AccumuloException;
    
    List<TabletLocation> lookupTablets(String tserver, Map<KeyExtent,List<Range>> map, TabletLocator parent) throws AccumuloSecurityException,
        AccumuloException;
  }
  
  public TabletLocatorImpl(Text table, TabletLocator parent, TabletLocationObtainer tlo) {
    this.tableId = table;
    this.parent = parent;
    this.locationObtainer = tlo;
    
    this.lastTabletRow = new Text(tableId);
    lastTabletRow.append(new byte[] {'<'}, 0, 1);
  }
  
  @Override
  public void binMutations(List<Mutation> mutations, Map<String,TabletServerMutations> binnedMutations, List<Mutation> failures) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    
    OpTimer opTimer = null;
    if (log.isTraceEnabled())
      opTimer = new OpTimer(log, Level.TRACE).start("Binning " + mutations.size() + " mutations for table " + tableId);
    
    ArrayList<Mutation> notInCache = new ArrayList<Mutation>();
    Text row = new Text();
    
    rLock.lock();
    try {
      processInvalidated();
      
      // for this to be efficient rows need to be in sorted order, but always sorting is slow... therefore only sort the
      // stuff not in the cache.... it is most efficient to pass _locateTablet rows in sorted order
      
      // For this to be efficient, need to avoid fine grained synchronization and fine grained logging.
      // Therefore methods called by this are not synchronized and should not log.
      
      for (Mutation mutation : mutations) {
        row.set(mutation.getRow());
        TabletLocation tl = locateTabletInCache(row);
        if (tl == null)
          notInCache.add(mutation);
        else
          addMutation(binnedMutations, mutation, tl);
        
      }
    } finally {
      rLock.unlock();
    }
    
    if (notInCache.size() > 0) {
      Collections.sort(notInCache, new Comparator<Mutation>() {
        public int compare(Mutation o1, Mutation o2) {
          return WritableComparator.compareBytes(o1.getRow(), 0, o1.getRow().length, o2.getRow(), 0, o2.getRow().length);
        }
      });
      
      wLock.lock();
      try {
        boolean failed = false;
        for (Mutation mutation : notInCache) {
          if (failed) {
            // when one table does not return a location, something is probably
            // screwy, go ahead and fail everything.
            failures.add(mutation);
            continue;
          }
          
          row.set(mutation.getRow());
          
          TabletLocation tl = _locateTablet(row, false, false, false);
          
          if (tl == null) {
            failures.add(mutation);
            failed = true;
          } else {
            addMutation(binnedMutations, mutation, tl);
          }
        }
      } finally {
        wLock.unlock();
      }
    }
    
    if (opTimer != null)
      opTimer.stop("Binned " + mutations.size() + " mutations for table " + tableId + " to " + binnedMutations.size() + " tservers in %DURATION%");
  }
  
  private void addMutation(Map<String,TabletServerMutations> binnedMutations, Mutation mutation, TabletLocation tl) {
    TabletServerMutations tsm = binnedMutations.get(tl.tablet_location);
    
    if (tsm == null) {
      tsm = new TabletServerMutations();
      binnedMutations.put(tl.tablet_location, tsm);
    }
    
    tsm.addMutation(tl.tablet_extent, mutation);
  }
  
  private List<Range> binRanges(List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges, boolean useCache) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    List<Range> failures = new ArrayList<Range>();
    List<TabletLocation> tabletLocations = new ArrayList<TabletLocation>();
    
    boolean lookupFailed = false;
    
    l1: for (Range range : ranges) {
      tabletLocations.clear();
      
      Text startRow;
      
      if (range.getStartKey() != null) {
        startRow = range.getStartKey().getRow();
      } else
        startRow = new Text();
      
      TabletLocation tl = null;
      
      if (useCache)
        tl = locateTabletInCache(startRow);
      else if (!lookupFailed)
        tl = _locateTablet(startRow, false, false, false);
      
      if (tl == null) {
        failures.add(range);
        if (!useCache)
          lookupFailed = true;
        continue;
      }
      
      tabletLocations.add(tl);
      
      while (tl.tablet_extent.getEndRow() != null && !range.afterEndKey(new Key(tl.tablet_extent.getEndRow()).followingKey(PartialKey.ROW))) {
        if (useCache) {
          Text row = new Text(tl.tablet_extent.getEndRow());
          row.append(new byte[] {0}, 0, 1);
          tl = locateTabletInCache(row);
        } else {
          tl = _locateTablet(tl.tablet_extent.getEndRow(), true, false, false);
        }
        
        if (tl == null) {
          failures.add(range);
          if (!useCache)
            lookupFailed = true;
          continue l1;
        }
        tabletLocations.add(tl);
      }
      
      for (TabletLocation tl2 : tabletLocations) {
        TabletLocatorImpl.addRange(binnedRanges, tl2.tablet_location, tl2.tablet_extent, range);
      }
      
    }
    
    return failures;
  }
  
  @Override
  public List<Range> binRanges(List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    
    /*
     * For this to be efficient, need to avoid fine grained synchronization and fine grained logging. Therefore methods called by this are not synchronized and
     * should not log.
     */
    
    OpTimer opTimer = null;
    if (log.isTraceEnabled())
      opTimer = new OpTimer(log, Level.TRACE).start("Binning " + ranges.size() + " ranges for table " + tableId);
    
    List<Range> failures;
    rLock.lock();
    try {
      processInvalidated();
      
      // for this to be optimal, need to look ranges up in sorted order when
      // ranges are not present in cache... however do not want to always
      // sort ranges... therefore try binning ranges using only the cache
      // and sort whatever fails and retry
      
      failures = binRanges(ranges, binnedRanges, true);
    } finally {
      rLock.unlock();
    }
    
    if (failures.size() > 0) {
      // sort failures by range start key
      Collections.sort(failures);
      
      // try lookups again
      wLock.lock();
      try {
        failures = binRanges(failures, binnedRanges, false);
      } finally {
        wLock.unlock();
      }
    }
    
    if (opTimer != null)
      opTimer.stop("Binned " + ranges.size() + " ranges for table " + tableId + " to " + binnedRanges.size() + " tservers in %DURATION%");
    
    return failures;
  }
  
  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    wLock.lock();
    try {
      badExtents.add(failedExtent);
    } finally {
      wLock.unlock();
    }
    if (log.isTraceEnabled())
      log.trace("Invalidated extent=" + failedExtent);
  }
  
  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    wLock.lock();
    try {
      badExtents.addAll(keySet);
    } finally {
      wLock.unlock();
    }
    if (log.isTraceEnabled())
      log.trace("Invalidated " + keySet.size() + " cache entries for table " + tableId);
  }
  
  @Override
  public void invalidateCache(String server) {
    int invalidatedCount = 0;
    
    wLock.lock();
    try {
      for (TabletLocation cacheEntry : metaCache.values())
        if (cacheEntry.tablet_location.equals(server)) {
          badExtents.add(cacheEntry.tablet_extent);
          invalidatedCount++;
        }
    } finally {
      wLock.unlock();
    }
    
    if (log.isTraceEnabled())
      log.trace("invalidated " + invalidatedCount + " cache entries  table=" + tableId + " server=" + server);
    
  }
  
  @Override
  public void invalidateCache() {
    int invalidatedCount;
    wLock.lock();
    try {
      invalidatedCount = metaCache.size();
      metaCache.clear();
    } finally {
      wLock.unlock();
    }
    if (log.isTraceEnabled())
      log.trace("invalidated all " + invalidatedCount + " cache entries for table=" + tableId);
  }
  
  @Override
  public TabletLocation locateTablet(Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    
    OpTimer opTimer = null;
    if (log.isTraceEnabled())
      opTimer = new OpTimer(log, Level.TRACE).start("Locating tablet  table=" + tableId + " row=" + TextUtil.truncate(row) + "  skipRow=" + skipRow + " retry="
          + retry);
    
    while (true) {
      
      TabletLocation tl;
      
      tl = _locateTablet(row, skipRow, retry, true);
      
      if (retry && tl == null) {
        UtilWaitThread.sleep(100);
        if (log.isTraceEnabled())
          log.trace("Failed to locate tablet containing row " + TextUtil.truncate(row) + " in table " + tableId + ", will retry...");
        continue;
      }
      
      if (opTimer != null)
        opTimer.stop("Located tablet " + (tl == null ? null : tl.tablet_extent) + " at " + (tl == null ? null : tl.tablet_location) + " in %DURATION%");
      
      return tl;
    }
  }
  
  private void lookupTabletLocation(Text row, boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Text metadataRow = new Text(tableId);
    metadataRow.append(new byte[] {';'}, 0, 1);
    metadataRow.append(row.getBytes(), 0, row.getLength());
    TabletLocation ptl = parent.locateTablet(metadataRow, false, retry);
    
    if (ptl != null) {
      List<TabletLocation> locations = locationObtainer.lookupTablet(ptl, metadataRow, lastTabletRow, parent);
      if (locations.size() == 0 && !ptl.tablet_extent.isRootTablet()) {
        // try the next tablet
        Text er = ptl.tablet_extent.getEndRow();
        if (er != null && er.compareTo(lastTabletRow) < 0) {
          // System.out.println("er "+er+"  ltr "+lastTabletRow);
          ptl = parent.locateTablet(er, true, retry);
          if (ptl != null)
            locations = locationObtainer.lookupTablet(ptl, metadataRow, lastTabletRow, parent);
        }
      }
      
      // cannot assume the list contains contiguous key extents... so it is probably
      // best to deal with each extent individually
      
      Text lastEndRow = null;
      for (TabletLocation tabletLocation : locations) {
        
        KeyExtent ke = tabletLocation.tablet_extent;
        TabletLocation locToCache;
        
        // create new location if current prevEndRow == endRow
        if ((lastEndRow != null) && (ke.getPrevEndRow() != null) && ke.getPrevEndRow().equals(lastEndRow)) {
          locToCache = new TabletLocation(new KeyExtent(ke.getTableId(), ke.getEndRow(), lastEndRow), tabletLocation.tablet_location);
        } else {
          locToCache = tabletLocation;
        }
        
        // save endRow for next iteration
        lastEndRow = locToCache.tablet_extent.getEndRow();
        
        updateCache(locToCache);
      }
    }
    
  }
  
  private void updateCache(TabletLocation tabletLocation) {
    if (!tabletLocation.tablet_extent.getTableId().equals(tableId)) {
      // sanity check
      throw new IllegalStateException("Unexpected extent returned " + tableId + "  " + tabletLocation.tablet_extent);
    }
    
    if (tabletLocation.tablet_location == null) {
      // sanity check
      throw new IllegalStateException("Cannot add null locations to cache " + tableId + "  " + tabletLocation.tablet_extent);
    }
    
    if (!tabletLocation.tablet_extent.getTableId().equals(tableId)) {
      // sanity check
      throw new IllegalStateException("Cannot add other table ids to locations cache " + tableId + "  " + tabletLocation.tablet_extent);
    }
    
    // clear out any overlapping extents in cache
    removeOverlapping(metaCache, tabletLocation.tablet_extent);
    
    // add it to cache
    Text er = tabletLocation.tablet_extent.getEndRow();
    if (er == null)
      er = MAX_TEXT;
    metaCache.put(er, tabletLocation);
    
    if (badExtents.size() > 0)
      removeOverlapping(badExtents, tabletLocation.tablet_extent);
  }
  
  static void removeOverlapping(TreeMap<Text,TabletLocation> metaCache, KeyExtent nke) {
    Iterator<Entry<Text,TabletLocation>> iter = null;
    
    if (nke.getPrevEndRow() == null) {
      iter = metaCache.entrySet().iterator();
    } else {
      Text row = rowAfterPrevRow(nke);
      SortedMap<Text,TabletLocation> tailMap = metaCache.tailMap(row);
      iter = tailMap.entrySet().iterator();
    }
    
    while (iter.hasNext()) {
      Entry<Text,TabletLocation> entry = iter.next();
      
      KeyExtent ke = entry.getValue().tablet_extent;
      
      if (stopRemoving(nke, ke)) {
        break;
      }
      
      iter.remove();
    }
  }
  
  private static boolean stopRemoving(KeyExtent nke, KeyExtent ke) {
    return ke.getPrevEndRow() != null && nke.getEndRow() != null && ke.getPrevEndRow().compareTo(nke.getEndRow()) >= 0;
  }
  
  private static Text rowAfterPrevRow(KeyExtent nke) {
    Text row = new Text(nke.getPrevEndRow());
    row.append(new byte[] {0}, 0, 1);
    return row;
  }
  
  static void removeOverlapping(TreeSet<KeyExtent> extents, KeyExtent nke) {
    for (KeyExtent overlapping : KeyExtent.findOverlapping(nke, extents)) {
      extents.remove(overlapping);
    }
  }
  
  private TabletLocation locateTabletInCache(Text row) {
    
    Entry<Text,TabletLocation> entry = metaCache.ceilingEntry(row);
    
    if (entry != null) {
      KeyExtent ke = entry.getValue().tablet_extent;
      if (ke.getPrevEndRow() == null || ke.getPrevEndRow().compareTo(row) < 0)
        return entry.getValue();
    }
    return null;
  }
  
  protected TabletLocation _locateTablet(Text row, boolean skipRow, boolean retry, boolean lock) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    
    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }
    
    TabletLocation tl;
    
    if (lock)
      rLock.lock();
    try {
      processInvalidated();
      tl = locateTabletInCache(row);
    } finally {
      if (lock)
        rLock.unlock();
    }
    
    if (tl == null) {
      if (lock)
        wLock.lock();
      try {
        // not in cache, so obtain info
        lookupTabletLocation(row, retry);
        
        tl = locateTabletInCache(row);
      } finally {
        if (lock)
          wLock.unlock();
      }
    }
    
    return tl;
  }
  
  private void processInvalidated() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    
    if (badExtents.size() == 0)
      return;
    
    boolean writeLockHeld = rwLock.isWriteLockedByCurrentThread();
    try {
      if (!writeLockHeld) {
        rLock.unlock();
        wLock.lock();
        if (badExtents.size() == 0)
          return;
      }
      
      List<Range> lookups = new ArrayList<Range>(badExtents.size());
      
      for (KeyExtent be : badExtents) {
        lookups.add(be.toMetadataRange());
        removeOverlapping(metaCache, be);
      }
      
      lookups = Range.mergeOverlapping(lookups);
      
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
      
      parent.binRanges(lookups, binnedRanges);
      
      // randomize server order
      ArrayList<String> tabletServers = new ArrayList<String>(binnedRanges.keySet());
      Collections.shuffle(tabletServers);
      
      for (String tserver : tabletServers) {
        List<TabletLocation> locations = locationObtainer.lookupTablets(tserver, binnedRanges.get(tserver), parent);
        
        for (TabletLocation tabletLocation : locations) {
          updateCache(tabletLocation);
        }
      }
    } finally {
      if (!writeLockHeld) {
        rLock.lock();
        wLock.unlock();
      }
    }
  }
  
  protected static void addRange(Map<String,Map<KeyExtent,List<Range>>> binnedRanges, String location, KeyExtent ke, Range range) {
    Map<KeyExtent,List<Range>> tablets = binnedRanges.get(location);
    if (tablets == null) {
      tablets = new HashMap<KeyExtent,List<Range>>();
      binnedRanges.put(location, tablets);
    }
    
    List<Range> tabletsRanges = tablets.get(ke);
    if (tabletsRanges == null) {
      tabletsRanges = new ArrayList<Range>();
      tablets.put(ke, tabletsRanges);
    }
    
    tabletsRanges.add(range);
  }
  
}
