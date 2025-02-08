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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.END_ROW_COMPARATOR;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.LockCheckerSession;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.MAX_TEXT;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.TabletLocationObtainer;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.TabletServerLockChecker;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.addMutation;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.isContiguous;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.locateTabletInCache;
import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.removeOverlapping;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class ConcurrentTabletLocator extends TabletLocator {

  protected TabletLocator parent;
  private ConcurrentSkipListMap<Text,TabletLocation> metaCache =
      new ConcurrentSkipListMap<>(END_ROW_COMPARATOR);
  protected TabletLocationObtainer locationObtainer;
  private final TabletServerLockChecker lockChecker;
  private final Lock lookupLock = new ReentrantLock();
  protected Text lastTabletRow;
  private final TableId tableId;

  public ConcurrentTabletLocator(TableId tableId, TabletLocator parent, TabletLocationObtainer tlo,
      TabletServerLockChecker tslc) {
    this.tableId = tableId;
    this.parent = parent;
    this.locationObtainer = tlo;
    this.lockChecker = tslc;

    this.lastTabletRow = new Text(tableId.canonical());
    lastTabletRow.append(new byte[] {'<'}, 0, 1);
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }

    LockCheckerSession lcSession = new LockCheckerSession(lockChecker);
    TabletLocation tl = locateTablet(context, row, retry, lcSession);
    while (retry && tl == null) {
      sleepUninterruptibly(100, MILLISECONDS);
      tl = locateTablet(context, row, retry, lcSession);
    }

    return tl;
    // TODO add logging that is same as TabletLocatorImpl
  }

  private TabletLocation locateTablet(ClientContext context, Text row, boolean retry,
      LockCheckerSession lcSession)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    TabletLocation tl = lcSession.checkLock(locateTabletInCache(metaCache, row));
    if (tl == null) {
      // This lock is not protecting any in memory data structures in this class its only purpose is
      // to limit concurrent metadata lookups to one at a time.
      lookupLock.lock();
      try {
        // while waiting for the lock its possible another thread did a metadata lookup that would
        // satisfy this request, so check again before reading the metadata table
        tl = lcSession.checkLock(locateTabletInCache(metaCache, row));
        if (tl == null) {
          lookupTabletLocation(context, row, retry, lcSession);
        }
      } finally {
        lookupLock.unlock();
      }
      tl = lcSession.checkLock(locateTabletInCache(metaCache, row));
    }
    return tl;
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    // TODO handle empty mutations, otherwise this could loop forever

    // this is also much simpler than the existing code because it does not do the switch from read
    // lock to write lock
    LockCheckerSession lcSession = new LockCheckerSession(lockChecker);
    Text row = new Text();

    // Make a first past where only the cache is examined, this is done to avoid always sorting the
    // mutations.
    ArrayList<T> notInCache = new ArrayList<>();
    for (T mutation : mutations) {
      row.set(mutation.getRow());
      TabletLocation tl = lcSession.checkLock(locateTabletInCache(metaCache, row));
      if (tl == null || !addMutation(binnedMutations, mutation, tl, lcSession)) {
        notInCache.add(mutation);
      }
    }

    if (!notInCache.isEmpty()) {
      // it is best to do the metadata lookups in sorted order as this may as this maximizes the
      // chance that looking up one row in the metadata table will also cover the row after it that
      // need the be looked up
      notInCache.sort((o1, o2) -> WritableComparator.compareBytes(o1.getRow(), 0,
          o1.getRow().length, o2.getRow(), 0, o2.getRow().length));
      for (T mutation : notInCache) {
        // TODO this seems like it could have terrible performance in the case where tablets do not
        // have locations and a metadata lookup is done for each mutation. This problem also exists
        // in TabletLocatorImpl. In main 4280 addressed this problem, may want to explore fixing in
        // 2.1.
        row.set(mutation.getRow());
        lookupTabletLocation(context, row, false, lcSession);
        TabletLocation tl = lcSession.checkLock(locateTabletInCache(metaCache, row));
        if (tl == null || !addMutation(binnedMutations, mutation, tl, lcSession)) {
          failures.add(mutation);
        }
      }
    }

    // TODO add logging that is same as TabletLocatorImpl
  }

  // TODO this code was mostly copied from TabletLocatorImpl w/ small changes
  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    List<Range> failures = new ArrayList<>();
    List<TabletLocation> tabletLocations = new ArrayList<>();

    LockCheckerSession lcSession = new LockCheckerSession(lockChecker);

    l1: for (Range range : ranges) {

      tabletLocations.clear();

      Text startRow;

      if (range.getStartKey() != null) {
        startRow = range.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      TabletLocation tl = null;

      tl = locateTablet(context, startRow, false, lcSession);

      if (tl == null) {
        failures.add(range);
        continue;
      }

      tabletLocations.add(tl);

      while (tl.tablet_extent.endRow() != null
          && !range.afterEndKey(new Key(tl.tablet_extent.endRow()).followingKey(PartialKey.ROW))) {

        Text row = new Text(tl.tablet_extent.endRow());
        row.append(new byte[] {0}, 0, 1);
        tl = locateTablet(context, row, false, lcSession);

        if (tl == null) {
          failures.add(range);
          continue l1;
        }
        tabletLocations.add(tl);
      }

      // Ensure the extents found are non overlapping and have no holes. When reading some extents
      // from the cache and other from the metadata table in the loop above we may end up with
      // non-contiguous extents. This can happen when a subset of exents are placed in the cache and
      // then after that merges and splits happen.
      if (isContiguous(tabletLocations)) {
        for (TabletLocation tl2 : tabletLocations) {
          TabletLocatorImpl.addRange(binnedRanges, tl2.tablet_location, tl2.tablet_extent, range);
        }
      } else {
        failures.add(range);
        // These extents in the cache are not contiguous, so something is wrong. Clear them out in
        // the cache to force a reread from metadata table the next go around.
        tabletLocations
            .forEach(tabletLocation -> removeOverlapping(metaCache, tabletLocation.tablet_extent));
      }

    }

    return failures;
    // TODO add logging that is same as TabletLocatorImpl
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    removeOverlapping(metaCache, failedExtent);
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    keySet.forEach(extent -> removeOverlapping(metaCache, extent));
  }

  @Override
  public void invalidateCache() {
    metaCache.clear();
  }

  private BlockingQueue<String> serverInvalidationQueue = new LinkedBlockingQueue<>();
  private final Lock serverInvalidationLock = new ReentrantLock();

  @Override
  public void invalidateCache(ClientContext context, String server) {
    // This method is structured so that when lots of threads attempt to invalidate servers at
    // around the same time the amount of full scans of the metadata cache is minimized.
    serverInvalidationQueue.add(server);
    serverInvalidationLock.lock();
    try {
      if (serverInvalidationQueue.isEmpty()) {
        // some other thread invalidated our server so nothing to do
        return;
      }
      HashSet<String> serversToInvalidate = new HashSet<>();
      serverInvalidationQueue.drainTo(serversToInvalidate);
      metaCache.values().removeIf(tl -> serversToInvalidate.contains(tl.tablet_location));
    } finally {
      serverInvalidationLock.unlock();
    }
  }

  // TODO this code was copied from TabletLocatorImpl and really small changes were made, attempt to
  // share code
  private void lookupTabletLocation(ClientContext context, Text row, boolean retry,
      LockCheckerSession lcSession)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Text metadataRow = new Text(tableId.canonical());
    metadataRow.append(new byte[] {';'}, 0, 1);
    metadataRow.append(row.getBytes(), 0, row.getLength());
    TabletLocation ptl = parent.locateTablet(context, metadataRow, false, retry);

    if (ptl != null) {
      TabletLocations locations =
          locationObtainer.lookupTablet(context, ptl, metadataRow, lastTabletRow, parent);
      while (locations != null && locations.getLocations().isEmpty()
          && locations.getLocationless().isEmpty()) {
        // try the next tablet, the current tablet does not have any tablets that overlap the row
        Text er = ptl.tablet_extent.endRow();
        if (er != null && er.compareTo(lastTabletRow) < 0) {
          // System.out.println("er "+er+" ltr "+lastTabletRow);
          ptl = parent.locateTablet(context, er, true, retry);
          if (ptl != null) {
            locations =
                locationObtainer.lookupTablet(context, ptl, metadataRow, lastTabletRow, parent);
          } else {
            break;
          }
        } else {
          break;
        }
      }

      if (locations == null) {
        return;
      }

      // cannot assume the list contains contiguous key extents... so it is probably
      // best to deal with each extent individually

      Text lastEndRow = null;
      for (TabletLocation tabletLocation : locations.getLocations()) {

        KeyExtent ke = tabletLocation.tablet_extent;
        TabletLocation locToCache;

        // create new location if current prevEndRow == endRow
        if ((lastEndRow != null) && (ke.prevEndRow() != null)
            && ke.prevEndRow().equals(lastEndRow)) {
          locToCache = new TabletLocation(new KeyExtent(ke.tableId(), ke.endRow(), lastEndRow),
              tabletLocation.tablet_location, tabletLocation.tablet_session);
        } else {
          locToCache = tabletLocation;
        }

        // save endRow for next iteration
        lastEndRow = locToCache.tablet_extent.endRow();

        updateCache(locToCache, lcSession);
      }
    }

  }

  // TODO this code was copied from TabletLocatorImpl and really small changes were made, attempt to
  // share code
  private void updateCache(TabletLocation tabletLocation, LockCheckerSession lcSession) {
    if (!tabletLocation.tablet_extent.tableId().equals(tableId)) {
      // sanity check
      throw new IllegalStateException(
          "Unexpected extent returned " + tableId + "  " + tabletLocation.tablet_extent);
    }

    if (tabletLocation.tablet_location == null) {
      // sanity check
      throw new IllegalStateException(
          "Cannot add null locations to cache " + tableId + "  " + tabletLocation.tablet_extent);
    }

    // clear out any overlapping extents in cache
    removeOverlapping(metaCache, tabletLocation.tablet_extent);

    // do not add to cache unless lock is held
    if (lcSession.checkLock(tabletLocation) == null) {
      return;
    }

    // add it to cache
    Text er = tabletLocation.tablet_extent.endRow();
    if (er == null) {
      er = MAX_TEXT;
    }
    metaCache.put(er, tabletLocation);
  }
}
