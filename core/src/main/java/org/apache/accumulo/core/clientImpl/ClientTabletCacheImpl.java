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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LockMap;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class has two concurrency goals. First when a thread request data that is currently present
 * in the cache, it should never block. Second when a thread request data that is not in the cache
 * only one lookup per metadata tablet will happen concurrently. The purpose of this second goal is
 * to avoid redundant concurrent metadata lookups in a client process.
 *
 * <p>
 * The first goal is achieved by using a ConcurrentSkipListMap to store the caches data making it
 * safe for multiple threads to read and write to the map. The second goal is achieved by using a
 * {@link LockMap} keyed on metadata table extents when doing metadata table lookups.
 *
 * <p>
 * Below is an example of how this cache is intended to work.
 *
 * <ol>
 * <li>Thread_1 lookups up row A that is not currently present in the cache.
 * <li>Thread_2 lookups up row C that is not currently present in the cache.
 * <li>Thread_3 lookups up row Q that is not currently present in the cache.
 * <li>Thread_1 finds metadata tablet MT1 stores information on row A and locks the extent for MT1.
 * <li>Thread_2 finds metadata tablet MT1 stores information on row C and locks the extent for MT1.
 * <li>Thread_3 finds metadata tablet MT2 stores information on row Q and locks the extent for MT2.
 * <li>Thread_1 acquires the lock for MT1
 * <li>Thread_2 blocks waiting to lock MT1
 * <li>Thread_3 acquires the lock for MT2
 * <li>Thread_4 finds row Z in the cache and immediately returns its user tablet information. If
 * this data was not cached, it would have needed to read metadata tablet MT2 which is currently
 * locked and would have blocked.
 * <li>Thread_1 reads user_tablet_1_metadata that contains row A from MT1 and adds it to the cache.
 * It also opportunistically reads a few more user tablets metadata from MT1 after the first user
 * tablet adds them the cache.
 * <li>Thread_3 reads user_tablet_10_metadata that contains row Q from MT2 and adds it to the cache.
 * <li>Thread_1 finds user_tablet_1_metadata in the cache and returns it as the tablet for row A.
 * <li>Thread_1 unlocks the lock for MT1
 * <li>Thread_3 finds user_tablet_10_metadata in the cache and returns it as the tablet for row Q.
 * <li>Thread_3 unlocks the lock for MT2
 * <li>Thread_2 acquires the lock for MT1
 * <li>Thread_2 checks the cache and finds the information it needs is now present in the cache
 * because it was found by Thread_1. No metadata lookup is done, the information from the cache is
 * returned.
 * <li>Thread_2 unlocks the lock for MT1
 * </ol>
 *
 *
 */
public class ClientTabletCacheImpl extends ClientTabletCache {

  private static final Logger log = LoggerFactory.getLogger(ClientTabletCacheImpl.class);
  private static final AtomicBoolean HOSTING_ENABLED = new AtomicBoolean(true);

  // MAX_TEXT represents a TEXT object that is greater than all others. Attempted to use null for
  // this purpose, but there seems to be a bug in TreeMap.tailMap with null. Therefore instead of
  // using null, created MAX_TEXT.
  static final Text MAX_TEXT = new Text();

  static final Comparator<Text> END_ROW_COMPARATOR = (o1, o2) -> {
    if (o1 == o2) {
      return 0;
    }
    if (o1 == MAX_TEXT) {
      return 1;
    }
    if (o2 == MAX_TEXT) {
      return -1;
    }
    return o1.compareTo(o2);
  };

  protected final TableId tableId;
  protected final ClientTabletCache parent;
  protected final ConcurrentSkipListMap<Text,CachedTablet> metaCache =
      new ConcurrentSkipListMap<>(END_ROW_COMPARATOR);
  protected final CachedTabletObtainer tabletObtainer;
  private final TabletServerLockChecker lockChecker;
  protected final Text lastTabletRow;

  private final AtomicLong tabletHostingRequestCount = new AtomicLong(0);

  private final LockMap<KeyExtent> lookupLocks = new LockMap<>();

  public interface CachedTabletObtainer {
    /**
     * @return null when unable to read information successfully
     */
    CachedTablets lookupTablet(ClientContext context, CachedTablet src, Text row, Text stopRow)
        throws AccumuloSecurityException, AccumuloException;
  }

  public interface TabletServerLockChecker {
    boolean isLockHeld(String tserver, String session);

    void invalidateCache(String server);
  }

  private class LockCheckerSession {

    private final HashSet<Pair<String,String>> okLocks = new HashSet<>();
    private final HashSet<Pair<String,String>> invalidLocks = new HashSet<>();

    private CachedTablet checkLock(CachedTablet tl) {
      // the goal of this class is to minimize calls out to lockChecker under that
      // assumption that
      // it is a resource synchronized among many threads... want to
      // avoid fine-grained synchronization when binning lots of mutations or ranges... remember
      // decisions from the lockChecker in thread local unsynchronized
      // memory

      if (tl == null) {
        return null;
      }

      if (tl.getTserverLocation().isEmpty()) {
        return tl;
      }

      Pair<String,String> lock =
          new Pair<>(tl.getTserverLocation().orElseThrow(), tl.getTserverSession().orElseThrow());

      if (okLocks.contains(lock)) {
        return tl;
      }

      if (invalidLocks.contains(lock)) {
        return null;
      }

      if (lockChecker.isLockHeld(tl.getTserverLocation().orElseThrow(),
          tl.getTserverSession().orElseThrow())) {
        okLocks.add(lock);
        return tl;
      }

      if (log.isTraceEnabled()) {
        log.trace("Tablet server {} {} no longer holds its lock", tl.getTserverLocation(),
            tl.getTserverSession());
      }

      invalidLocks.add(lock);

      return null;
    }
  }

  public ClientTabletCacheImpl(TableId tableId, ClientTabletCache parent, CachedTabletObtainer tlo,
      TabletServerLockChecker tslc) {
    this.tableId = tableId;
    this.parent = parent;
    this.tabletObtainer = tlo;
    this.lockChecker = tslc;

    this.lastTabletRow = new Text(tableId.canonical());
    lastTabletRow.append(new byte[] {'<'}, 0, 1);
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {

    Timer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Binning {} mutations for table {}", Thread.currentThread().getId(),
          mutations.size(), tableId);
      timer = Timer.startNew();
    }

    ArrayList<T> notInCache = new ArrayList<>();
    Text row = new Text();

    LockCheckerSession lcSession = new LockCheckerSession();

    // for this to be efficient rows need to be in sorted order, but always sorting is slow...
    // therefore only sort the
    // stuff not in the cache.... it is most efficient to pass _locateTablet rows in sorted order

    // For this to be efficient, need to avoid fine grained synchronization and fine grained
    // logging.
    // Therefore methods called by this are not synchronized and should not log.

    for (T mutation : mutations) {
      row.set(mutation.getRow());
      CachedTablet tl = findTabletInCache(row);
      if (!addMutation(binnedMutations, mutation, tl, lcSession)) {
        notInCache.add(mutation);
      }
    }

    HashSet<CachedTablet> locationLess = new HashSet<>();

    if (!notInCache.isEmpty()) {
      notInCache.sort((o1, o2) -> WritableComparator.compareBytes(o1.getRow(), 0,
          o1.getRow().length, o2.getRow(), 0, o2.getRow().length));

      // Want to ignore any entries in the cache w/o a location that were created before the
      // following time. Entries created after the following time may have been populated by the
      // following loop, and we want to use those.
      Timer cacheCutoffTimer = Timer.startNew();

      for (T mutation : notInCache) {

        row.set(mutation.getRow());

        CachedTablet tl =
            _findTablet(context, row, false, lcSession, LocationNeed.REQUIRED, cacheCutoffTimer);

        if (!addMutation(binnedMutations, mutation, tl, lcSession)) {
          failures.add(mutation);
          if (tl != null && tl.getTserverLocation().isEmpty()) {
            locationLess.add(tl);
          }
        }
      }
    }

    requestTabletHosting(context, locationLess);

    if (timer != null) {
      log.trace("tid={} Binned {} mutations for table {} to {} tservers in {}",
          Thread.currentThread().getId(), mutations.size(), tableId, binnedMutations.size(),
          String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0));
    }

  }

  private <T extends Mutation> boolean addMutation(
      Map<String,TabletServerMutations<T>> binnedMutations, T mutation, CachedTablet tl,
      LockCheckerSession lcSession) {

    if (tl == null || tl.getTserverLocation().isEmpty()) {
      return false;
    }

    TabletServerMutations<T> tsm = binnedMutations.get(tl.getTserverLocation().orElseThrow());

    if (tsm == null) {
      // do lock check once per tserver here to make binning faster
      boolean lockHeld = lcSession.checkLock(tl) != null;
      if (lockHeld) {
        tsm = new TabletServerMutations<>(tl.getTserverSession().orElseThrow());
        binnedMutations.put(tl.getTserverLocation().orElseThrow(), tsm);
      } else {
        return false;
      }
    }

    // its possible the same tserver could be listed with different sessions
    if (tsm.getSession().equals(tl.getTserverSession().orElseThrow())) {
      tsm.addMutation(tl.getExtent(), mutation);
      return true;
    }

    return false;
  }

  static boolean isContiguous(List<CachedTablet> cachedTablets) {

    Iterator<CachedTablet> iter = cachedTablets.iterator();
    KeyExtent prevExtent = iter.next().getExtent();

    while (iter.hasNext()) {
      KeyExtent currExtent = iter.next().getExtent();

      if (!currExtent.isPreviousExtent(prevExtent)) {
        return false;
      }

      prevExtent = currExtent;
    }

    return true;
  }

  private List<Range> findTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer, boolean useCache, LockCheckerSession lcSession,
      LocationNeed locationNeed, Consumer<CachedTablet> locationlessConsumer)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {
    List<Range> failures = new ArrayList<>();
    List<CachedTablet> cachedTablets = new ArrayList<>();

    // Use anything in the cache w/o a location populated after this point in time. Cache entries
    // w/o a location created before the following time should be ignored and the metadata table
    // consulted.
    Timer cacheCutoffTimer = Timer.startNew();

    l1: for (Range range : ranges) {

      cachedTablets.clear();

      Text startRow;

      if (range.getStartKey() != null) {
        startRow = range.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      CachedTablet tl = null;

      if (useCache) {
        tl = lcSession.checkLock(findTabletInCache(startRow));
      } else {
        tl = _findTablet(context, startRow, false, lcSession, locationNeed, cacheCutoffTimer);
      }

      if (tl == null) {
        failures.add(range);
        continue;
      }

      cachedTablets.add(tl);

      // a range may extend over multiple tablets, look for additional tablet that overlap the range
      while (tl.getExtent().endRow() != null
          && !range.afterEndKey(new Key(tl.getExtent().endRow()).followingKey(PartialKey.ROW))) {
        if (useCache) {
          Text row = new Text(tl.getExtent().endRow());
          row.append(new byte[] {0}, 0, 1);
          tl = lcSession.checkLock(findTabletInCache(row));
        } else {
          tl = _findTablet(context, tl.getExtent().endRow(), true, lcSession, locationNeed,
              cacheCutoffTimer);
        }

        if (tl == null) {
          failures.add(range);
          continue l1;
        }
        cachedTablets.add(tl);
      }

      // pass all tablets without a location before failing range
      cachedTablets.stream().filter(tloc -> tloc.getTserverLocation().isEmpty())
          .forEach(locationlessConsumer);

      if (locationNeed == LocationNeed.REQUIRED
          && !cachedTablets.stream().allMatch(tloc -> tloc.getTserverLocation().isPresent())) {
        failures.add(range);
        continue;
      }

      // Ensure the extents found are non overlapping and have no holes. When reading some extents
      // from the cache and other from the metadata table in the loop above we may end up with
      // non-contiguous extents. This can happen when a subset of exents are placed in the cache and
      // then after that merges and splits happen.
      if (isContiguous(cachedTablets)) {
        for (CachedTablet tl2 : cachedTablets) {
          rangeConsumer.accept(tl2, range);
        }
      } else {
        failures.add(range);
      }

    }

    return failures;
  }

  @Override
  public List<Range> findTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer, LocationNeed locationNeed)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {

    /*
     * For this to be efficient, need to avoid fine grained synchronization and fine grained
     * logging. Therefore methods called by this are not synchronized and should not log.
     */

    Timer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Binning {} ranges for table {}", Thread.currentThread().getId(),
          ranges.size(), tableId);
      timer = Timer.startNew();
    }

    LockCheckerSession lcSession = new LockCheckerSession();

    List<Range> failures;
    // for this to be optimal, need to look ranges up in sorted order when
    // ranges are not present in cache... however do not want to always
    // sort ranges... therefore try binning ranges using only the cache
    // and sort whatever fails and retry
    failures =
        findTablets(context, ranges, rangeConsumer, true, lcSession, locationNeed, keyExtent -> {});

    if (!failures.isEmpty()) {
      // sort failures by range start key
      Collections.sort(failures);

      // use a hashset because some ranges may overlap the same extent, so want to avoid duplicate
      // extents
      HashSet<CachedTablet> locationLess = new HashSet<>();
      Consumer<CachedTablet> locationLessConsumer;
      if (locationNeed == LocationNeed.REQUIRED) {
        locationLessConsumer = locationLess::add;
      } else {
        locationLessConsumer = keyExtent -> {};
      }

      // try lookups again
      failures = findTablets(context, failures, rangeConsumer, false, lcSession, locationNeed,
          locationLessConsumer);

      requestTabletHosting(context, locationLess);
    }

    if (timer != null) {
      log.trace("tid={} Binned {} ranges for table {} in {}", Thread.currentThread().getId(),
          ranges.size(), tableId, String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0));
    }

    return failures;
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    removeOverlapping(metaCache, failedExtent);
    if (log.isTraceEnabled()) {
      log.trace("Invalidated extent={}", failedExtent);
    }
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    keySet.forEach(extent -> removeOverlapping(metaCache, extent));
    if (log.isTraceEnabled()) {
      log.trace("Invalidated {} cache entries for table {}", keySet.size(), tableId);
    }
  }

  @Override
  public void invalidateCache() {
    int invalidatedCount = metaCache.size();
    metaCache.clear();
    this.tabletHostingRequestCount.set(0);
    if (log.isTraceEnabled()) {
      log.trace("invalidated all {} cache entries for table={}", invalidatedCount, tableId);
    }
  }

  @Override
  public CachedTablet findTablet(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed, int minimumHostAhead, Range hostAheadRange)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {

    Timer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Locating tablet  table={} row={} skipRow={}",
          Thread.currentThread().getId(), tableId, TextUtil.truncate(row), skipRow);
      timer = Timer.startNew();
    }

    LockCheckerSession lcSession = new LockCheckerSession();
    CachedTablet tl = _findTablet(context, row, skipRow, lcSession, locationNeed, Timer.startNew());

    if (timer != null) {
      log.trace("tid={} Located tablet {} at {} in {}", Thread.currentThread().getId(),
          (tl == null ? "null" : tl.getExtent()), (tl == null ? "null" : tl.getTserverLocation()),
          String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0));
    }

    if (tl != null && locationNeed == LocationNeed.REQUIRED) {
      // Look at the next (minimumHostAhead * 2) tablets and return which ones need hosting. See the
      // javadoc in the superclass of this method for more details.
      Map<KeyExtent,CachedTablet> extentsToHost = findExtentsToHost(context, minimumHostAhead * 2,
          hostAheadRange, lcSession, tl, locationNeed);

      if (!extentsToHost.isEmpty()) {
        if (extentsToHost.containsKey(tl.getExtent()) || extentsToHost.size() >= minimumHostAhead) {
          requestTabletHosting(context, extentsToHost.values());
        }
      }

      if (tl.getTserverLocation().isEmpty()) {
        return null;
      }
    }

    return tl;

  }

  private Map<KeyExtent,CachedTablet> findExtentsToHost(ClientContext context, int hostAheadCount,
      Range hostAheadRange, LockCheckerSession lcSession, CachedTablet firstTablet,
      LocationNeed locationNeed) throws AccumuloException, TableNotFoundException,
      InvalidTabletHostingRequestException, AccumuloSecurityException {

    // its only expected that this method is called when location need is required
    Preconditions.checkArgument(locationNeed == LocationNeed.REQUIRED);

    Map<KeyExtent,CachedTablet> extentsToHost;

    if (hostAheadCount > 0) {
      extentsToHost = new HashMap<>();
      if (firstTablet.getTserverLocation().isEmpty()) {
        extentsToHost.put(firstTablet.getExtent(), firstTablet);
      }

      KeyExtent extent = firstTablet.getExtent();

      var currTablet = extent;

      // Use anything in the cache w/o a location populated after this point in time. Cache entries
      // w/o a location created before the following time should be ignored and the metadata table
      // consulted.
      Timer cacheCutoffTimer = Timer.startNew();

      for (int i = 0; i < hostAheadCount; i++) {
        if (currTablet.endRow() == null || hostAheadRange
            .afterEndKey(new Key(currTablet.endRow()).followingKey(PartialKey.ROW))) {
          break;
        }

        CachedTablet followingTablet = _findTablet(context, currTablet.endRow(), true, lcSession,
            locationNeed, cacheCutoffTimer);

        if (followingTablet == null) {
          break;
        }

        currTablet = followingTablet.getExtent();

        if (followingTablet.getTserverLocation().isEmpty()
            && !followingTablet.wasHostingRequested()) {
          extentsToHost.put(followingTablet.getExtent(), followingTablet);
        }
      }
    } else if (firstTablet.getTserverLocation().isEmpty()) {
      extentsToHost = Map.of(firstTablet.getExtent(), firstTablet);
    } else {
      extentsToHost = Map.of();
    }
    return extentsToHost;
  }

  @Override
  public long getTabletHostingRequestCount() {
    return tabletHostingRequestCount.get();
  }

  @VisibleForTesting
  public void resetTabletHostingRequestCount() {
    tabletHostingRequestCount.set(0);
  }

  @VisibleForTesting
  public void enableTabletHostingRequests(boolean enabled) {
    HOSTING_ENABLED.set(enabled);
  }

  private static final Duration STALE_DURATION = Duration.ofMinutes(2);

  private void requestTabletHosting(ClientContext context,
      Collection<CachedTablet> tabletsWithNoLocation) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, InvalidTabletHostingRequestException {

    if (!HOSTING_ENABLED.get()) {
      return;
    }

    // System tables should always be hosted
    if (AccumuloTable.ROOT.tableId() == tableId || AccumuloTable.METADATA.tableId() == tableId) {
      return;
    }

    if (tabletsWithNoLocation.isEmpty()) {
      return;
    }

    if (context.getTableState(tableId) != TableState.ONLINE) {
      log.trace("requestTabletHosting: table {} is not online", tableId);
      return;
    }

    List<TKeyExtent> extentsToBringOnline = new ArrayList<>();
    for (var cachedTablet : tabletsWithNoLocation) {
      if (cachedTablet.getCreationTimer().elapsed().compareTo(STALE_DURATION) < 0) {
        if (cachedTablet.getAvailability() == TabletAvailability.ONDEMAND) {
          if (!cachedTablet.wasHostingRequested()) {
            extentsToBringOnline.add(cachedTablet.getExtent().toThrift());
            log.trace("requesting ondemand tablet to be hosted {}", cachedTablet.getExtent());
          } else {
            log.trace("ignoring ondemand tablet that already has a hosting request in place {} {}",
                cachedTablet.getExtent(), cachedTablet.getCreationTimer().elapsed());
          }
        } else if (cachedTablet.getAvailability() == TabletAvailability.UNHOSTED) {
          throw new InvalidTabletHostingRequestException("Extent " + cachedTablet.getExtent()
              + " has a tablet availability " + TabletAvailability.UNHOSTED);
        }
      } else {
        // When a tablet does not have a location it is reread from the metadata table before this
        // method is called. Therefore, it's expected that entries in the cache are recent. If the
        // entries are not recent it could have two causes. One is a bug in the Accumulo code.
        // Another is externalities like process swapping or slow metadata table reads. Logging a
        // warning in case there is a bug. If the warning ends up being too spammy and is caused by
        // externalities then this code/warning will need to be improved.
        log.warn("Unexpected stale tablet seen in cache {}", cachedTablet.getExtent());
        invalidateCache(cachedTablet.getExtent());
      }
    }

    if (!extentsToBringOnline.isEmpty()) {
      log.debug("Requesting hosting for {} ondemand tablets for table id {}.",
          extentsToBringOnline.size(), tableId);
      ThriftClientTypes.MANAGER.executeVoid(context,
          client -> client.requestTabletHosting(TraceUtil.traceInfo(), context.rpcCreds(),
              tableId.canonical(), extentsToBringOnline));
      tabletHostingRequestCount.addAndGet(extentsToBringOnline.size());
    }
  }

  private void lookupTablet(ClientContext context, Text row, LockCheckerSession lcSession)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {
    Text metadataRow = new Text(tableId.canonical());
    metadataRow.append(new byte[] {';'}, 0, 1);
    metadataRow.append(row.getBytes(), 0, row.getLength());
    CachedTablet ptl = parent.findTablet(context, metadataRow, false, LocationNeed.REQUIRED);

    if (ptl != null) {
      // Only allow a single lookup at time per parent tablet. For example if a tables tablets are
      // all stored in three metadata tablets, then that table could have up to three concurrent
      // metadata lookups.
      Timer timer = Timer.startNew();
      try (var unused = lookupLocks.lock(ptl.getExtent())) {
        // See if entry was added to cache by another thread while we were waiting on the lock
        var cached = findTabletInCache(row);
        if (cached != null && cached.getCreationTimer().startedAfter(timer)) {
          // This cache entry was added after we started waiting on the lock so lets use it and not
          // go to the metadata table. This means another thread was holding the lock and doing
          // metadata lookups when we requested the lock.
          return;
        }
        // Lookup tablets in metadata table and update cache. Also updating the cache while holding
        // the lock is important as it ensures other threads that are waiting on the lock will see
        // what this thread found and may be able to avoid metadata lookups.
        lookupTablet(context, lcSession, ptl, metadataRow);
      }
    }
  }

  private void lookupTablet(ClientContext context, LockCheckerSession lcSession, CachedTablet ptl,
      Text metadataRow) throws AccumuloSecurityException, AccumuloException, TableNotFoundException,
      InvalidTabletHostingRequestException {
    CachedTablets cachedTablets =
        tabletObtainer.lookupTablet(context, ptl, metadataRow, lastTabletRow);
    if (cachedTablets == null) {
      parent.invalidateCache(ptl.getExtent());
    }
    while (cachedTablets != null && cachedTablets.getCachedTablets().isEmpty()) {
      // try the next tablet, the current tablet does not have any tablets that overlap the row
      Text er = ptl.getExtent().endRow();
      if (er != null && er.compareTo(lastTabletRow) < 0) {
        // System.out.println("er "+er+" ltr "+lastTabletRow);
        ptl = parent.findTablet(context, er, true, LocationNeed.REQUIRED);
        if (ptl != null) {
          cachedTablets = tabletObtainer.lookupTablet(context, ptl, metadataRow, lastTabletRow);
          if (cachedTablets == null) {
            parent.invalidateCache(ptl.getExtent());
          }
        } else {
          break;
        }
      } else {
        break;
      }
    }

    if (cachedTablets == null) {
      return;
    }

    // cannot assume the list contains contiguous key extents... so it is probably
    // best to deal with each extent individually

    Text lastEndRow = null;
    for (CachedTablet cachedTablet : cachedTablets.getCachedTablets()) {

      KeyExtent ke = cachedTablet.getExtent();
      CachedTablet locToCache;

      // create new location if current prevEndRow == endRow
      if ((lastEndRow != null) && (ke.prevEndRow() != null) && ke.prevEndRow().equals(lastEndRow)) {
        locToCache = new CachedTablet(new KeyExtent(ke.tableId(), ke.endRow(), lastEndRow),
            cachedTablet.getTserverLocation(), cachedTablet.getTserverSession(),
            cachedTablet.getAvailability(), cachedTablet.wasHostingRequested());
      } else {
        locToCache = cachedTablet;
      }

      // save endRow for next iteration
      lastEndRow = locToCache.getExtent().endRow();

      updateCache(locToCache, lcSession);
    }
  }

  private void updateCache(CachedTablet cachedTablet, LockCheckerSession lcSession) {
    if (!cachedTablet.getExtent().tableId().equals(tableId)) {
      // sanity check
      throw new IllegalStateException(
          "Unexpected extent returned " + tableId + "  " + cachedTablet.getExtent());
    }

    // clear out any overlapping extents in cache
    removeOverlapping(metaCache, cachedTablet.getExtent());

    // do not add to cache unless lock is held
    if (lcSession.checkLock(cachedTablet) == null) {
      return;
    }

    // add it to cache
    Text er = cachedTablet.getExtent().endRow();
    if (er == null) {
      er = MAX_TEXT;
    }
    metaCache.put(er, cachedTablet);
  }

  static void removeOverlapping(NavigableMap<Text,CachedTablet> metaCache, KeyExtent nke) {
    Iterator<Entry<Text,CachedTablet>> iter;

    if (nke.prevEndRow() == null) {
      iter = metaCache.entrySet().iterator();
    } else {
      Text row = rowAfterPrevRow(nke);
      SortedMap<Text,CachedTablet> tailMap = metaCache.tailMap(row);
      iter = tailMap.entrySet().iterator();
    }

    while (iter.hasNext()) {
      Entry<Text,CachedTablet> entry = iter.next();

      KeyExtent ke = entry.getValue().getExtent();

      if (stopRemoving(nke, ke)) {
        break;
      }

      iter.remove();
    }
  }

  private static boolean stopRemoving(KeyExtent nke, KeyExtent ke) {
    return ke.prevEndRow() != null && nke.endRow() != null
        && ke.prevEndRow().compareTo(nke.endRow()) >= 0;
  }

  private static Text rowAfterPrevRow(KeyExtent nke) {
    Text row = new Text(nke.prevEndRow());
    row.append(new byte[] {0}, 0, 1);
    return row;
  }

  private CachedTablet findTabletInCache(Text row) {

    Entry<Text,CachedTablet> entry = metaCache.ceilingEntry(row);

    if (entry != null) {
      KeyExtent ke = entry.getValue().getExtent();
      if (ke.prevEndRow() == null || ke.prevEndRow().compareTo(row) < 0) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * @param cacheCutoffTimer Tablets w/o locations are cached. When LocationNeed is REQUIRED, this
   *        Timer value is used to determine if cached entries w/o a location should be used or of
   *        we should instead ignore them and reread the tablet information from the metadata table.
   */
  protected CachedTablet _findTablet(ClientContext context, Text row, boolean skipRow,
      LockCheckerSession lcSession, LocationNeed locationNeed, Timer cacheCutoffTimer)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {
    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }

    CachedTablet tl = lcSession.checkLock(findTabletInCache(row));

    if (tl == null || (locationNeed == LocationNeed.REQUIRED && tl.getTserverLocation().isEmpty()
        && cacheCutoffTimer.startedAfter(tl.getCreationTimer()))) {

      // not in cache OR the cutoff timer was started after when the cached entry timer was started,
      // so obtain info from metadata table
      tl = lookupTabletLocationAndCheckLock(context, row, lcSession);

    }

    return tl;
  }

  private CachedTablet lookupTabletLocationAndCheckLock(ClientContext context, Text row,
      LockCheckerSession lcSession) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, InvalidTabletHostingRequestException {
    lookupTablet(context, row, lcSession);
    return lcSession.checkLock(findTabletInCache(row));
  }

  static void addRange(Map<String,Map<KeyExtent,List<Range>>> binnedRanges, CachedTablet ct,
      Range range) {
    binnedRanges.computeIfAbsent(ct.getTserverLocation().orElseThrow(), k -> new HashMap<>())
        .computeIfAbsent(ct.getExtent(), k -> new ArrayList<>()).add(range);
  }
}
