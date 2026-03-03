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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.Timer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy.Eviction;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;

public class OfflineTabletLocatorImpl extends TabletLocator {

  private static final Logger LOG = LoggerFactory.getLogger(OfflineTabletLocatorImpl.class);

  public static class OfflineTabletLocation extends TabletLocation {

    public static final String SERVER = "offline_table_marker";

    public OfflineTabletLocation(KeyExtent tablet_extent) {
      super(tablet_extent, SERVER, SERVER);
    }

  }

  private class OfflineTabletsCache implements RemovalListener<KeyExtent,KeyExtent> {

    private final ClientContext context;
    private final int maxCacheSize;
    private final int prefetch;
    private final Cache<KeyExtent,KeyExtent> cache;
    private final LinkedBlockingQueue<KeyExtent> evictions = new LinkedBlockingQueue<>();
    private final TreeSet<KeyExtent> extents = new TreeSet<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Timer scanTimer = Timer.startNew();
    private final AtomicInteger cacheCount = new AtomicInteger(0);
    private final Eviction<KeyExtent,KeyExtent> evictionPolicy;

    private OfflineTabletsCache(ClientContext context)
        throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
      this.context = context;
      Properties clientProperties = context.getProperties();
      Duration cacheDuration = Duration.ofMillis(
          ClientProperty.OFFLINE_LOCATOR_CACHE_DURATION.getTimeInMillis(clientProperties));
      maxCacheSize =
          Integer.parseInt(ClientProperty.OFFLINE_LOCATOR_CACHE_SIZE.getValue(clientProperties));
      prefetch = Integer
          .parseInt(ClientProperty.OFFLINE_LOCATOR_CACHE_PREFETCH.getValue(clientProperties));

      // This cache is used to evict KeyExtents from the extents TreeSet when
      // they have not been accessed in cacheDuration. We are targeting to have
      // maxCacheSize objects in the cache, but are not using the Cache's maximumSize
      // to achieve this as the Cache will remove things from the Cache that were
      // newly inserted and not yet used. This negates the pre-fetching feature
      // that we have added into this TabletLocator for offline tables. Here we
      // set the maximum size much larger than the property and use the cacheCount
      // variable to manage the max size manually.
      // @formatter:off
      Caffeine<KeyExtent,KeyExtent> builder = Caffeine.newBuilder()
          .expireAfterAccess(cacheDuration)
          .removalListener(this)
          .scheduler(Scheduler.systemScheduler())
          .recordStats(() -> new CaffeineStatsCounter(Metrics.globalRegistry,
              OfflineTabletsCache.class.getSimpleName()));
      if (maxCacheSize > 0) {
        builder.initialCapacity(maxCacheSize).maximumSize(maxCacheSize * 2);
      } else {
        String tname = context.getTableName(tid);
        builder.initialCapacity(context.tableOperations().listSplits(tname).size());
      }
      cache = builder.build();
      // @formatter:on
      if (maxCacheSize > 0) {
        evictionPolicy = cache.policy().eviction().orElseThrow();
      } else {
        evictionPolicy = null;
      }
    }

    @Override
    public void onRemoval(KeyExtent key, KeyExtent value, RemovalCause cause) {
      if (cause == RemovalCause.REPLACED) {
        // Don't remove from `extents` if the object was replaced in the cache
        return;
      }
      LOG.trace("Extent {} was evicted from cache for {} ", key, cause);
      cacheCount.decrementAndGet();
      evictions.add(key);
      try {
        if (lock.writeLock().tryLock(1, TimeUnit.MILLISECONDS)) {
          try {
            processRecentCacheEvictions();
          } finally {
            lock.writeLock().unlock();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting to acquire write lock", e);
      }
    }

    private void processRecentCacheEvictions() {
      Preconditions.checkArgument(lock.writeLock().isHeldByCurrentThread());
      Set<KeyExtent> copy = new HashSet<>();
      evictions.drainTo(copy);
      int numEvictions = copy.size();
      if (numEvictions > 0) {
        LOG.trace("Processing {} prior evictions", numEvictions);
        extents.removeAll(copy);
      }
    }

    private KeyExtent findOrLoadExtent(KeyExtent searchKey) {
      lock.readLock().lock();
      try {
        KeyExtent match = extents.ceiling(searchKey);
        if (match != null && match.contains(searchKey.endRow())) {
          // update access time in cache
          @SuppressWarnings("unused")
          var unused = cache.getIfPresent(match);
          LOG.trace("Extent {} found in cache for start row {}", match, searchKey);
          return match;
        }
      } finally {
        lock.readLock().unlock();
      }
      lock.writeLock().lock();
      // process prior evictions since we have the write lock
      processRecentCacheEvictions();
      // The following block of code fixes an issue with
      // the cache where recently pre-fetched extents
      // will be evicted from the cache when it reaches
      // the maxCacheSize. This is because from the cache's
      // perspective they are the coldest objects. The code
      // below manually removes the coldest extents that are
      // before the searchKey.endRow to make room for the next
      // batch of extents that we are going to load into the
      // cache so that they are not immediately evicted.
      if (maxCacheSize > 0 && cacheCount.get() + prefetch + 1 >= maxCacheSize) {
        int evictionSize = prefetch * 2;
        Set<KeyExtent> candidates = new HashSet<>(evictionPolicy.coldest(evictionSize).keySet());
        LOG.trace("Cache near max size, evaluating {} coldest entries", candidates);
        candidates.removeIf(ke -> ke.contains(searchKey.endRow()) || ke.endRow() == null
            || ke.endRow().compareTo(searchKey.endRow()) >= 0);
        LOG.trace("Manually evicting coldest entries: {}", candidates);
        cache.invalidateAll(candidates);
        cache.cleanUp();
      }
      // Load TabletMetadata
      if (LOG.isDebugEnabled()) {
        scanTimer.restart();
      }
      int added = 0;
      try (TabletsMetadata tm =
          context.getAmple().readTablets().forTable(tid).overlapping(searchKey.endRow(), true, null)
              .fetch(ColumnType.PREV_ROW, ColumnType.LOCATION).build()) {
        Iterator<TabletMetadata> iter = tm.iterator();
        for (int i = 0; i < prefetch && iter.hasNext(); i++) {
          TabletMetadata t = iter.next();
          KeyExtent ke = t.getExtent();
          LOG.trace("Caching extent: {}", ke);
          cache.put(ke, ke);
          cacheCount.incrementAndGet();
          TabletLocatorImpl.removeOverlapping(extents, ke);
          extents.add(ke);
          added++;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Took {}ms to scan and load {} metadata tablets for table {}",
              scanTimer.elapsed(TimeUnit.MILLISECONDS), added, tid);
        }
        return extents.ceiling(searchKey);
      } finally {
        lock.writeLock().unlock();
      }
    }

    private void invalidate(KeyExtent failedExtent) {
      cache.invalidate(failedExtent);
    }

    private void invalidate(Collection<KeyExtent> keySet) {
      cache.invalidateAll(keySet);
    }

    private void invalidateAll() {
      cache.invalidateAll();
    }

  }

  private final TableId tid;
  private final OfflineTabletsCache extentCache;

  public OfflineTabletLocatorImpl(ClientContext context, TableId tableId) {
    tid = tableId;
    if (context.getTableState(tid) != TableState.OFFLINE) {
      throw new IllegalStateException("Table " + tableId + " is not offline");
    }
    try {
      extentCache = new OfflineTabletsCache(context);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Table " + tableId + " does not exist", e);
    } catch (AccumuloSecurityException | AccumuloException e) {
      throw new IllegalStateException("Unable to get split points for table: " + tableId, e);
    }
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }

    Text metadataRow = new Text(tid.canonical());
    metadataRow.append(new byte[] {';'}, 0, 1);
    metadataRow.append(row.getBytes(), 0, row.getLength());

    LOG.trace("Locating offline tablet for row: {}", metadataRow);
    KeyExtent searchKey = KeyExtent.fromMetaRow(metadataRow);
    KeyExtent match = extentCache.findOrLoadExtent(searchKey);
    if (match != null) {
      if (match.contains(row)) {
        LOG.trace("Found match for row: {}, extent = {}", row, match);
        return new OfflineTabletLocation(match);
      }
    }
    LOG.trace("Found no matching extent for row: {}", row);
    return null;
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    List<TabletLocation> tabletLocations = new ArrayList<>(ranges.size());
    List<Range> failures = new ArrayList<>();

    l1: for (Range r : ranges) {
      LOG.trace("Looking up locations for range: {}", r);
      tabletLocations.clear();
      Text startRow;

      if (r.getStartKey() != null) {
        startRow = r.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      TabletLocation tl = this.locateTablet(context, startRow, false, false);
      if (tl == null) {
        LOG.trace("NOT FOUND first tablet in range: {}", r);
        failures.add(r);
        continue;
      }
      LOG.trace("Found first tablet in range: {}, extent: {}", r, tl.tablet_extent);
      tabletLocations.add(tl);

      while (tl.tablet_extent.endRow() != null
          && !r.afterEndKey(new Key(tl.tablet_extent.endRow()).followingKey(PartialKey.ROW))) {
        KeyExtent priorExtent = tl.tablet_extent;
        tl = locateTablet(context, tl.tablet_extent.endRow(), true, false);

        if (tl == null) {
          LOG.trace("NOT FOUND tablet following {} in range: {}", priorExtent, r);
          failures.add(r);
          continue l1;
        }
        LOG.trace("Found following tablet in range: {}, extent: {}", r, tl.tablet_extent);
        tabletLocations.add(tl);
      }

      // Ensure the extents found are non overlapping and have no holes. When reading some extents
      // from the cache and other from the metadata table in the loop above we may end up with
      // non-contiguous extents. This can happen when a subset of exents are placed in the cache and
      // then after that merges and splits happen.
      if (TabletLocatorImpl.isContiguous(tabletLocations)) {
        for (TabletLocation tl2 : tabletLocations) {
          TabletLocatorImpl.addRange(binnedRanges, tl2.tablet_location, tl2.tablet_extent, r);
        }
      } else {
        LOG.trace("Found non-contiguous tablet in range: {}", r);
        failures.add(r);
      }

    }
    return failures;
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    extentCache.invalidate(failedExtent);
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    extentCache.invalidate(keySet);
  }

  @Override
  public void invalidateCache() {
    extentCache.invalidateAll();
  }

  @Override
  public void invalidateCache(ClientContext context, String server) {
    invalidateCache();
  }

}
