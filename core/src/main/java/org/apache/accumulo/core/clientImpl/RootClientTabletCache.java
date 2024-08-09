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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientTabletCacheImpl.TabletServerLockChecker;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.util.Timer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides ability to get the location of the root tablet from a zoocache. This cache
 * implementation does not actually do any caching on its own and soley relies on zoocache. One nice
 * feature of using zoo cache is that if the location changes in zookeeper, it will eventually be
 * updated by zookeeper watchers in zoocache. Therefore, the invalidation functions are
 * intentionally no-ops and rely on the zookeeper watcher to keep things up to date.
 *
 * <p>
 * This code is relying on the assumption that if two client objects are created for the same
 * accumulo instance in the same process that both will have the same zoocache. This assumption
 * means there is only a single zookeeper watch per process per accumulo instance. This assumptions
 * leads to efficiencies at the cluster level by reduce the total number of zookeeper watches.
 * </p>
 */
public class RootClientTabletCache extends ClientTabletCache {

  private final TabletServerLockChecker lockChecker;

  RootClientTabletCache(TabletServerLockChecker lockChecker) {
    this.lockChecker = lockChecker;
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures) {
    CachedTablet rootCachedTablet = getRootTabletLocation(context);
    if (rootCachedTablet != null && rootCachedTablet.getTserverLocation().isPresent()) {
      var tsm = new TabletServerMutations<T>(rootCachedTablet.getTserverSession().orElseThrow());
      for (T mutation : mutations) {
        tsm.addMutation(RootTable.EXTENT, mutation);
      }
      binnedMutations.put(rootCachedTablet.getTserverLocation().orElseThrow(), tsm);
    } else {
      failures.addAll(mutations);
    }
  }

  @Override
  public List<Range> findTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer, LocationNeed locationNeed) {

    CachedTablet rootCachedTablet = getRootTabletLocation(context);

    if (rootCachedTablet.getTserverLocation().isEmpty() && locationNeed == LocationNeed.REQUIRED) {
      // there is no location and one is required so return all ranges as failures
      return ranges;
    } else {
      for (Range range : ranges) {
        rangeConsumer.accept(rootCachedTablet, range);
      }
      return Collections.emptyList();
    }
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    // no-op see class level javadoc
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    // no-op see class level javadoc
  }

  @Override
  public void invalidateCache(ClientContext context, String server) {
    // no-op see class level javadoc
  }

  @Override
  public void invalidateCache() {
    // no-op see class level javadoc
  }

  protected CachedTablet getRootTabletLocation(ClientContext context) {
    Logger log = LoggerFactory.getLogger(this.getClass());

    Timer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up root tablet location in zookeeper.",
          Thread.currentThread().getId());
      timer = Timer.startNew();
    }

    var zpath = RootTabletMetadata.zooPath(context);
    var zooCache = context.getZooCache();
    Location loc = new RootTabletMetadata(new String(zooCache.get(zpath), UTF_8)).toTabletMetadata()
        .getLocation();

    if (timer != null) {
      log.trace("tid={} Found root tablet at {} in {}", Thread.currentThread().getId(), loc,
          String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0));
    }

    if (loc == null || loc.getType() != LocationType.CURRENT) {
      return new CachedTablet(RootTable.EXTENT, Optional.empty(), Optional.empty(),
          TabletAvailability.HOSTED, false);
    }

    String server = loc.getHostPort();

    if (lockChecker.isLockHeld(server, loc.getSession())) {
      return new CachedTablet(RootTable.EXTENT, server, loc.getSession(), TabletAvailability.HOSTED,
          false);
    } else {
      return new CachedTablet(RootTable.EXTENT, Optional.empty(), Optional.empty(),
          TabletAvailability.HOSTED, false);
    }
  }

  @Override
  public CachedTablet findTablet(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed, int hostAheadCount, Range hostAheadRange) {

    CachedTablet cachedTablet = getRootTabletLocation(context);

    // Always retry when finding the root tablet
    while (cachedTablet.getTserverLocation().isEmpty() && locationNeed == LocationNeed.REQUIRED) {
      sleepUninterruptibly(500, MILLISECONDS);
      cachedTablet = getRootTabletLocation(context);
    }

    return cachedTablet;
  }
}
