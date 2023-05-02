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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientTabletCacheImpl.TabletServerLockChecker;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.ReadConsistency;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class RootClientTabletCache extends ClientTabletCache {

  private final TabletServerLockChecker lockChecker;

  RootClientTabletCache(TabletServerLockChecker lockChecker) {
    this.lockChecker = lockChecker;
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures) {
    CachedTablet rootCachedTablet = getRootTabletLocation(context);
    if (rootCachedTablet != null) {
      var tsm = new TabletServerMutations<T>(rootCachedTablet.getTserverSession().get());
      for (T mutation : mutations) {
        tsm.addMutation(RootTable.EXTENT, mutation);
      }
      binnedMutations.put(rootCachedTablet.getTserverLocation().get(), tsm);
    } else {
      failures.addAll(mutations);
    }
  }

  @Override
  public List<Range> findTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer, LocationNeed locationNeed) {

    // only expect the hosted case so this code only handles that, so throw an exception is
    // something else is seen
    Preconditions.checkArgument(locationNeed == LocationNeed.REQUIRED);

    CachedTablet rootCachedTablet = getRootTabletLocation(context);
    if (rootCachedTablet != null) {
      for (Range range : ranges) {
        rangeConsumer.accept(rootCachedTablet, range);
      }
      return Collections.emptyList();
    }
    return ranges;
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {}

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {}

  @Override
  public void invalidateCache(ClientContext context, String server) {
    ZooCache zooCache = context.getZooCache();
    String root = context.getZooKeeperRoot() + Constants.ZTSERVERS;
    zooCache.clear(root + "/" + server);
  }

  @Override
  public void invalidateCache() {}

  protected CachedTablet getRootTabletLocation(ClientContext context) {
    Logger log = LoggerFactory.getLogger(this.getClass());

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up root tablet location in zookeeper.",
          Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    Location loc = context.getAmple()
        .readTablet(RootTable.EXTENT, ReadConsistency.EVENTUAL, LOCATION).getLocation();

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found root tablet at {} in {}", Thread.currentThread().getId(), loc,
          String.format("%.3f secs", timer.scale(SECONDS)));
    }

    if (loc == null || loc.getType() != LocationType.CURRENT) {
      return null;
    }

    String server = loc.getHostPort();

    if (lockChecker.isLockHeld(server, loc.getSession())) {
      return new CachedTablet(RootTable.EXTENT, server, loc.getSession(), TabletHostingGoal.ALWAYS);
    } else {
      return null;
    }
  }

  @Override
  public CachedTablet findTablet(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed) {
    // only expect the hosted case so this code only handles that, so throw an exception is
    // something else is seen
    Preconditions.checkArgument(locationNeed == LocationNeed.REQUIRED);

    CachedTablet location = getRootTabletLocation(context);
    // Always retry when finding the root tablet
    while (location == null) {
      sleepUninterruptibly(500, MILLISECONDS);
      location = getRootTabletLocation(context);
    }

    return location;
  }

}
