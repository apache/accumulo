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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.TabletLocatorImpl.TabletServerLockChecker;
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

public class RootTabletLocator extends TabletLocator {

  private final TabletServerLockChecker lockChecker;

  RootTabletLocator(TabletServerLockChecker lockChecker) {
    this.lockChecker = lockChecker;
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures) {
    TabletLocation rootTabletLocation = getRootTabletLocation(context);
    if (rootTabletLocation != null) {
      TabletServerMutations<T> tsm = new TabletServerMutations<>(rootTabletLocation.tablet_session);
      for (T mutation : mutations) {
        tsm.addMutation(RootTable.EXTENT, mutation);
      }
      binnedMutations.put(rootTabletLocation.tablet_location, tsm);
    } else {
      failures.addAll(mutations);
    }
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges) {

    TabletLocation rootTabletLocation = getRootTabletLocation(context);
    if (rootTabletLocation != null) {
      for (Range range : ranges) {
        TabletLocatorImpl.addRange(binnedRanges, rootTabletLocation.tablet_location,
            RootTable.EXTENT, range);
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

  protected TabletLocation getRootTabletLocation(ClientContext context) {
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
      return new TabletLocation(RootTable.EXTENT, server, loc.getSession());
    } else {
      return null;
    }
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry) {
    TabletLocation location = getRootTabletLocation(context);
    // Always retry when finding the root tablet
    while (retry && location == null) {
      sleepUninterruptibly(500, MILLISECONDS);
      location = getRootTabletLocation(context);
    }

    return location;
  }

}
