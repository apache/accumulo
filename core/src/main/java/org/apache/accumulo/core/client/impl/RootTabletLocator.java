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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletServerLockChecker;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RootTabletLocator extends TabletLocator {

  private final TabletServerLockChecker lockChecker;
  private final ZooCacheFactory zcf;

  RootTabletLocator(TabletServerLockChecker lockChecker) {
    this(lockChecker, new ZooCacheFactory());
  }

  RootTabletLocator(TabletServerLockChecker lockChecker, ZooCacheFactory zcf) {
    this.lockChecker = lockChecker;
    this.zcf = zcf;
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
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
  public List<Range> binRanges(ClientContext context, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {

    TabletLocation rootTabletLocation = getRootTabletLocation(context);
    if (rootTabletLocation != null) {
      for (Range range : ranges) {
        TabletLocatorImpl.addRange(binnedRanges, rootTabletLocation.tablet_location, RootTable.EXTENT, range);
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
  public void invalidateCache(Instance instance, String server) {
    ZooCache zooCache = zcf.getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    String root = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
    zooCache.clear(root + "/" + server);
  }

  @Override
  public void invalidateCache() {}

  protected TabletLocation getRootTabletLocation(ClientContext context) {
    Instance instance = context.getInstance();
    String zRootLocPath = ZooUtil.getRoot(instance) + RootTable.ZROOT_TABLET_LOCATION;
    ZooCache zooCache = zcf.getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());

    Logger log = LoggerFactory.getLogger(this.getClass());

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up root tablet location in zookeeper.", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = zooCache.get(zRootLocPath);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found root tablet at {} in {}", Thread.currentThread().getId(), (loc == null ? "null" : new String(loc)),
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    if (loc == null) {
      return null;
    }

    String[] tokens = new String(loc).split("\\|");

    if (lockChecker.isLockHeld(tokens[0], tokens[1]))
      return new TabletLocation(RootTable.EXTENT, tokens[0], tokens[1]);
    else
      return null;
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    TabletLocation location = getRootTabletLocation(context);
    // Always retry when finding the root tablet
    while (retry && location == null) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      location = getRootTabletLocation(context);
    }

    return location;
  }

}
