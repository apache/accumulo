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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletServerLockChecker;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RootTabletLocator extends TabletLocator {

  private final Instance instance;
  private final TabletServerLockChecker lockChecker;
  private final ZooCacheFactory zcf;

  RootTabletLocator(Instance instance, TabletServerLockChecker lockChecker) {
    this(instance, lockChecker, new ZooCacheFactory());
  }

  RootTabletLocator(Instance instance, TabletServerLockChecker lockChecker, ZooCacheFactory zcf) {
    this.instance = instance;
    this.lockChecker = lockChecker;
    this.zcf = zcf;
  }

  @Override
  public <T extends Mutation> void binMutations(Credentials credentials, List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations,
      List<T> failures) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    TabletLocation rootTabletLocation = getRootTabletLocation();
    if (rootTabletLocation != null) {
      TabletServerMutations<T> tsm = new TabletServerMutations<T>(rootTabletLocation.tablet_session);
      for (T mutation : mutations) {
        tsm.addMutation(RootTable.EXTENT, mutation);
      }
      binnedMutations.put(rootTabletLocation.tablet_location, tsm);
    } else {
      failures.addAll(mutations);
    }
  }

  @Override
  public List<Range> binRanges(Credentials credentials, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {

    TabletLocation rootTabletLocation = getRootTabletLocation();
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
  public void invalidateCache(String server) {
    ZooCache zooCache = zcf.getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    String root = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
    zooCache.clear(root + "/" + server);
  }

  @Override
  public void invalidateCache() {}

  protected TabletLocation getRootTabletLocation() {
    String zRootLocPath = ZooUtil.getRoot(instance) + RootTable.ZROOT_TABLET_LOCATION;
    ZooCache zooCache = zcf.getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());

    OpTimer opTimer = new OpTimer(Logger.getLogger(this.getClass()), Level.TRACE).start("Looking up root tablet location in zookeeper.");
    byte[] loc = zooCache.get(zRootLocPath);
    opTimer.stop("Found root tablet at " + (loc == null ? null : new String(loc)) + " in %DURATION%");

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
  public TabletLocation locateTablet(Credentials credentials, Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    TabletLocation location = getRootTabletLocation();
    // Always retry when finding the root tablet
    while (retry && location == null) {
      UtilWaitThread.sleep(500);
      location = getRootTabletLocation();
    }

    return location;
  }

}
