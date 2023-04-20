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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataCachedTabletObtainer;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonService;
import org.apache.accumulo.core.util.Interner;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Client side cache of information about Tablets. Currently, a tablet prev end row is cached and
 * locations are cached if they exists.
 */
public abstract class ClientTabletCache {

  /**
   * Flipped false on call to {@link #clearLocators}. Checked by client classes that locally cache
   * Locators.
   */
  private volatile boolean isValid = true;

  boolean isValid() {
    return isValid;
  }

  /**
   * Used to indicate if a user of this interface needs a tablet with a location. This simple enum
   * was created instead of using a boolean for code clarity.
   */
  public enum LocationNeed {
    REQUIRED, NOT_REQUIRED
  }

  /**
   * Finds the tablet that contains the given row.
   *
   * @param locationNeed When {@link LocationNeed#REQUIRED} is passed will only return a tablet if
   *        it has location. When {@link LocationNeed#NOT_REQUIRED} is passed will return the tablet
   *        that overlaps the row with or without a location.
   *
   * @return overlapping tablet. If no overlapping tablet exists, returns null. If location is
   *         required and the tablet currently has no location ,returns null.
   */
  public abstract CachedTablet findTablet(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  public CachedTablet findTabletWithRetry(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    var tl = findTablet(context, row, skipRow, locationNeed);
    while (tl == null && locationNeed == LocationNeed.REQUIRED) {
      UtilWaitThread.sleep(100);
      tl = findTablet(context, row, skipRow, locationNeed);
    }
    return tl;
  }

  public abstract <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  // ELASTICITY_TODO rename to findTablets
  /**
   * <p>
   * This method finds what tablets overlap a given set of ranges, passing each range and its
   * associated tablet to the range consumer. If a range overlaps multiple tablets then it can be
   * passed to the range consumer multiple times.
   * </p>
   *
   * @param locationNeed When {@link LocationNeed#REQUIRED} is passed only tablets that have a
   *        location are provided to the rangeConsumer, any range that overlaps a tablet without a
   *        location will be returned as a failure. When {@link LocationNeed#NOT_REQUIRED} is
   *        passed, ranges that overlap tablets with and without a location are provided to the
   *        range consumer.
   * @param ranges For each range will try to find overlapping contiguous tablets that optionally
   *        have a location.
   * @param rangeConsumer If all of the tablets that a range overlaps are found, then the range and
   *        tablets will be passed to this consumer one at time. A range will either be passed to
   *        this consumer one more mor times OR returned as a failuer, but never both.
   *
   * @return The failed ranges that did not have a location (if a location is required) or where
   *         contiguous tablets could not be found.
   */
  public abstract List<Range> findTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer, LocationNeed locationNeed)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * The behavior of this method is similar to
   * {@link #findTablets(ClientContext, List, BiConsumer, LocationNeed)}, except it bins ranges to
   * the passed in binnedRanges map instead of passing them to a consumer. This method only bins to
   * hosted tablets with a location.
   */
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return findTablets(context, ranges, ((cachedTablet, range) -> ClientTabletCacheImpl
        .addRange(binnedRanges, cachedTablet, range)), LocationNeed.REQUIRED);
  }

  public abstract void invalidateCache(KeyExtent failedExtent);

  public abstract void invalidateCache(Collection<KeyExtent> keySet);

  /**
   * Invalidate entire cache
   */
  public abstract void invalidateCache();

  /**
   * Invalidate all metadata entries that point to server
   */
  public abstract void invalidateCache(ClientContext context, String server);

  private static class LocatorKey {
    InstanceId instanceId;
    TableId tableId;

    LocatorKey(InstanceId instanceId, TableId table) {
      this.instanceId = instanceId;
      this.tableId = table;
    }

    @Override
    public int hashCode() {
      return instanceId.hashCode() + tableId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof LocatorKey) {
        return equals((LocatorKey) o);
      }
      return false;
    }

    public boolean equals(LocatorKey lk) {
      return instanceId.equals(lk.instanceId) && tableId.equals(lk.tableId);
    }

  }

  private static final HashMap<LocatorKey,ClientTabletCache> locators = new HashMap<>();
  private static boolean enabled = true;

  public static synchronized void clearLocators() {
    for (ClientTabletCache locator : locators.values()) {
      locator.isValid = false;
    }
    locators.clear();
  }

  static synchronized boolean isEnabled() {
    return enabled;
  }

  static synchronized void disable() {
    clearLocators();
    enabled = false;
  }

  static synchronized void enable() {
    enabled = true;
  }

  public long getTabletHostingRequestCount() {
    return 0L;
  }

  public static synchronized ClientTabletCache getLocator(ClientContext context, TableId tableId) {
    Preconditions.checkState(enabled, "The Accumulo singleton that that tracks tablet locations is "
        + "disabled. This is likely caused by all AccumuloClients being closed or garbage collected");
    LocatorKey key = new LocatorKey(context.getInstanceID(), tableId);
    ClientTabletCache tl = locators.get(key);
    if (tl == null) {
      MetadataCachedTabletObtainer mlo = new MetadataCachedTabletObtainer();

      if (RootTable.ID.equals(tableId)) {
        tl = new RootClientTabletCache(new ZookeeperLockChecker(context));
      } else if (MetadataTable.ID.equals(tableId)) {
        tl = new ClientTabletCacheImpl(MetadataTable.ID, getLocator(context, RootTable.ID), mlo,
            new ZookeeperLockChecker(context));
      } else {
        tl = new ClientTabletCacheImpl(tableId, getLocator(context, MetadataTable.ID), mlo,
            new ZookeeperLockChecker(context));
      }
      locators.put(key, tl);
    }

    return tl;
  }

  static {
    SingletonManager.register(new SingletonService() {

      @Override
      public boolean isEnabled() {
        return ClientTabletCache.isEnabled();
      }

      @Override
      public void enable() {
        ClientTabletCache.enable();
      }

      @Override
      public void disable() {
        ClientTabletCache.disable();
      }
    });
  }

  public static class CachedTablets {

    private final List<CachedTablet> locations;

    public CachedTablets(List<CachedTablet> locations) {
      this.locations = locations;
    }

    public List<CachedTablet> getLocations() {
      return locations;
    }
  }

  public static class CachedTablet {
    private static final Interner<String> interner = new Interner<>();

    private final KeyExtent tablet_extent;
    private final String tserverLocation;
    private final String tserverSession;

    public CachedTablet(KeyExtent tablet_extent, String tablet_location, String session) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      checkArgument(tablet_location != null, "tablet_location is null");
      checkArgument(session != null, "session is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = interner.intern(tablet_location);
      this.tserverSession = interner.intern(session);
    }

    public CachedTablet(KeyExtent tablet_extent, Optional<String> tablet_location,
        Optional<String> session) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = tablet_location.map(interner::intern).orElse(null);
      this.tserverSession = session.map(interner::intern).orElse(null);
    }

    public CachedTablet(KeyExtent tablet_extent) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = null;
      this.tserverSession = null;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CachedTablet) {
        CachedTablet otl = (CachedTablet) o;
        return getExtent().equals(otl.getExtent())
            && getTserverLocation().equals(otl.getTserverLocation())
            && getTserverSession().equals(otl.getTserverSession());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getExtent(), tserverLocation, tserverSession);
    }

    @Override
    public String toString() {
      return "(" + getExtent() + "," + getTserverLocation() + "," + getTserverSession() + ")";
    }

    public KeyExtent getExtent() {
      return tablet_extent;
    }

    public Optional<String> getTserverLocation() {
      return Optional.ofNullable(tserverLocation);
    }

    public Optional<String> getTserverSession() {
      return Optional.ofNullable(tserverSession);
    }
  }

  public static class TabletServerMutations<T extends Mutation> {
    private Map<KeyExtent,List<T>> mutations;
    private String tserverSession;

    public TabletServerMutations(String tserverSession) {
      this.tserverSession = tserverSession;
      this.mutations = new HashMap<>();
    }

    public void addMutation(KeyExtent ke, T m) {
      List<T> mutList = mutations.computeIfAbsent(ke, k -> new ArrayList<>());
      mutList.add(m);
    }

    public Map<KeyExtent,List<T>> getMutations() {
      return mutations;
    }

    final String getSession() {
      return tserverSession;
    }
  }
}
