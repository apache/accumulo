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

import java.time.Duration;
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
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.MetadataCachedTabletObtainer;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonService;
import org.apache.accumulo.core.util.Interner;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Client side cache of information about Tablets. Currently, a tablet prev end row is cached and
 * locations are cached if they exist.
 */
public abstract class ClientTabletCache {

  /**
   * Flipped false on call to {@link #clearInstances}. Checked by client classes that locally cache
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
   * This method allows linear scans to host tablet ahead of time that they may read in the future.
   * The goal of this method is to allow tablets to request hosting of tablet for a scan before the
   * scan actually needs it. Below is an example of how this method could work with a scan when
   * {@code minimumHostAhead=4} is passed and avoid the scan having to wait on tablet hosting.
   *
   * <ol>
   * <li>4*2 tablets are initially hosted (the scan has to wait on this)</li>
   * <li>The 1st,2nd,3rd, and 4th tablets are read by the scan</li>
   * <li>The request to read the 5th tablets causes a request to host 4 more tablets (this would be
   * the 9th,10th,11th, and 12th tablets)</li>
   * <li>The 5th,6th,7th, and 8th tablet are read by the scan</li>
   * <li>While the scan does the read above, the 9th,10th,11th, and 12th tablets are actually
   * hosted. This happens concurrently with the scan above.</li>
   * <li>When the scan goes to read the 9th tablet, hopefully its already hosted. Also attempting to
   * read the 9th tablet will cause a request to host the 13th,14th,15th, and 16th tablets.</li>
   * </ol>
   *
   * In the situation above, the goal is that while we are reading 4 hosted tablets the 4 following
   * tablets are in the process of being hosted.
   *
   * @param minimumHostAhead Attempts to keep between minimumHostAhead and 2*minimumHostAhead
   *        tablets following the found tablet hosted.
   * @param hostAheadRange Only host following tablets that are within this range.
   */
  public abstract CachedTablet findTablet(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed, int minimumHostAhead, Range hostAheadRange)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException;

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
  public CachedTablet findTablet(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, InvalidTabletHostingRequestException {
    return findTablet(context, row, skipRow, locationNeed, 0, null);
  }

  public CachedTablet findTabletWithRetry(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, InvalidTabletHostingRequestException {
    var tl = findTablet(context, row, skipRow, locationNeed);
    while (tl == null && locationNeed == LocationNeed.REQUIRED) {
      UtilWaitThread.sleep(100);
      tl = findTablet(context, row, skipRow, locationNeed);
    }
    return tl;
  }

  public abstract <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException;

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
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException;

  /**
   * The behavior of this method is similar to
   * {@link #findTablets(ClientContext, List, BiConsumer, LocationNeed)}, except it bins ranges to
   * the passed in binnedRanges map instead of passing them to a consumer. This method only bins to
   * hosted tablets with a location.
   */
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, InvalidTabletHostingRequestException {
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

  private static class InstanceKey {
    InstanceId instanceId;
    TableId tableId;

    InstanceKey(InstanceId instanceId, TableId table) {
      this.instanceId = instanceId;
      this.tableId = table;
    }

    @Override
    public int hashCode() {
      return instanceId.hashCode() + tableId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof InstanceKey) {
        return equals((InstanceKey) o);
      }
      return false;
    }

    public boolean equals(InstanceKey lk) {
      return instanceId.equals(lk.instanceId) && tableId.equals(lk.tableId);
    }

  }

  private static final HashMap<InstanceKey,ClientTabletCache> instances = new HashMap<>();
  private static boolean enabled = true;

  public static synchronized void clearInstances() {
    for (ClientTabletCache locator : instances.values()) {
      locator.isValid = false;
    }
    instances.clear();
  }

  static synchronized boolean isEnabled() {
    return enabled;
  }

  static synchronized void disable() {
    clearInstances();
    enabled = false;
  }

  static synchronized void enable() {
    enabled = true;
  }

  public long getTabletHostingRequestCount() {
    return 0L;
  }

  public static synchronized ClientTabletCache getInstance(ClientContext context, TableId tableId) {
    Preconditions.checkState(enabled, "The Accumulo singleton that that tracks tablet locations is "
        + "disabled. This is likely caused by all AccumuloClients being closed or garbage collected");
    InstanceKey key = new InstanceKey(context.getInstanceID(), tableId);
    ClientTabletCache tl = instances.get(key);
    if (tl == null) {
      MetadataCachedTabletObtainer mlo = new MetadataCachedTabletObtainer();

      if (AccumuloTable.ROOT.tableId().equals(tableId)) {
        tl = new RootClientTabletCache(new ZookeeperLockChecker(context));
      } else if (AccumuloTable.METADATA.tableId().equals(tableId)) {
        tl = new ClientTabletCacheImpl(AccumuloTable.METADATA.tableId(),
            getInstance(context, AccumuloTable.ROOT.tableId()), mlo,
            new ZookeeperLockChecker(context));
      } else {
        tl = new ClientTabletCacheImpl(tableId,
            getInstance(context, AccumuloTable.METADATA.tableId()), mlo,
            new ZookeeperLockChecker(context));
      }
      instances.put(key, tl);
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

    private final List<CachedTablet> cachedTablets;

    public CachedTablets(List<CachedTablet> cachedTablets) {
      this.cachedTablets = cachedTablets;
    }

    public List<CachedTablet> getCachedTablets() {
      return cachedTablets;
    }
  }

  public static class CachedTablet {
    private static final Interner<String> interner = new Interner<>();

    private final KeyExtent tablet_extent;
    private final String tserverLocation;
    private final String tserverSession;
    private final TabletAvailability availability;
    private final boolean hostingRequested;

    private final Long creationTime = System.nanoTime();

    public CachedTablet(KeyExtent tablet_extent, String tablet_location, String session,
        TabletAvailability availability, boolean hostingRequested) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      checkArgument(tablet_location != null, "tablet_location is null");
      checkArgument(session != null, "session is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = interner.intern(tablet_location);
      this.tserverSession = interner.intern(session);
      this.availability = Objects.requireNonNull(availability);
      this.hostingRequested = hostingRequested;
    }

    public CachedTablet(KeyExtent tablet_extent, Optional<String> tablet_location,
        Optional<String> session, TabletAvailability availability, boolean hostingRequested) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = tablet_location.map(interner::intern).orElse(null);
      this.tserverSession = session.map(interner::intern).orElse(null);
      this.availability = Objects.requireNonNull(availability);
      this.hostingRequested = hostingRequested;
    }

    public CachedTablet(KeyExtent tablet_extent, TabletAvailability availability,
        boolean hostingRequested) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = null;
      this.tserverSession = null;
      this.availability = Objects.requireNonNull(availability);
      this.hostingRequested = hostingRequested;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CachedTablet) {
        CachedTablet otl = (CachedTablet) o;
        return getExtent().equals(otl.getExtent())
            && getTserverLocation().equals(otl.getTserverLocation())
            && getTserverSession().equals(otl.getTserverSession())
            && getAvailability() == otl.getAvailability()
            && hostingRequested == otl.hostingRequested;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getExtent(), tserverLocation, tserverSession, availability,
          hostingRequested);
    }

    @Override
    public String toString() {
      return "(" + getExtent() + "," + getTserverLocation() + "," + getTserverSession() + ","
          + getAvailability() + ")";
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

    /**
     * The ClientTabletCache will remove and replace a CachedTablet when the location is no longer
     * valid. However, it will not do the same when the availability is no longer valid. The
     * availability returned by this method may be out of date. If this information is needed to be
     * fresh, then you may want to consider clearing the cache first.
     */
    public TabletAvailability getAvailability() {
      return this.availability;
    }

    public Duration getAge() {
      return Duration.ofNanos(System.nanoTime() - creationTime);
    }

    public boolean wasHostingRequested() {
      return hostingRequested;
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
