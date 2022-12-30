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
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataLocationObtainer;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonService;
import org.apache.accumulo.core.util.Interner;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

//TODO rename to TabletCache
public abstract class TabletLocator {

  /**
   * Flipped false on call to {@link #clearLocators}. Checked by client classes that locally cache
   * Locators.
   */
  private volatile boolean isValid = true;

  boolean isValid() {
    return isValid;
  }

  public abstract TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  public abstract <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  public abstract List<Range> locateTablets(ClientContext context, List<Range> ranges,
      BiConsumer<TabletLocation,Range> rangeConsumer)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return locateTablets(context, ranges,
        ((cachedTablet, range) -> TabletLocatorImpl.addRange(binnedRanges, cachedTablet, range)));
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

  public abstract Mode getMode();

  public enum Mode {
    OFFLINE, ONLINE
  }

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

  private static final HashMap<LocatorKey,TabletLocator> locators = new HashMap<>();
  private static boolean enabled = true;

  public static synchronized void clearLocators() {
    for (TabletLocator locator : locators.values()) {
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

  private static synchronized TabletLocator getOfflineCache(ClientContext context,
      TableId tableId) {
    Preconditions.checkArgument(!RootTable.ID.equals(tableId) && !MetadataTable.ID.equals(tableId));

    LocatorKey key = new LocatorKey(context.getInstanceID(), tableId);
    TabletLocator tl = locators.get(key);

    if (tl != null && tl.getMode() == Mode.ONLINE) {
      tl.invalidateCache();
      locators.remove(key);
      tl = null;
    }

    if (tl == null) {
      MetadataLocationObtainer mlo = new MetadataLocationObtainer(Mode.OFFLINE);
      TabletLocatorImpl.TabletServerLockChecker tslc =
          new TabletLocatorImpl.TabletServerLockChecker() {
            @Override
            public boolean isLockHeld(String tserver, String session) {
              return true;
            }

            @Override
            public void invalidateCache(String server) {

            }
          };

      tl = new TabletLocatorImpl(tableId, getLocator(context, MetadataTable.ID), mlo, tslc);

      locators.put(key, tl);
    }

    return tl;
  }

  // TODO rename to getInstance
  public static synchronized TabletLocator getLocator(ClientContext context, TableId tableId,
      ScannerBase.ConsistencyLevel consistency) {
    if (consistency == ScannerBase.ConsistencyLevel.EVENTUAL
        && context.getTableState(tableId) == TableState.OFFLINE) {
      return getOfflineCache(context, tableId);
    }

    return getLocator(context, tableId);
  }

  // TODO rename to getInstance
  public static synchronized TabletLocator getLocator(ClientContext context, TableId tableId) {
    Preconditions.checkState(enabled, "The Accumulo singleton that that tracks tablet locations is "
        + "disabled. This is likely caused by all AccumuloClients being closed or garbage collected");
    LocatorKey key = new LocatorKey(context.getInstanceID(), tableId);
    TabletLocator tl = locators.get(key);

    if (tl != null && tl.getMode() == Mode.OFFLINE) {
      tl.invalidateCache();
      locators.remove(key);
      tl = null;
    }

    if (tl == null) {
      MetadataLocationObtainer mlo = new MetadataLocationObtainer(Mode.ONLINE);

      if (RootTable.ID.equals(tableId)) {
        tl = new RootTabletLocator(new ZookeeperLockChecker(context));
      } else if (MetadataTable.ID.equals(tableId)) {
        tl = new TabletLocatorImpl(MetadataTable.ID, getLocator(context, RootTable.ID), mlo,
            new ZookeeperLockChecker(context));
      } else {
        tl = new TabletLocatorImpl(tableId, getLocator(context, MetadataTable.ID), mlo,
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
        return TabletLocator.isEnabled();
      }

      @Override
      public void enable() {
        TabletLocator.enable();
      }

      @Override
      public void disable() {
        TabletLocator.disable();
      }
    });
  }

  // TODO rename to CachedTablets
  public static class TabletLocations {

    private final List<TabletLocation> locations;
    private final List<KeyExtent> locationless;

    public TabletLocations(List<TabletLocation> locations, List<KeyExtent> locationless) {
      this.locations = locations;
      this.locationless = locationless;
    }

    public List<TabletLocation> getLocations() {
      return locations;
    }

    public List<KeyExtent> getLocationless() {
      return locationless;
    }
  }

  // TODO rename to CachedTablet
  public static class TabletLocation {

    private static final Interner<String> interner = new Interner<>();
    private final KeyExtent tablet_extent;
    private final String tserverLocation;
    private final String tserverSession;

    public TabletLocation(KeyExtent tablet_extent, String tablet_location, String session) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      checkArgument(tablet_location != null, "tablet_location is null");
      checkArgument(session != null, "session is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = interner.intern(tablet_location);
      this.tserverSession = interner.intern(session);
    }

    public TabletLocation(KeyExtent tablet_extent) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = null;
      this.tserverSession = null;
    }

    public TabletLocation(KeyExtent tablet_extent, TabletLocation other) {
      checkArgument(tablet_extent != null, "tablet_extent is null");
      this.tablet_extent = tablet_extent;
      this.tserverLocation = other.tserverLocation;
      this.tserverSession = other.tserverSession;
    }

    public KeyExtent getExtent() {
      return tablet_extent;
    }

    public boolean hasTserverLocation() {
      return tserverLocation != null;
    }

    public String getTserverLocation() {
      return Objects.requireNonNull(tserverLocation);
    }

    public String getTserverSession() {
      return Objects.requireNonNull(tserverSession);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TabletLocation that = (TabletLocation) o;
      return getExtent().equals(that.getExtent())
          && Objects.equals(tserverLocation, that.tserverLocation)
          && Objects.equals(tserverSession, that.tserverSession);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getExtent(), tserverLocation, tserverSession);
    }

    @Override
    public String toString() {
      return "(" + getExtent() + "," + tserverLocation + "," + tserverSession + ")";
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
