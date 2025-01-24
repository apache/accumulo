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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.MetadataLocationObtainer;
import org.apache.accumulo.core.util.Interner;
import org.apache.hadoop.io.Text;

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

  public abstract List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

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

  public static synchronized void clearLocators(ClientContext context) {
    final var locators = context.tabletLocators();
    for (TabletLocator locator : locators.values()) {
      locator.isValid = false;
    }
    locators.clear();
  }

  public static synchronized TabletLocator getLocator(ClientContext context, TableId tableId) {
    final var locators = context.tabletLocators();
    TabletLocator tl = locators.get(tableId);
    if (tl == null) {
      MetadataLocationObtainer mlo = new MetadataLocationObtainer();

      if (AccumuloTable.ROOT.tableId().equals(tableId)) {
        tl = new RootTabletLocator(context.getTServerLockChecker());
      } else if (AccumuloTable.METADATA.tableId().equals(tableId)) {
        tl = new TabletLocatorImpl(AccumuloTable.METADATA.tableId(),
            getLocator(context, AccumuloTable.ROOT.tableId()), mlo,
            context.getTServerLockChecker());
      } else {
        tl = new TabletLocatorImpl(tableId, getLocator(context, AccumuloTable.METADATA.tableId()),
            mlo, context.getTServerLockChecker());
      }
      locators.put(tableId, tl);
    }

    return tl;
  }

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

    @Override
    public boolean equals(Object o) {
      if (o instanceof TabletLocation) {
        TabletLocation otl = (TabletLocation) o;
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

    public String getTserverLocation() {
      return tserverLocation;
    }

    public String getTserverSession() {
      return tserverSession;
    }
  }

  public static class TabletServerMutations<T extends Mutation> {
    private final Map<KeyExtent,List<T>> mutations;
    private final String tserverSession;

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
