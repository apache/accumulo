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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.MetadataLocationObtainer;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

public abstract class TabletLocator {

  public abstract TabletLocation locateTablet(Credentials credentials, Text row, boolean skipRow, boolean retry) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException;

  public abstract <T extends Mutation> void binMutations(Credentials credentials, List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations,
      List<T> failures) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  public abstract List<Range> binRanges(Credentials credentials, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
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
  public abstract void invalidateCache(String server);

  private static class LocatorKey {
    String instanceId;
    Text tableName;

    LocatorKey(String instanceId, Text table) {
      this.instanceId = instanceId;
      this.tableName = table;
    }

    @Override
    public int hashCode() {
      return instanceId.hashCode() + tableName.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof LocatorKey)
        return equals((LocatorKey) o);
      return false;
    }

    public boolean equals(LocatorKey lk) {
      return instanceId.equals(lk.instanceId) && tableName.equals(lk.tableName);
    }

  }

  private static HashMap<LocatorKey,TabletLocator> locators = new HashMap<LocatorKey,TabletLocator>();

  public static synchronized void clearLocators() {
    locators.clear();
  }

  public static synchronized TabletLocator getLocator(Instance instance, Text tableId) {

    LocatorKey key = new LocatorKey(instance.getInstanceID(), tableId);
    TabletLocator tl = locators.get(key);
    if (tl == null) {
      MetadataLocationObtainer mlo = new MetadataLocationObtainer(instance);

      if (tableId.toString().equals(RootTable.ID)) {
        tl = new RootTabletLocator(instance, new ZookeeperLockChecker(instance));
      } else if (tableId.toString().equals(MetadataTable.ID)) {
        tl = new TabletLocatorImpl(new Text(MetadataTable.ID), getLocator(instance, new Text(RootTable.ID)), mlo, new ZookeeperLockChecker(instance));
      } else {
        tl = new TabletLocatorImpl(tableId, getLocator(instance, new Text(MetadataTable.ID)), mlo, new ZookeeperLockChecker(instance));
      }
      locators.put(key, tl);
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

  public static class TabletLocation implements Comparable<TabletLocation> {
    private static final WeakHashMap<String,WeakReference<String>> tabletLocs = new WeakHashMap<String,WeakReference<String>>();

    private static String dedupeLocation(String tabletLoc) {
      synchronized (tabletLocs) {
        WeakReference<String> lref = tabletLocs.get(tabletLoc);
        if (lref != null) {
          String loc = lref.get();
          if (loc != null) {
            return loc;
          }
        }

        tabletLoc = new String(tabletLoc);
        tabletLocs.put(tabletLoc, new WeakReference<String>(tabletLoc));
        return tabletLoc;
      }
    }

    public final KeyExtent tablet_extent;
    public final String tablet_location;
    public final String tablet_session;

    public TabletLocation(KeyExtent tablet_extent, String tablet_location, String session) {
      ArgumentChecker.notNull(tablet_extent, tablet_location, session);
      this.tablet_extent = tablet_extent;
      this.tablet_location = dedupeLocation(tablet_location);
      this.tablet_session = dedupeLocation(session);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TabletLocation) {
        TabletLocation otl = (TabletLocation) o;
        return tablet_extent.equals(otl.tablet_extent) && tablet_location.equals(otl.tablet_location) && tablet_session.equals(otl.tablet_session);
      }
      return false;
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException("hashcode is not implemented for class " + this.getClass().toString());
    }

    @Override
    public String toString() {
      return "(" + tablet_extent + "," + tablet_location + "," + tablet_session + ")";
    }

    @Override
    public int compareTo(TabletLocation o) {
      int result = tablet_extent.compareTo(o.tablet_extent);
      if (result == 0) {
        result = tablet_location.compareTo(o.tablet_location);
        if (result == 0)
          result = tablet_session.compareTo(o.tablet_session);
      }
      return result;
    }
  }

  public static class TabletServerMutations<T extends Mutation> {
    private Map<KeyExtent,List<T>> mutations;
    private String tserverSession;

    public TabletServerMutations(String tserverSession) {
      this.tserverSession = tserverSession;
      this.mutations = new HashMap<KeyExtent,List<T>>();
    }

    public void addMutation(KeyExtent ke, T m) {
      List<T> mutList = mutations.get(ke);
      if (mutList == null) {
        mutList = new ArrayList<T>();
        mutations.put(ke, mutList);
      }

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
