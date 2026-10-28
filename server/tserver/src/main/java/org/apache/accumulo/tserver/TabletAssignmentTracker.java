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
package org.apache.accumulo.tserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSortedMap;

/*
 * Tracks the tablets assigned to a tablet server as they move from unopened to opening to online.
 * All three collections are guarded by a single private lock so that moving a tablet between them
 * is atomic.
 *
 * The set of online tablets is frequently read by many threads and infrequently updated, so an
 * immutable snapshot of it is kept up to date. Many threads can access the snapshot without
 * interfering with each other.
 */
class TabletAssignmentTracker {
  private static final Logger log = LoggerFactory.getLogger(TabletAssignmentTracker.class);

  // guards all three collections; also used to wake threads in awaitNotOpening, so any change
  // that removes a tablet from opening must call notifyAll
  private final Object lock = new Object();
  private final SortedSet<KeyExtent> unopened = new TreeSet<>();
  private final SortedSet<KeyExtent> opening = new TreeSet<>();
  private final SortedMap<KeyExtent,Tablet> online = new TreeMap<>();
  private volatile SortedMap<KeyExtent,Tablet> onlineSnapshot = Collections.emptySortedMap();

  static class Overlapping {
    final Set<KeyExtent> unopened;
    final Set<KeyExtent> opening;
    // the tablets are captured under the lock so callers do not need to look them up again
    final Map<KeyExtent,Tablet> online;

    Overlapping(Set<KeyExtent> unopened, Set<KeyExtent> opening, Map<KeyExtent,Tablet> online) {
      this.unopened = unopened;
      this.opening = opening;
      this.online = online;
    }

    boolean isEmpty() {
      return unopened.isEmpty() && opening.isEmpty() && online.isEmpty();
    }
  }

  /**
   * Adds the extent to the unopened set if it does not overlap any unopened, opening, or online
   * tablet.
   *
   * @return null if the extent was added, otherwise the tablets it overlaps
   */
  Overlapping addUnopenedIfNoOverlap(KeyExtent extent) {
    synchronized (lock) {
      // Checking if the current tablet is in any of the sets
      // below is not a strong enough check to catch all overlapping tablets
      // when splits and fix splits are occurring
      Overlapping overlapping = new Overlapping(KeyExtent.findOverlapping(extent, unopened),
          KeyExtent.findOverlapping(extent, opening), KeyExtent.findOverlapping(extent, online)
              .stream().collect(Collectors.toMap(ke -> ke, online::get)));
      if (!overlapping.isEmpty()) {
        return overlapping;
      }
      unopened.add(extent);
      return null;
    }
  }

  /**
   * Moves the extent from unopened to opening.
   *
   * @return false if the extent is already opening or online, or is no longer unopened
   * @throws IllegalStateException if the extent overlaps another unopened, opening, or online
   *         tablet
   */
  boolean unopenedToOpening(KeyExtent extent) {
    synchronized (lock) {
      // nothing should be moving between sets, do a sanity check
      Set<KeyExtent> unopenedOverlapping = KeyExtent.findOverlapping(extent, unopened);
      Set<KeyExtent> openingOverlapping = KeyExtent.findOverlapping(extent, opening);
      Set<KeyExtent> onlineOverlapping = KeyExtent.findOverlapping(extent, online);

      if (openingOverlapping.contains(extent) || onlineOverlapping.contains(extent)) {
        return false;
      }

      if (!unopenedOverlapping.contains(extent)) {
        log.info("assignment {} no longer in the unopened set", extent);
        return false;
      }

      if (unopenedOverlapping.size() != 1 || !openingOverlapping.isEmpty()
          || !onlineOverlapping.isEmpty()) {
        throw new IllegalStateException(
            "overlaps assigned " + extent + " " + !unopened.contains(extent) + " "
                + unopenedOverlapping + " " + openingOverlapping + " " + onlineOverlapping);
      }

      unopened.remove(extent);
      opening.add(extent);
      return true;
    }
  }

  void openingToOnline(KeyExtent extent, Tablet tablet) {
    synchronized (lock) {
      opening.remove(extent);
      online.put(extent, tablet);
      onlineSnapshot = ImmutableSortedMap.copyOf(online);
      lock.notifyAll();
    }
  }

  /**
   * Removes the extent from opening and adds unopenedExtent to unopened so its assignment can be
   * retried. The two extents differ when a split was rolled back.
   */
  void openingToUnopened(KeyExtent extent, KeyExtent unopenedExtent) {
    synchronized (lock) {
      opening.remove(extent);
      unopened.add(unopenedExtent);
      lock.notifyAll();
    }
  }

  void removeOpening(KeyExtent extent) {
    synchronized (lock) {
      opening.remove(extent);
      lock.notifyAll();
    }
  }

  boolean removeUnopened(KeyExtent extent) {
    synchronized (lock) {
      return unopened.remove(extent);
    }
  }

  /**
   * Blocks until the extent is no longer opening, either because it came online or failed to open.
   */
  void awaitNotOpening(KeyExtent extent) {
    synchronized (lock) {
      while (opening.contains(extent)) {
        try {
          log.info("Waiting for tablet {} to finish opening before unloading.", extent);
          lock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  void removeOnline(KeyExtent extent) {
    synchronized (lock) {
      online.remove(extent);
      onlineSnapshot = ImmutableSortedMap.copyOf(online);
    }
  }

  void split(KeyExtent oldTablet, Tablet newTablet1, Tablet newTablet2) {
    synchronized (lock) {
      online.remove(oldTablet);
      online.put(newTablet1.getExtent(), newTablet1);
      online.put(newTablet2.getExtent(), newTablet2);
      onlineSnapshot = ImmutableSortedMap.copyOf(online);
    }
  }

  SortedMap<KeyExtent,Tablet> getOnline() {
    return onlineSnapshot;
  }

  /**
   * Gets a consistent snapshot of the opening and online tablets, this avoids missing any tablets
   * that are moving between them.
   */
  OpeningAndOnlineCompactables getOpeningAndOnline() {
    synchronized (lock) {
      return new OpeningAndOnlineCompactables(Set.copyOf(opening), onlineSnapshot);
    }
  }

  List<KeyExtent> getUnopenedAndOpening() {
    synchronized (lock) {
      List<KeyExtent> copy = new ArrayList<>(unopened.size() + opening.size());
      copy.addAll(unopened);
      copy.addAll(opening);
      return copy;
    }
  }

  int getOpeningCount() {
    synchronized (lock) {
      return opening.size();
    }
  }

  int getUnopenedCount() {
    synchronized (lock) {
      return unopened.size();
    }
  }
}
