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
 * All methods synchronize on this object so that moving a tablet between collections is atomic.
 * It is also used to wake threads in awaitNotOpening, so any change that removes a tablet from
 * opening must call notifyAll.
 *
 * The set of online tablets is frequently read by many threads and infrequently updated, so an
 * immutable snapshot of it is kept up to date. Many threads can access the snapshot without
 * interfering with each other.
 */
class TabletAssignmentTracker {
  private static final Logger log = LoggerFactory.getLogger(TabletAssignmentTracker.class);

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
      this.unopened = Collections.unmodifiableSet(unopened);
      this.opening = Collections.unmodifiableSet(opening);
      this.online = Collections.unmodifiableMap(online);
    }

    boolean isEmpty() {
      return unopened.isEmpty() && opening.isEmpty() && online.isEmpty();
    }
  }

  /**
   * Adds the extent to the unopened set if it does not overlap any unopened, opening, or online
   * tablet.
   *
   * @return the tablets the extent overlaps, empty if the extent was added
   */
  synchronized Overlapping addUnopenedIfNoOverlap(KeyExtent extent) {
    // Checking if the current tablet is in any of the sets
    // below is not a strong enough check to catch all overlapping tablets
    // when splits and fix splits are occurring
    Overlapping overlapping = new Overlapping(KeyExtent.findOverlapping(extent, unopened),
        KeyExtent.findOverlapping(extent, opening), KeyExtent.findOverlapping(extent, online)
            .stream().collect(Collectors.toMap(ke -> ke, online::get)));
    if (overlapping.isEmpty()) {
      unopened.add(extent);
    }
    return overlapping;
  }

  /**
   * Moves the extent from unopened to opening.
   *
   * @return false if the extent is already opening or online, or is no longer unopened
   * @throws IllegalStateException if the extent overlaps another unopened, opening, or online
   *         tablet
   */
  synchronized boolean unopenedToOpening(KeyExtent extent) {
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

  synchronized void openingToOnline(KeyExtent extent, Tablet tablet) {
    opening.remove(extent);
    online.put(extent, tablet);
    onlineSnapshot = ImmutableSortedMap.copyOf(online);
    notifyAll();
  }

  /**
   * Removes the extent from opening and adds unopenedExtent to unopened so its assignment can be
   * retried. The two extents differ when a split was rolled back.
   */
  synchronized void openingToUnopened(KeyExtent extent, KeyExtent unopenedExtent) {
    opening.remove(extent);
    unopened.add(unopenedExtent);
    notifyAll();
  }

  synchronized void removeOpening(KeyExtent extent) {
    opening.remove(extent);
    notifyAll();
  }

  synchronized boolean removeUnopened(KeyExtent extent) {
    return unopened.remove(extent);
  }

  /**
   * Blocks until the extent is no longer opening, either because it came online or failed to open.
   */
  synchronized void awaitNotOpening(KeyExtent extent) {
    boolean interrupted = false;
    while (opening.contains(extent)) {
      try {
        log.info("Waiting for tablet {} to finish opening before unloading.", extent);
        wait();
      } catch (InterruptedException e) {
        // re-interrupting here would make every later wait() throw without releasing the monitor
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  synchronized void removeOnline(KeyExtent extent) {
    online.remove(extent);
    onlineSnapshot = ImmutableSortedMap.copyOf(online);
  }

  synchronized void split(KeyExtent oldTablet, Tablet newTablet1, Tablet newTablet2) {
    online.remove(oldTablet);
    online.put(newTablet1.getExtent(), newTablet1);
    online.put(newTablet2.getExtent(), newTablet2);
    onlineSnapshot = ImmutableSortedMap.copyOf(online);
  }

  SortedMap<KeyExtent,Tablet> getOnline() {
    return onlineSnapshot;
  }

  synchronized Set<KeyExtent> getOpening() {
    return Set.copyOf(opening);
  }

  synchronized List<KeyExtent> getUnopenedAndOpening() {
    List<KeyExtent> copy = new ArrayList<>(unopened.size() + opening.size());
    copy.addAll(unopened);
    copy.addAll(opening);
    return Collections.unmodifiableList(copy);
  }

  synchronized int getOpeningCount() {
    return opening.size();
  }

  synchronized int getUnopenedCount() {
    return unopened.size();
  }
}
