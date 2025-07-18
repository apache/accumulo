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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class OpeningTablets {
  private static final Logger log = LoggerFactory.getLogger(OpeningTablets.class);

  // Tracks the set of data levels that are shutting down and can not load new tablets.
  final Set<Ample.DataLevel> shutdownDataLevels = EnumSet.noneOf(Ample.DataLevel.class);
  final SortedSet<KeyExtent> openingTablets = new TreeSet<>();

  public synchronized boolean add(KeyExtent extent) {
    if (shutdownDataLevels.contains(Ample.DataLevel.of(extent.tableId()))) {
      log.info("ignoring request to load {} because the data level is shutdown", extent);
      return false;
    }

    return openingTablets.add(extent);
  }

  public synchronized boolean remove(KeyExtent extent) {
    return openingTablets.remove(extent);
  }

  public synchronized boolean contains(KeyExtent extent) {
    return openingTablets.contains(extent);
  }

  /**
   * Prevents loading any new tablets for the given data level and waits for any tablets in the
   * level that are currently loading.
   */
  public synchronized void stopLoadingTablets(Ample.DataLevel level) {
    while (openingTablets.stream()
        .anyMatch(extent -> Ample.DataLevel.of(extent.tableId()) == level)) {
      log.debug("Waiting for {} opening tablets for level {} during shutdown", openingTablets
          .stream().filter(extent -> Ample.DataLevel.of(extent.tableId()) == level).count(), level);
      try {
        wait(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    shutdownDataLevels.add(level);
  }

  public synchronized int size() {
    return openingTablets.size();
  }

  /**
   * The calling thread must hold the monitor lock on this object when calling this and while using
   * the returned set.
   *
   * @return an immutable view of the tablets, does not copy the set.
   */
  public SortedSet<KeyExtent> immutableView() {
    Preconditions.checkState(Thread.holdsLock(this));
    return Collections.unmodifiableSortedSet(openingTablets);
  }
}
