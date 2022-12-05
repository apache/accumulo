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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.tserver.tablet.Tablet;

import com.google.common.collect.Ordering;

/**
 * Computes the N tablets that have the highest deltas for a given monotonically increasing counter.
 */
public abstract class BusiestTracker {

  private Map<KeyExtent,Long> lastCounts = Collections.emptyMap();
  private final int numBusiestTabletsToLog;

  BusiestTracker(int numBusiestTabletsToLog) {
    this.numBusiestTabletsToLog = numBusiestTabletsToLog;
  }

  protected abstract long extractCount(Tablet tablet);

  public List<ComparablePair<Long,KeyExtent>> computeBusiest(Collection<Tablet> tablets) {

    HashMap<KeyExtent,Long> counts = new HashMap<>();

    ArrayList<ComparablePair<Long,KeyExtent>> tabletsWithDelta = new ArrayList<>();

    for (Tablet tablet : tablets) {
      KeyExtent extent = tablet.getExtent();

      // only get the count once to ensure consistency in the case of multiple threads
      long count = extractCount(tablet);

      if (count == 0) {
        continue;
      }

      counts.put(extent, count);

      long lastCount = lastCounts.getOrDefault(extent, 0L);

      // if a tablet leaves a tserver and comes back, then its count will be reset. This could make
      // lastCount higher than the current count. That is why lastCount > count is checked below.
      long delta = (lastCount > count) ? count : count - lastCount;

      // handle case where tablet had no activity
      if (delta > 0) {
        tabletsWithDelta.add(new ComparablePair<>(delta, extent));
      }
    }

    lastCounts = counts;

    return Ordering.natural().greatestOf(tabletsWithDelta, numBusiestTabletsToLog);
  }

  static BusiestTracker newBusiestIngestTracker(int numBusiestTabletsToLog) {
    return new BusiestTracker(numBusiestTabletsToLog) {
      @Override
      protected long extractCount(Tablet tablet) {
        return tablet.totalIngest();
      }
    };
  }

  static BusiestTracker newBusiestQueryTracker(int numBusiestTabletsToLog) {
    return new BusiestTracker(numBusiestTabletsToLog) {
      @Override
      protected long extractCount(Tablet tablet) {
        return tablet.totalQueriesResults();
      }
    };
  }
}
