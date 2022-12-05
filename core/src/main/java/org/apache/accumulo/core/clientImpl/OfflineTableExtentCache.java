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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;

public class OfflineTableExtentCache {

  private static final Logger LOG = LoggerFactory.getLogger(OfflineTableExtentCache.class);

  private final Cache<TableId,TreeMap<Range,List<KeyExtent>>> cache;
  private final ClientContext ctx;

  OfflineTableExtentCache(ClientContext ctx) {
    this.ctx = ctx;
    cache = Caffeine.newBuilder().scheduler(Scheduler.systemScheduler())
        .expireAfterAccess(Duration.ofMinutes(10)).build();
  }

  private List<KeyExtent> loadExtentsForRange(TableId tid, Range r) {
    LOG.trace("Loading extents for tid: {} and range: {}", tid, r);
    TabletsMetadata tm =
        ctx.getAmple().readTablets().forTable(tid)
            .overlapping(r.getStartKey() == null ? null : r.getStartKey().getRow(),
                r.isStartKeyInclusive(), r.getEndKey() == null ? null : r.getEndKey().getRow())
            .build();
    List<KeyExtent> result = new ArrayList<>();
    for (TabletMetadata t : tm) {
      LOG.trace("Evaluating: {}", t.getExtent());
      if (t.getExtent().endRow() == null || r.getEndKey() == null || t.getExtent().endRow() != null
          && r.getEndKey().compareTo(new Key(t.getExtent().endRow()), PartialKey.ROW) >= 0) {
        LOG.trace("Adding: {}", t.getExtent());
        result.add(t.getExtent());
      }
    }
    LOG.trace("Loaded extents: {}", result);
    return result;
  }

  public Map<Range,List<KeyExtent>> lookup(TableId tid, List<Range> ranges) {
    LOG.trace("Performing lookup for table: {}, range: {}", tid, ranges);
    Map<Range,List<KeyExtent>> results = new TreeMap<>();
    TreeMap<Range,List<KeyExtent>> extentMap = cache.get(tid, (t) -> {
      return new TreeMap<Range,List<KeyExtent>>();
    });

    Collections.sort(ranges);
    ranges.forEach(r -> {
      // Find an existing range in the extentMap that overlaps the scan range
      boolean foundRange = false;
      Range rangeBefore = extentMap.floorKey(r);
      if (rangeBefore != null) {
        Map<Range,List<KeyExtent>> tm = extentMap.tailMap(rangeBefore);
        for (Entry<Range,List<KeyExtent>> e : tm.entrySet()) {
          if (e.getKey().overlaps(r)) {
            // Ignore extents that don't contain the scan range
            List<KeyExtent> extents = new ArrayList<>();
            if (r.getStartKey() == null) {
              extents.addAll(e.getValue());
            } else {
              e.getValue().forEach(extent -> {
                Text row = r.isStartKeyInclusive() ? r.getStartKey().getRow()
                    : r.getStartKey().followingKey(PartialKey.ROW).getRow();
                if (extent.contains(row)) {
                  extents.add(extent);
                }
              });
            }
            results.put(r, extents);
            foundRange = true;
            break;
          }
        }
      }
      if (!foundRange) {
        List<KeyExtent> extents = loadExtentsForRange(tid, r);
        if (extents != null) {
          extentMap.put(r, extents);
          results.put(r, extents);
        }
      }
    });
    // remove any overlapping from the cache
    removeOverlapping(extentMap);
    LOG.trace("Returning extents: {}", extentMap);
    return results;
  }

  private void removeOverlapping(TreeMap<Range,List<KeyExtent>> map) {

    Range idx = map.firstKey();
    Set<Range> removals = new HashSet<>();
    for (Range range : map.keySet()) {
      if (range == idx) {
        continue;
      }
      if (idx.overlaps(range)) {
        LOG.trace("Removing {} because {} overlaps it", range, idx);
        removals.add(range);
      } else {
        idx = range;
      }
    }
    removals.forEach(map::remove);
  }

  public List<KeyExtent> lookup(TableId tid, Range r) {
    return lookup(tid, Collections.singletonList(r)).get(r);
  }

  public void invalidate(TableId tid) {
    cache.invalidate(tid);
    LOG.trace("Invalidated cache entry for table: {}", tid);
  }

  public void invalidate(TableId tid, List<Range> ranges) {
    TreeMap<Range,List<KeyExtent>> extentMap = cache.getIfPresent(tid);
    if (extentMap != null) {
      Collections.sort(ranges);
      ranges.forEach(r -> {
        extentMap.remove(r);
        LOG.trace("Invalidated cache entry for table {}, range: {}", tid, r);
      });
    }
  }

  public void invalidate(TableId tid, Range r) {
    invalidate(tid, Collections.singletonList(r));
  }

}
