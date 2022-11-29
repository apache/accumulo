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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;

public class OfflineTableExtentCache {

  private static final Logger LOG = LoggerFactory.getLogger(OfflineTableExtentCache.class);

  private static class KeyExtentCacheLoader
      implements CacheLoader<Pair<TableId,Range>,List<KeyExtent>> {

    private final ClientContext ctx;

    public KeyExtentCacheLoader(ClientContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public List<KeyExtent> load(Pair<TableId,Range> pair) throws Exception {
      final TableId tid = pair.getFirst();
      final Range r = pair.getSecond();
      LOG.trace("Loading extents for tid: {} and range: {}", tid, r);
      TabletsMetadata tm = ctx.getAmple().readTablets().forTable(tid)
          .overlapping(r.getStartKey() == null ? null : r.getStartKey().getRow(),
              r.isStartKeyInclusive(), r.getEndKey() == null ? null : r.getEndKey().getRow())
          .build();
      List<KeyExtent> result = new ArrayList<>();
      for (TabletMetadata t : tm) {
        LOG.trace("Evaluating: {}", t.getExtent());
        if (t.getExtent().endRow() == null || r.getEndKey() == null
            || t.getExtent().endRow() != null
                && r.getEndKey().compareTo(new Key(t.getExtent().endRow()), PartialKey.ROW) >= 0) {
          LOG.trace("Adding: {}", t.getExtent());
          result.add(t.getExtent());
        }
      }
      LOG.trace("Returning extents: {}", result);
      return result;
    }

    @Override
    public Map<? extends Pair<TableId,Range>,? extends List<KeyExtent>>
        loadAll(Set<? extends Pair<TableId,Range>> pairs) throws Exception {

      // Sort the inputs
      TreeMap<TableId,SortedSet<Range>> inputs = new TreeMap<>();
      pairs.forEach(p -> {
        inputs.computeIfAbsent(p.getFirst(), (t) -> new TreeSet<Range>(Range::compareTo))
            .add(p.getSecond());
      });
      LOG.trace("Loading extents for {}", inputs);

      Map<Pair<TableId,Range>,List<KeyExtent>> output = new HashMap<>();
      for (Entry<TableId,SortedSet<Range>> e : inputs.entrySet()) {
        for (Range r : e.getValue()) {
          Pair<TableId,Range> pair = new Pair<>(e.getKey(), r);
          List<KeyExtent> ke = load(pair);
          if (ke != null) {
            output.put(pair, ke);
          }
        }
      }
      LOG.trace("Returning extents: {}", output);
      return output;
    }

  }

  private final LoadingCache<Pair<TableId,Range>,List<KeyExtent>> cache;
  private final ClientContext ctx;

  OfflineTableExtentCache(ClientContext ctx) {
    this.ctx = ctx;
    cache = Caffeine.newBuilder().scheduler(Scheduler.systemScheduler())
        .expireAfterAccess(Duration.ofMinutes(3)).build(new KeyExtentCacheLoader(this.ctx));
  }

  public Map<Pair<TableId,Range>,List<KeyExtent>> lookup(TableId tid, List<Range> r) {
    List<Pair<TableId,Range>> inputs = new ArrayList<>();
    r.forEach(range -> {
      inputs.add(new Pair<>(tid, range));
    });
    return cache.getAll(inputs);
  }

  public List<KeyExtent> lookup(TableId tid, Range r) {
    return cache.get(new Pair<TableId,Range>(tid, r));
  }

  public void invalidate(TableId tid) {
    Set<Pair<TableId,Range>> removals = new HashSet<>();
    cache.asMap().keySet().forEach(pair -> {
      if (pair.getFirst().equals(tid)) {
        removals.add(pair);
      }
    });
    cache.invalidateAll(removals);
  }

  public void invalidate(TableId tid, List<Range> ranges) {
    ranges.forEach(r -> invalidate(tid, r));
  }

  public void invalidate(TableId tid, Range r) {
    cache.invalidate(new Pair<>(tid, r));
    LOG.trace("Invalidated cache entry for table {}, range: {}", tid, r);
  }

}
