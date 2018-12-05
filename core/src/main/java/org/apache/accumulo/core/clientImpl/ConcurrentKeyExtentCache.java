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
package org.apache.accumulo.core.clientImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.BulkImport.KeyExtentCache;
import org.apache.accumulo.core.clientImpl.Table.ID;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
class ConcurrentKeyExtentCache implements KeyExtentCache {

  private static final Text MAX = new Text();

  private Set<Text> rowsToLookup = Collections.synchronizedSet(new HashSet<>());

  List<Text> lookupRows = new ArrayList<>();

  private ConcurrentSkipListMap<Text,KeyExtent> extents = new ConcurrentSkipListMap<>((t1, t2) -> {
    return (t1 == t2) ? 0 : (t1 == MAX ? 1 : (t2 == MAX ? -1 : t1.compareTo(t2)));
  });
  private ID tableId;
  private ClientContext ctx;

  @VisibleForTesting
  ConcurrentKeyExtentCache(Table.ID tableId, ClientContext ctx) {
    this.tableId = tableId;
    this.ctx = ctx;
  }

  private KeyExtent getFromCache(Text row) {
    Entry<Text,KeyExtent> entry = extents.ceilingEntry(row);
    if (entry != null && entry.getValue().contains(row)) {
      return entry.getValue();
    }

    return null;
  }

  private boolean inCache(KeyExtent e) {
    return Objects.equals(e, extents.get(e.getEndRow() == null ? MAX : e.getEndRow()));
  }

  @VisibleForTesting
  protected void updateCache(KeyExtent e) {
    Text prevRow = e.getPrevEndRow() == null ? new Text() : e.getPrevEndRow();
    Text endRow = e.getEndRow() == null ? MAX : e.getEndRow();
    extents.subMap(prevRow, e.getPrevEndRow() == null, endRow, true).clear();
    extents.put(endRow, e);
  }

  @VisibleForTesting
  protected Stream<KeyExtent> lookupExtents(Text row)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    return TabletsMetadata.builder().forTable(tableId).overlapping(row, null).checkConsistency()
        .fetchPrev().build(ctx).stream().limit(100).map(TabletMetadata::getExtent);
  }

  @Override
  public KeyExtent lookup(Text row)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    while (true) {
      KeyExtent ke = getFromCache(row);
      if (ke != null)
        return ke;

      // If a metadata lookup is currently in progress, then multiple threads can queue up their
      // rows. The next lookup will process all queued. Processing multiple at once can be more
      // efficient.
      rowsToLookup.add(row);

      synchronized (this) {
        // This check is done to avoid processing rowsToLookup when the current thread's row is in
        // the cache.
        ke = getFromCache(row);
        if (ke != null) {
          rowsToLookup.remove(row);
          return ke;
        }

        lookupRows.clear();
        synchronized (rowsToLookup) {
          // Gather all rows that were queued for lookup before this point in time.
          rowsToLookup.forEach(lookupRows::add);
          rowsToLookup.clear();
        }
        // Lookup rows in the metadata table in sorted order. This could possibly lead to less
        // metadata lookups.
        lookupRows.sort(Text::compareTo);

        for (Text lookupRow : lookupRows) {
          if (getFromCache(lookupRow) == null) {
            Iterator<KeyExtent> iter = lookupExtents(lookupRow).iterator();
            while (iter.hasNext()) {
              KeyExtent ke2 = iter.next();
              if (inCache(ke2))
                break;
              updateCache(ke2);
            }
          }
        }
      }
    }
  }
}
