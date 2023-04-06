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
package org.apache.accumulo.core.clientImpl.bulk;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

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

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.bulk.BulkImport.KeyExtentCache;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

class ConcurrentKeyExtentCache implements KeyExtentCache {

  private static Logger log = LoggerFactory.getLogger(ConcurrentKeyExtentCache.class);

  private static final Text MAX = new Text();

  private Set<Text> rowsToLookup = Collections.synchronizedSet(new HashSet<>());

  List<Text> lookupRows = new ArrayList<>();

  private ConcurrentSkipListMap<Text,KeyExtent> extents = new ConcurrentSkipListMap<>((t1, t2) -> {
    return (t1 == t2) ? 0 : (t1 == MAX ? 1 : (t2 == MAX ? -1 : t1.compareTo(t2)));
  });
  private TableId tableId;
  private ClientContext ctx;

  ConcurrentKeyExtentCache(TableId tableId, ClientContext ctx) {
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
    return Objects.equals(e, extents.get(e.endRow() == null ? MAX : e.endRow()));
  }

  @VisibleForTesting
  protected void updateCache(KeyExtent e) {
    Text prevRow = e.prevEndRow() == null ? new Text() : e.prevEndRow();
    Text endRow = e.endRow() == null ? MAX : e.endRow();
    extents.subMap(prevRow, e.prevEndRow() == null, endRow, true).clear();
    extents.put(endRow, e);
  }

  @VisibleForTesting
  protected Stream<KeyExtent> lookupExtents(Text row) {
    return TabletsMetadata.builder(ctx).forTable(tableId).overlapping(row, true, null)
        .checkConsistency().fetch(PREV_ROW).build().stream().limit(100)
        .map(TabletMetadata::getExtent);
  }

  @Override
  public KeyExtent lookup(Text row) {
    while (true) {

      KeyExtent ke = getFromCache(row);
      if (ke != null) {
        return ke;
      }

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
            while (true) {
              try {
                Iterator<KeyExtent> iter = lookupExtents(lookupRow).iterator();
                while (iter.hasNext()) {
                  KeyExtent ke2 = iter.next();
                  if (inCache(ke2)) {
                    break;
                  }
                  updateCache(ke2);
                }
                break;
              } catch (TabletDeletedException tde) {
                // tablets were merged away in the table, start over and try again
                log.debug("While trying to obtain a tablet location for bulk import, a tablet was "
                    + "deleted. If this was caused by a concurrent merge tablet "
                    + "operation, this is okay. Otherwise, it could be a problem.", tde);
              }
            }
          }
        }
      }
    }
  }
}
