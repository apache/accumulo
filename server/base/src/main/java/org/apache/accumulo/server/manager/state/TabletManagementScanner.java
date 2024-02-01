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
package org.apache.accumulo.server.manager.state;

import java.io.IOException;
import java.lang.ref.Cleaner.Cleanable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.clientImpl.ClientTabletCache.LocationNeed;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletManagementScanner implements ClosableIterator<TabletManagement> {

  private static final Logger log = LoggerFactory.getLogger(TabletManagementScanner.class);
  private static final List<Range> ALL_TABLETS_RANGE = List.of(TabletsSection.getRange());

  private final Cleanable cleanable;
  private final BatchScanner mdScanner;
  private final Iterator<Entry<Key,Value>> iter;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  // This constructor is called from TabletStateStore implementations
  public TabletManagementScanner(ClientContext context, List<Range> ranges,
      TabletManagementParameters tmgmtParams, String tableName) {
    // scan over metadata table, looking for tablets in the wrong state based on the live servers
    // and online tables
    try {
      int numLocations = 0;
      try {
        final TableId tid = context.getTableId(tableName);
        final ClientTabletCache locator = ClientTabletCache.getInstance(context, tid);
        final Set<String> locations = new HashSet<>();
        final List<Range> failures = locator.findTablets(context, ALL_TABLETS_RANGE,
            (ct, r) -> ct.getTserverLocation().ifPresent(locations::add),
            LocationNeed.NOT_REQUIRED);
        // If failures is not empty, then there are tablets that we don't know the location of.
        // In this case, add an extra thread.
        numLocations = (failures.isEmpty()) ? locations.size() : locations.size() + 1;
      } catch (InvalidTabletHostingRequestException e) {
        // this should not happen as we are using NOT_REQUIRED
        throw new IllegalStateException(
            "InvalidTabletHostingRequestException raised when using LocationNeed.NOT_REQUIRED");
      }
      int numThreads = Math.min(numLocations,
          context.getConfiguration().getCount(Property.MANAGER_TABLET_GROUP_WATCHER_SCAN_THREADS));
      numThreads = Math.max(1, numThreads);
      mdScanner = context.createBatchScanner(tableName, Authorizations.EMPTY, numThreads);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Metadata table " + tableName + " should exist", e);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException("Error obtaining locations for table: " + tableName);
    }
    cleanable = CleanerUtil.unclosed(this, TabletManagementScanner.class, closed, log, mdScanner);
    TabletManagementIterator.configureScanner(mdScanner, tmgmtParams);
    mdScanner.setRanges(ranges);
    iter = mdScanner.iterator();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // deregister cleanable, but it won't run because it checks
      // the value of closed first, which is now true
      cleanable.clean();
      mdScanner.close();
    }
  }

  @Override
  public boolean hasNext() {
    if (closed.get()) {
      return false;
    }

    boolean result = iter.hasNext();
    if (!result) {
      close();
    }
    return result;
  }

  @Override
  public TabletManagement next() {
    if (closed.get()) {
      throw new NoSuchElementException(this.getClass().getSimpleName() + " is closed");
    }

    Entry<Key,Value> e = iter.next();
    try {
      TabletManagement tm = TabletManagementIterator.decode(e);
      log.trace(
          "Returning metadata tablet, extent: {}, tabletAvailability: {}, actions: {}, error: {}",
          tm.getTabletMetadata().getExtent(), tm.getTabletMetadata().getTabletAvailability(),
          tm.getActions(), tm.getErrorMessage());
      return tm;
    } catch (IOException e1) {
      throw new RuntimeException("Error creating TabletMetadata object", e1);
    }
  }

}
