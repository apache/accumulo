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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;

/**
 * Syncs itself with the static collection of TabletLocators, so that when the server clears it, it
 * will automatically get the most up-to-date version. Caching TabletLocators locally is safe when
 * using SyncingTabletLocator.
 */
public class SyncingTabletLocator extends TabletLocator {

  private volatile TabletLocator locator;
  private final Supplier<TabletLocator> getLocatorFunction;

  public SyncingTabletLocator(Supplier<TabletLocator> getLocatorFunction) {
    this.getLocatorFunction = getLocatorFunction;
    this.locator = getLocatorFunction.get();
  }

  public SyncingTabletLocator(final ClientContext context, final TableId tableId) {
    this(() -> TabletLocator.getLocator(context, tableId));
  }

  private TabletLocator syncLocator() {
    TabletLocator loc = this.locator;
    if (!loc.isValid()) {
      synchronized (this) {
        if (locator == loc) {
          loc = locator = getLocatorFunction.get();
        }
      }
    }
    return loc;
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      HostingNeed hostingNeed)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return syncLocator().locateTablet(context, row, skipRow, hostingNeed);
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    syncLocator().binMutations(context, mutations, binnedMutations, failures);
  }

  @Override
  public List<Range> locateTablets(ClientContext context, List<Range> ranges,
      BiConsumer<TabletLocation,Range> rangeConsumer, HostingNeed hostingNeed)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return syncLocator().locateTablets(context, ranges, rangeConsumer, hostingNeed);
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return syncLocator().binRanges(context, ranges, binnedRanges);
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    syncLocator().invalidateCache(failedExtent);
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    syncLocator().invalidateCache(keySet);
  }

  @Override
  public void invalidateCache() {
    syncLocator().invalidateCache();
  }

  @Override
  public void invalidateCache(ClientContext context, String server) {
    syncLocator().invalidateCache(context, server);
  }
}
