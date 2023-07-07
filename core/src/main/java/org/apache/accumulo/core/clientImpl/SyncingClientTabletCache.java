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
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
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
public class SyncingClientTabletCache extends ClientTabletCache {

  private volatile ClientTabletCache locator;
  private final Supplier<ClientTabletCache> getLocatorFunction;

  public SyncingClientTabletCache(Supplier<ClientTabletCache> getLocatorFunction) {
    this.getLocatorFunction = getLocatorFunction;
    this.locator = getLocatorFunction.get();
  }

  public SyncingClientTabletCache(final ClientContext context, final TableId tableId) {
    this(() -> ClientTabletCache.getInstance(context, tableId));
  }

  private ClientTabletCache syncLocator() {
    ClientTabletCache loc = this.locator;
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
  public CachedTablet findTablet(ClientContext context, Text row, boolean skipRow,
      LocationNeed locationNeed, int minimumHostAhead, Range hostAheadRange)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {
    return syncLocator().findTablet(context, row, skipRow, locationNeed, minimumHostAhead,
        hostAheadRange);
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {
    syncLocator().binMutations(context, mutations, binnedMutations, failures);
  }

  @Override
  public List<Range> findTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer, LocationNeed locationNeed)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      InvalidTabletHostingRequestException {
    return syncLocator().findTablets(context, ranges, rangeConsumer, locationNeed);
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, InvalidTabletHostingRequestException {
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
