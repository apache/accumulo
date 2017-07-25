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
package org.apache.accumulo.core.client.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Syncs itself with the static collection of TabletLocators, so that when the server clears it, it will automatically get the most up-to-date version. Caching
 * TabletLocators locally is safe when using SyncingTabletLocator.
 */
public class SyncingTabletLocator extends TabletLocator {
  private static final Logger log = Logger.getLogger(SyncingTabletLocator.class);

  private volatile TabletLocator locator;
  private final Callable<TabletLocator> getLocatorFunction;

  public SyncingTabletLocator(Callable<TabletLocator> getLocatorFunction) {
    this.getLocatorFunction = getLocatorFunction;
    try {
      this.locator = getLocatorFunction.call();
    } catch (Exception e) {
      log.error("Problem obtaining TabletLocator", e);
      throw new RuntimeException(e);
    }
  }

  public SyncingTabletLocator(final ClientContext context, final Table.ID tableId) {
    this(new Callable<TabletLocator>() {
      @Override
      public TabletLocator call() throws Exception {
        return TabletLocator.getLocator(context, tableId);
      }
    });
  }

  private TabletLocator syncLocator() {
    TabletLocator loc = this.locator;
    if (!loc.isValid())
      synchronized (this) {
        if (locator == loc)
          try {
            loc = locator = getLocatorFunction.call();
          } catch (Exception e) {
            log.error("Problem obtaining TabletLocator", e);
            throw new RuntimeException(e);
          }
      }
    return loc;
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    return syncLocator().locateTablet(context, row, skipRow, retry);
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    syncLocator().binMutations(context, mutations, binnedMutations, failures);
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
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
  public void invalidateCache(Instance instance, String server) {
    syncLocator().invalidateCache(instance, server);
  }
}
