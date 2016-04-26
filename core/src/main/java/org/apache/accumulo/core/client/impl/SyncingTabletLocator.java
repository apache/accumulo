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
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Credentials;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Syncs itself with the static collection of TabletLocators, so that when the server clears it, it will automatically get the most up-to-date version. Caching
 * TabletLocators locally is safe when using SyncingTabletLocator.
 */
public class SyncingTabletLocator extends TabletLocator {
  private static final Logger log = Logger.getLogger(SyncingTabletLocator.class);

  /*
   * Implementation: the invalidateCache calls do not need to call syncLocator because it is okay if an outdated locator is invalidated. In this case, on the
   * next call to a meaningful function like binRanges, the new locator will replace the current one and be used instead. The new locator is fresh; it has no
   * cache at all when created.
   */

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

  public SyncingTabletLocator(final Instance instance, final Text tableId) {
    this(new Callable<TabletLocator>() {
      @Override
      public TabletLocator call() throws Exception {
        return TabletLocator.getLocator(instance, tableId);
      }
    });
  }

  private void syncLocator() {
    if (!locator.isValid())
      synchronized (this) {
        if (!locator.isValid())
          try {
            locator = getLocatorFunction.call();
          } catch (Exception e) {
            log.error("Problem obtaining TabletLocator", e);
            throw new RuntimeException(e);
          }
      }
  }

  @Override
  public TabletLocation locateTablet(Credentials credentials, Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    syncLocator();
    return locator.locateTablet(credentials, row, skipRow, retry);
  }

  @Override
  public <T extends Mutation> void binMutations(Credentials credentials, List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations,
      List<T> failures) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    syncLocator();
    locator.binMutations(credentials, mutations, binnedMutations, failures);
  }

  @Override
  public List<Range> binRanges(Credentials credentials, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    syncLocator();
    return locator.binRanges(credentials, ranges, binnedRanges);
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    locator.invalidateCache(failedExtent);
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    locator.invalidateCache(keySet);
  }

  @Override
  public void invalidateCache() {
    locator.invalidateCache();
  }

  @Override
  public void invalidateCache(String server) {
    locator.invalidateCache(server);
  }

}
