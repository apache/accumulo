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

import static org.apache.accumulo.core.clientImpl.TabletLocatorImpl.END_ROW_COMPARATOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;

public class ConcurrentTabletLocator extends TabletLocator {

  private ConcurrentSkipListMap<Text,TabletLocation> metaCache =
      new ConcurrentSkipListMap<>(END_ROW_COMPARATOR);
  private BlockingQueue<Text> lookupQueue = new LinkedBlockingQueue<>();
  private final Lock lookupLock = new ReentrantLock();

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }

    // this does no locking when all the needed info is in the cache already and is much simpler
    // than the code in TabletLocatorImpl which tries w/ a read lock first and on miss switches to a
    // write lock
    TabletLocation tl = locateTabletInCache(row);
    while (tl == null) {
      // TODO sleep w/ backoff
      requestLookup(row);
      tl = locateTabletInCache(row);
    }

    return tl;
  }

  /**
   * This function gathers all work from all threads that currently need to do metadata lookups and
   * processes them all together. The goal of this function is to avoid N threads that all want the
   * same tablet metadata from doing N metadata lookups. This function attempt to take the N
   * metadata lookup needs and reduce them to a single metadata lookup for the client.
   *
   * @param row
   */
  private void requestLookup(Text row) {
    // Add lookup request to queue outside the lock, whatever thread gets the lock will process this
    // work. If a thread is currently processing a lookup, then new request will build up in the
    // queue until its done and when its done one thread will process all the work for all waiting
    // threads.
    lookupQueue.add(row);
    lookupLock.lock();
    try {
      if (lookupQueue.isEmpty()) {
        // some other thread processed our request, so nothing to do
        return;
      }

      // TODO could filter out anything that is now in the cache, could have been added since the
      // lookup miss and there may not be anything to do at this point

      ArrayList<Text> lookupsToProcess = new ArrayList<>();
      lookupQueue.drainTo(lookupsToProcess);

      // TODO process all the queued work from all threads in lookupsToProcess using a batch scanner
      // and update the cache. Could collapse the requested lookups into ranges and use a batch
      // scanner to get the top N tablets for each range.

    } finally {
      lookupLock.unlock();
    }

  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    // TODO handle empty mutations, otherwise this could loop forever

    // this is also much simpler than the existing code because it does not do the switch from read
    // lock to write lock
    do {
      TabletLocatorImpl.LockCheckerSession lcSession = null;
      ArrayList<T> notInCache = new ArrayList<>();
      Text row = new Text();

      for (T mutation : mutations) {
        row.set(mutation.getRow());
        TabletLocation tl = locateTabletInCache(row);
        if (tl == null || !addMutation(binnedMutations, mutation, tl, lcSession)) {
          notInCache.add(mutation);
        }
      }

      if (!notInCache.isEmpty()) {
        binnedMutations.clear();
        // TODO sleep w/ backoff
        // request coordinated lookup of the missing extents
        requestLookups(notInCache);
      }

    } while (binnedMutations.isEmpty());
  }

  private <T extends Mutation> void requestLookups(ArrayList<T> notInCache) {
    // TODO
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    // TODO implement
    throw new UnsupportedOperationException();
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    metaCache.remove(getCacheKey(failedExtent));
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    keySet.forEach(extent -> metaCache.remove(getCacheKey(extent)));
  }

  @Override
  public void invalidateCache() {
    metaCache.clear();
  }

  private BlockingQueue<String> serverInvalidationQueue = new LinkedBlockingQueue<>();
  private final Lock serverInvalidationLock = new ReentrantLock();

  @Override
  public void invalidateCache(ClientContext context, String server) {
    // This method is structured so that when lots of threads attempt to invalidate servers at
    // around the same time the amount of full scans of the metadata cache is minimized.
    serverInvalidationQueue.add(server);
    serverInvalidationLock.lock();
    try {
      if (serverInvalidationQueue.isEmpty()) {
        // some other thread invalidated our server so nothing to do
        return;
      }
      HashSet<String> serversToInvalidate = new HashSet<>();
      serverInvalidationQueue.drainTo(serversToInvalidate);
      metaCache.values().removeIf(tl -> serversToInvalidate.contains(tl.tablet_location));
    } finally {
      serverInvalidationLock.unlock();
    }
  }

  private Text getCacheKey(KeyExtent extent) {
    // TODO handle null end row
    return extent.endRow();
  }

  // TODO was copied from TabletLocatorImpl, could share code w/ TabletLocatorImpl
  private TabletLocation locateTabletInCache(Text row) {

    Map.Entry<Text,TabletLocation> entry = metaCache.ceilingEntry(row);

    if (entry != null) {
      KeyExtent ke = entry.getValue().tablet_extent;
      if (ke.prevEndRow() == null || ke.prevEndRow().compareTo(row) < 0) {
        return entry.getValue();
      }
    }
    return null;
  }

  // TODO was copied from TabletLocatorImpl, could share code w/ TabletLocatorImpl
  private <T extends Mutation> boolean addMutation(
      Map<String,TabletServerMutations<T>> binnedMutations, T mutation, TabletLocation tl,
      TabletLocatorImpl.LockCheckerSession lcSession) {
    TabletServerMutations<T> tsm = binnedMutations.get(tl.tablet_location);

    if (tsm == null) {
      // do lock check once per tserver here to make binning faster
      boolean lockHeld = lcSession.checkLock(tl) != null;
      if (lockHeld) {
        tsm = new TabletServerMutations<>(tl.tablet_session);
        binnedMutations.put(tl.tablet_location, tsm);
      } else {
        return false;
      }
    }

    // its possible the same tserver could be listed with different sessions
    if (tsm.getSession().equals(tl.tablet_session)) {
      tsm.addMutation(tl.tablet_extent, mutation);
      return true;
    }

    return false;
  }
}
