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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class TabletRefresher {

  private static final Logger log = LoggerFactory.getLogger(TabletRefresher.class);

  public static void refresh(Manager manager, FateId fateId, TableId tableId, byte[] startRow,
      byte[] endRow, Predicate<TabletMetadata> needsRefresh) {

    try (var tablets = manager.getContext().getAmple().readTablets().forTable(tableId)
        .overlapping(startRow, endRow).checkConsistency()
        .fetch(ColumnType.LOADED, ColumnType.LOCATION, ColumnType.PREV_ROW).build()) {

      // Find all tablets that need to refresh their metadata. There may be some tablets that were
      // hosted after the tablet files were updated, it just results in an unneeded refresh
      // request. There may also be tablets that had a location when the files were set but do not
      // have a location now, that is ok the next time that tablet loads somewhere it will see the
      // files.

      var tabletIterator =
          tablets.stream().filter(tabletMetadata -> tabletMetadata.getLocation() != null)
              .filter(needsRefresh).iterator();

      // avoid reading all tablets into memory and instead process batches of 1000 tablets at a time
      Iterators.partition(tabletIterator, 1000).forEachRemaining(batch -> {
        var refreshesNeeded = batch.stream().collect(groupingBy(TabletMetadata::getLocation,
            mapping(tabletMetadata -> tabletMetadata.getExtent().toThrift(), toList())));

        refreshTablets(manager.getTabletRefreshThreadPool(), fateId.canonical(),
            manager.getContext(), () -> manager.onlineTabletServers(), refreshesNeeded);
      });

    }

  }

  public static void refreshTablets(ExecutorService threadPool, String logId, ServerContext context,
      Supplier<Set<TServerInstance>> onlineTserversSupplier,
      Map<TabletMetadata.Location,List<TKeyExtent>> refreshesNeeded) {

    // make a copy as it will be mutated in this method
    refreshesNeeded = new HashMap<>(refreshesNeeded);

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(100))
        .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofSeconds(1)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    while (!refreshesNeeded.isEmpty()) {

      Map<TabletMetadata.Location,Future<List<TKeyExtent>>> futures = new HashMap<>();

      for (Map.Entry<TabletMetadata.Location,List<TKeyExtent>> entry : refreshesNeeded.entrySet()) {

        // Ask tablet server to reload the metadata for these tablets. The tablet server returns
        // the list of extents it was hosting but was unable to refresh (the tablets could be in
        // the process of loading). If it is not currently hosting the tablet it does not return
        // anything for it.
        Future<List<TKeyExtent>> future = threadPool
            .submit(() -> sendSyncRefreshRequest(context, logId, entry.getKey(), entry.getValue()));

        futures.put(entry.getKey(), future);
      }

      for (Map.Entry<TabletMetadata.Location,Future<List<TKeyExtent>>> entry : futures.entrySet()) {
        TabletMetadata.Location location = entry.getKey();
        Future<List<TKeyExtent>> future = entry.getValue();

        List<TKeyExtent> nonRefreshedExtents = null;
        try {
          nonRefreshedExtents = future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
        if (nonRefreshedExtents.isEmpty()) {
          // tablet server was able to refresh everything, so remove that location
          refreshesNeeded.remove(location);
        } else {
          // tablet server could not refresh some tablets, try them again later.
          refreshesNeeded.put(location, nonRefreshedExtents);
        }
      }

      // look for any tservers that have died since we read the metadata table and remove them
      if (!refreshesNeeded.isEmpty()) {
        Set<TServerInstance> liveTservers = onlineTserversSupplier.get();

        refreshesNeeded.keySet()
            .removeIf(location -> !liveTservers.contains(location.getServerInstance()));
      }

      if (!refreshesNeeded.isEmpty()) {
        // look for any tablets where the location changed, these tablets will no longer need a
        // refresh because when the tablet loads at the new location it will see the new tablet
        // metadata
        HashMap<KeyExtent,TabletMetadata.Location> prevLocations = new HashMap<>();
        refreshesNeeded.forEach((loc, extents) -> {
          for (TKeyExtent te : extents) {
            var extent = KeyExtent.fromThrift(te);
            prevLocations.put(extent, loc);
          }
        });

        // Build a map of tablets that exist and their current location. No need to includes tablets
        // that no longer exists or do not have a location as later logic is ok w/ these being null.
        HashMap<KeyExtent,TabletMetadata.Location> currLocations = new HashMap<>();
        try (var tablets =
            context.getAmple().readTablets().forTablets(prevLocations.keySet(), Optional.empty())
                .fetch(ColumnType.LOCATION).build()) {
          tablets.forEach(tablet -> {
            if (tablet.getLocation() != null) {
              currLocations.put(tablet.getExtent(), tablet.getLocation());
            }
          });
        }

        refreshesNeeded.clear();

        var finalrefreshesNeeded = refreshesNeeded;
        // rebuild refreshesNeeded only including those where the location is still the same
        prevLocations.forEach((extent, prevLoc) -> {
          var currLoc = currLocations.get(extent);
          // currLoc may be null and this is ok because it should not be equal then
          if (prevLoc.equals(currLoc)) {
            finalrefreshesNeeded.computeIfAbsent(currLoc, k -> new ArrayList<>())
                .add(extent.toThrift());
          } else {
            log.trace("The location of {} changed from {} to {}, so refresh no longer needed",
                extent, prevLoc, currLoc);
          }
        });
      }

      if (!refreshesNeeded.isEmpty()) {
        try {
          retry.waitForNextAttempt(log, logId + " waiting for " + refreshesNeeded.size()
              + " tservers to refresh their tablets metadata");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static List<TKeyExtent> sendSyncRefreshRequest(ServerContext context, String logId,
      TabletMetadata.Location location, List<TKeyExtent> refreshes) {
    TabletServerClientService.Client client = null;
    try {
      log.trace("{} sending refresh request to {} for {} extents", logId, location,
          refreshes.size());
      var timeInMillis = context.getConfiguration().getTimeInMillis(Property.MANAGER_BULK_TIMEOUT);
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, location.getHostAndPort(),
          context, timeInMillis);

      var unrefreshed = client.refreshTablets(TraceUtil.traceInfo(), context.rpcCreds(), refreshes);

      log.trace("{} refresh request to {} returned {} unrefreshed extents", logId, location,
          unrefreshed.size());

      return unrefreshed;
    } catch (TException ex) {
      log.debug("rpc failed server: " + location + ", " + logId + " " + ex.getMessage(), ex);

      // something went wrong w/ RPC return all extents as unrefreshed
      return refreshes;
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }
}
