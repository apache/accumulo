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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Repo asks hosted tablets that were bulk loaded into to refresh their metadata. It works by
 * getting a metadata snapshot once that includes tablets and their locations. Then it repeatedly
 * ask the tablets at those locations to refresh their metadata. It the tablets are no longer the
 * location its ok. That means the tablet either unloaded before of after the snapshot. In either
 * case the tablet will see the bulk files the next time its hosted somewhere.
 */
public class RefreshTablets extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(RefreshTablets.class);

  private static final long serialVersionUID = 1L;

  private final BulkInfo bulkInfo;

  public RefreshTablets(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {
    return 0;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    Map<Location,List<KeyExtent>> refreshesNeeded;

    // ELASTICITY_TODO limit tablets scanned to range of bulk import extents (after #3336 is merged)
    try (var tablets =
        manager.getContext().getAmple().readTablets().forTable(bulkInfo.tableId).checkConsistency()
            .fetch(ColumnType.LOADED, ColumnType.LOCATION, ColumnType.PREV_ROW).build()) {

      // Find all tablets that have a location and load markers for this bulk load operation and
      // therefore need to refresh their metadata. There may be some tablets in the map that were
      // hosted after the bulk files were set and that is ok, it just results in an unneeded refresh
      // request. There may also be tablets that had a location when the files were set but do not
      // have a location now, that is ok the next time that tablet loads somewhere it will see the
      // files.
      refreshesNeeded = tablets.stream()
          .filter(tabletMetadata -> tabletMetadata.getLocation() != null
              && tabletMetadata.getLoaded().containsValue(tid))
          .collect(groupingBy(TabletMetadata::getLocation,
              mapping(TabletMetadata::getExtent, toList())));
    }

    if (!refreshesNeeded.isEmpty()) {
      // block until all tablets are refreshed
      refreshTablets(tid, manager, refreshesNeeded);
    }
    return new CleanUpBulkImport(bulkInfo);
  }

  private void refreshTablets(long tid, Manager manager,
      Map<Location,List<KeyExtent>> refreshesNeeded)
      throws InterruptedException, ExecutionException {

    // ELASTICITY_TODO should this thread pool be configurable?
    ThreadPoolExecutor threadPool = manager.getContext().threadPools().createFixedThreadPool(5,
        "Bulk load refresh" + FateTxId.formatTid(tid), false);

    try {
      Retry retry = Retry.builder().infiniteRetries().retryAfter(100, MILLISECONDS)
          .incrementBy(100, MILLISECONDS).maxWait(1, SECONDS).backOffFactor(1.5)
          .logInterval(3, MINUTES).createRetry();

      while (!refreshesNeeded.isEmpty()) {

        Map<Location,Future<List<KeyExtent>>> futures = new HashMap<>();

        for (Map.Entry<Location,List<KeyExtent>> entry : refreshesNeeded.entrySet()) {

          // Ask tablet server to reload the metadata for these tablets. The tablet server returns
          // the list of extents it was hosting but was unable to refresh (the tablets could be in
          // the process of loading). If it is not currently hosting the tablet it treats that as
          // refreshed and does not return anything for it.
          Future<List<KeyExtent>> future = threadPool
              .submit(() -> sendSyncRefreshRequest(manager, tid, entry.getKey(), entry.getValue()));

          futures.put(entry.getKey(), future);
        }

        for (Map.Entry<Location,Future<List<KeyExtent>>> entry : futures.entrySet()) {
          Location location = entry.getKey();
          Future<List<KeyExtent>> future = entry.getValue();

          List<KeyExtent> nonRefreshedExtents = future.get();
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
          LiveTServerSet lts =
              new LiveTServerSet(manager.getContext(), (current, deleted, added) -> {});
          Set<TServerInstance> liveTservers = lts.getCurrentServers();

          refreshesNeeded.keySet()
              .removeIf(location -> !liveTservers.contains(location.getServerInstance()));
        }

        if (!refreshesNeeded.isEmpty()) {
          retry.waitForNextAttempt(log, FateTxId.formatTid(tid) + " waiting for "
              + refreshesNeeded.size() + " tservers to refresh their tablets metadata");
        }
      }
    } finally {
      threadPool.shutdownNow();
    }
  }

  private List<KeyExtent> sendSyncRefreshRequest(Manager manager, long tid, Location location,
      List<KeyExtent> extents) {
    TabletServerClientService.Client client = null;
    try {
      log.trace("{} sending refresh request to {} for {} extents", FateTxId.formatTid(tid),
          location, extents.size());
      var timeInMillis = manager.getConfiguration().getTimeInMillis(Property.MANAGER_BULK_TIMEOUT);
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, location.getHostAndPort(),
          manager.getContext(), timeInMillis);

      var unrefreshed =
          client.refreshTablets(TraceUtil.traceInfo(), manager.getContext().rpcCreds(),
              extents.stream().map(KeyExtent::toThrift).collect(toList()));

      log.trace("{} refresh request to {} returned {} unrefreshed extents", FateTxId.formatTid(tid),
          location, unrefreshed.size());

      var unrefreshedConverted = unrefreshed.stream().map(KeyExtent::fromThrift).collect(toList());

      if (log.isDebugEnabled() && !unrefreshedConverted.isEmpty()) {
        // this situation should be fairly uncommon, but could hold up bulk import. so lets log some
        // info about it in case it gets stuck on a particular tablet
        var fateStr = FateTxId.formatTid(tid);
        for (var extent : unrefreshedConverted) {
          log.debug("{} tserver {} was unable to refresh tablet {}", fateStr, location, extent);
        }
      }

      return unrefreshedConverted;
    } catch (TException ex) {
      var fmtTid = FateTxId.formatTid(tid);
      log.debug("rpc failed server: " + location + ", " + fmtTid + " " + ex.getMessage(), ex);

      // ELASTICITY_TODO are there any other exceptions we should catch in this method and check if
      // the tserver is till alive?

      // something went wrong w/ RPC return all extents as unrefreshed
      return extents;
    } finally {
      ThriftUtil.returnClient(client, manager.getContext());
    }
  }
}
