/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.SortedSet;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnloadTabletHandler implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(UnloadTabletHandler.class);
  private final KeyExtent extent;
  private final TUnloadTabletGoal goalState;
  private final long requestTimeSkew;
  private final SortedSet<KeyExtent> unopenedTablets;
  private final SortedSet<KeyExtent> openingTablets;
  private final OnlineTablets onlineTablets;
  private final TabletServer server;

  public UnloadTabletHandler(TabletServer server, KeyExtent extent, TUnloadTabletGoal goalState,
      long requestTime) {
    this.extent = extent;
    this.goalState = goalState;
    this.server = server;
    this.openingTablets = server.getOpeningTablets();
    this.unopenedTablets = server.getUnopenedTablets();
    this.onlineTablets = server.getOnlineTabletsRaw();
    this.requestTimeSkew = requestTime - MILLISECONDS.convert(System.nanoTime(), NANOSECONDS);
  }

  @Override
  public void run() {

    Tablet t = null;

    synchronized (unopenedTablets) {
      if (unopenedTablets.contains(extent)) {
        unopenedTablets.remove(extent);
        // enqueueMasterMessage(new TabletUnloadedMessage(extent));
        return;
      }
    }
    synchronized (openingTablets) {
      while (openingTablets.contains(extent)) {
        try {
          openingTablets.wait();
        } catch (InterruptedException e) {}
      }
    }
    synchronized (onlineTablets) {
      if (onlineTablets.snapshot().containsKey(extent)) {
        t = onlineTablets.snapshot().get(extent);
      }
    }

    if (t == null) {
      // Tablet has probably been recently unloaded: repeated master
      // unload request is crossing the successful unloaded message
      if (!server.getRecentlyUnloadedCache().containsKey(extent)) {
        log.info("told to unload tablet that was not being served {}", extent);
        server.enqueueMasterMessage(
            new TabletStatusMessage(TabletLoadState.UNLOAD_FAILURE_NOT_SERVING, extent));
      }
      return;
    }

    try {
      t.close(!goalState.equals(TUnloadTabletGoal.DELETED));
    } catch (Throwable e) {

      if ((t.isClosing() || t.isClosed()) && e instanceof IllegalStateException) {
        log.debug("Failed to unload tablet {}... it was already closing or closed : {}", extent,
            e.getMessage());
      } else {
        log.error("Failed to close tablet {}... Aborting migration", extent, e);
        server.enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.UNLOAD_ERROR, extent));
      }
      return;
    }

    // stop serving tablet - client will get not serving tablet
    // exceptions
    server.getRecentlyUnloadedCache().put(extent, System.currentTimeMillis());
    onlineTablets.remove(extent);

    try {
      TServerInstance instance =
          new TServerInstance(server.getClientAddress(), server.getLock().getSessionId());
      TabletLocationState tls = null;
      try {
        tls = new TabletLocationState(extent, null, instance, null, null, null, false);
      } catch (BadLocationStateException e) {
        log.error("Unexpected error", e);
      }
      if (!goalState.equals(TUnloadTabletGoal.SUSPENDED) || extent.isRootTablet()
          || (extent.isMeta()
              && !server.getConfiguration().getBoolean(Property.MASTER_METADATA_SUSPENDABLE))) {
        TabletStateStore.unassign(server.getContext(), tls, null);
      } else {
        TabletStateStore.suspend(server.getContext(), tls, null,
            requestTimeSkew + MILLISECONDS.convert(System.nanoTime(), NANOSECONDS));
      }
    } catch (DistributedStoreException ex) {
      log.warn("Unable to update storage", ex);
    } catch (KeeperException e) {
      log.warn("Unable determine our zookeeper session information", e);
    } catch (InterruptedException e) {
      log.warn("Interrupted while getting our zookeeper session information", e);
    }

    // tell the master how it went
    server.enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.UNLOADED, extent));

    // roll tablet stats over into tablet server's statsKeeper object as
    // historical data
    server.getStatsKeeper().saveMajorMinorTimes(t.getTabletStats());
  }
}
