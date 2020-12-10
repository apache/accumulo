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
package org.apache.accumulo.test.master;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.MasterClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

public class SuspendedTabletsIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(SuspendedTabletsIT.class);
  private static final Random RANDOM = new SecureRandom();
  private static ExecutorService THREAD_POOL;

  public static final int TSERVERS = 3;
  public static final long SUSPEND_DURATION = 20;
  public static final int TABLETS = 30;

  @Override
  protected int defaultTimeoutSeconds() {
    return 5 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.TABLE_SUSPEND_DURATION, SUSPEND_DURATION + "s");
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "5s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.setNumTservers(TSERVERS);
  }

  @Test
  public void crashAndResumeTserver() throws Exception {
    // Run the test body. When we get to the point where we need a tserver to go away, get rid of it
    // via crashing
    suspensionTestBody((ctx, locs, count) -> {
      List<ProcessReference> procs =
          new ArrayList<>(getCluster().getProcesses().get(ServerType.TABLET_SERVER));
      Collections.shuffle(procs);

      for (int i = 0; i < count; ++i) {
        ProcessReference pr = procs.get(i);
        log.info("Crashing {}", pr.getProcess());
        getCluster().killProcess(ServerType.TABLET_SERVER, pr);
      }
    });
  }

  @Test
  public void shutdownAndResumeTserver() throws Exception {
    // Run the test body. When we get to the point where we need tservers to go away, stop them via
    // a clean shutdown.
    suspensionTestBody((ctx, locs, count) -> {
      Set<TServerInstance> tserversSet = new HashSet<>();
      for (TabletLocationState tls : locs.locationStates.values()) {
        if (tls.current != null) {
          tserversSet.add(tls.current);
        }
      }
      List<TServerInstance> tserversList = new ArrayList<>(tserversSet);
      Collections.shuffle(tserversList, RANDOM);

      for (int i1 = 0; i1 < count; ++i1) {
        final String tserverName = tserversList.get(i1).getHostPortSession();
        MasterClient.executeVoid(ctx, client -> {
          log.info("Sending shutdown command to {} via MasterClientService", tserverName);
          client.shutdownTabletServer(null, ctx.rpcCreds(), tserverName, false);
        });
      }

      log.info("Waiting for tserver process{} to die", count == 1 ? "" : "es");
      for (int i2 = 0; i2 < 10; ++i2) {
        List<ProcessReference> deadProcs = new ArrayList<>();
        for (ProcessReference pr1 : getCluster().getProcesses().get(ServerType.TABLET_SERVER)) {
          Process p = pr1.getProcess();
          if (!p.isAlive()) {
            deadProcs.add(pr1);
          }
        }
        for (ProcessReference pr2 : deadProcs) {
          log.info("Process {} is dead, informing cluster control about this", pr2.getProcess());
          getCluster().getClusterControl().killProcess(ServerType.TABLET_SERVER, pr2);
          --count;
        }
        if (count == 0) {
          return;
        } else {
          Thread.sleep(MILLISECONDS.convert(2, SECONDS));
        }
      }
      throw new IllegalStateException("Tablet servers didn't die!");
    });
  }

  /**
   * Main test body for suspension tests.
   *
   * @param serverStopper
   *          callback which shuts down some tablet servers.
   */
  private void suspensionTestBody(TServerKiller serverStopper) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      ClientContext ctx = (ClientContext) client;

      String tableName = getUniqueNames(1)[0];

      // Create a table with a bunch of splits
      log.info("Creating table " + tableName);
      ctx.tableOperations().create(tableName);
      SortedSet<Text> splitPoints = new TreeSet<>();
      for (int i = 1; i < TABLETS; ++i) {
        splitPoints.add(new Text("" + i));
      }
      ctx.tableOperations().addSplits(tableName, splitPoints);

      // Wait for all of the tablets to hosted ...
      log.info("Waiting on hosting and balance");
      TabletLocations ds;
      for (ds = TabletLocations.retrieve(ctx, tableName); ds.hostedCount != TABLETS;
          ds = TabletLocations.retrieve(ctx, tableName)) {
        Thread.sleep(1000);
      }

      // ... and balanced.
      ctx.instanceOperations().waitForBalance();
      do {
        // Give at least another 5 seconds for migrations to finish up
        Thread.sleep(5000);
        ds = TabletLocations.retrieve(ctx, tableName);
      } while (ds.hostedCount != TABLETS);

      // Pray all of our tservers have at least 1 tablet.
      assertEquals(TSERVERS, ds.hosted.keySet().size());

      // Kill two tablet servers hosting our tablets. This should put tablets into suspended state,
      // and thus halt balancing.

      TabletLocations beforeDeathState = ds;
      log.info("Eliminating tablet servers");
      serverStopper.eliminateTabletServers(ctx, beforeDeathState, 2);

      // Eventually some tablets will be suspended.
      log.info("Waiting on suspended tablets");
      ds = TabletLocations.retrieve(ctx, tableName);
      // Until we can scan the metadata table, the master probably can't either, so won't have been
      // able to suspend the tablets.
      // So we note the time that we were first able to successfully scan the metadata table.
      long killTime = System.nanoTime();
      while (ds.suspended.keySet().size() != 2) {
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      }

      SetMultimap<HostAndPort,KeyExtent> deadTabletsByServer = ds.suspended;

      // By this point, all tablets should be either hosted or suspended. All suspended tablets
      // should
      // "belong" to the dead tablet servers, and should be in exactly the same place as before any
      // tserver death.
      for (HostAndPort server : deadTabletsByServer.keySet()) {
        assertEquals(deadTabletsByServer.get(server), beforeDeathState.hosted.get(server));
      }
      assertEquals(TABLETS, ds.hostedCount + ds.suspendedCount);

      // Restart the first tablet server, making sure it ends up on the same port
      HostAndPort restartedServer = deadTabletsByServer.keySet().iterator().next();
      log.info("Restarting " + restartedServer);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER,
          Map.of(Property.TSERV_CLIENTPORT.getKey(), "" + restartedServer.getPort(),
              Property.TSERV_PORTSEARCH.getKey(), "false"),
          1);

      // Eventually, the suspended tablets should be reassigned to the newly alive tserver.
      log.info("Awaiting tablet unsuspension for tablets belonging to " + restartedServer);
      while (ds.suspended.containsKey(restartedServer) || ds.assignedCount != 0) {
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      }
      assertEquals(deadTabletsByServer.get(restartedServer), ds.hosted.get(restartedServer));

      // Finally, after much longer, remaining suspended tablets should be reassigned.
      log.info("Awaiting tablet reassignment for remaining tablets");
      while (ds.hostedCount != TABLETS) {
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      }

      long recoverTime = System.nanoTime();
      assertTrue(recoverTime - killTime >= NANOSECONDS.convert(SUSPEND_DURATION, SECONDS));
    }
  }

  private interface TServerKiller {
    void eliminateTabletServers(ClientContext ctx, TabletLocations locs, int count)
        throws Exception;
  }

  private static final AtomicInteger threadCounter = new AtomicInteger(0);

  @BeforeClass
  public static void init() {
    THREAD_POOL = Executors.newCachedThreadPool(
        r -> new Thread(r, "Scanning deadline thread #" + threadCounter.incrementAndGet()));
  }

  @AfterClass
  public static void cleanup() {
    THREAD_POOL.shutdownNow();
  }

  private static class TabletLocations {
    public final Map<KeyExtent,TabletLocationState> locationStates = new HashMap<>();
    public final SetMultimap<HostAndPort,KeyExtent> hosted = HashMultimap.create();
    public final SetMultimap<HostAndPort,KeyExtent> suspended = HashMultimap.create();
    public int hostedCount = 0;
    public int assignedCount = 0;
    public int suspendedCount = 0;

    public static TabletLocations retrieve(final ClientContext ctx, final String tableName)
        throws Exception {
      int sleepTime = 200;
      int remainingAttempts = 30;

      while (true) {
        try {
          FutureTask<TabletLocations> tlsFuture = new FutureTask<>(() -> {
            TabletLocations answer = new TabletLocations();
            answer.scan(ctx, tableName);
            return answer;
          });
          THREAD_POOL.submit(tlsFuture);
          return tlsFuture.get(5, SECONDS);
        } catch (TimeoutException ex) {
          log.debug("Retrieval timed out", ex);
        } catch (Exception ex) {
          log.warn("Failed to scan metadata", ex);
        }
        sleepTime = Math.min(2 * sleepTime, 10000);
        Thread.sleep(sleepTime);
        --remainingAttempts;
        if (remainingAttempts == 0) {
          fail("Scanning of metadata failed, aborting");
        }
      }
    }

    private void scan(ClientContext ctx, String tableName) {
      Map<String,String> idMap = ctx.tableOperations().tableIdMap();
      String tableId = Objects.requireNonNull(idMap.get(tableName));
      try (var scanner = new MetaDataTableScanner(ctx, new Range(), MetadataTable.NAME)) {
        while (scanner.hasNext()) {
          TabletLocationState tls = scanner.next();

          if (!tls.extent.tableId().canonical().equals(tableId)) {
            continue;
          }
          locationStates.put(tls.extent, tls);
          if (tls.suspend != null) {
            suspended.put(tls.suspend.server, tls.extent);
            ++suspendedCount;
          } else if (tls.current != null) {
            hosted.put(tls.current.getHostAndPort(), tls.extent);
            ++hostedCount;
          } else if (tls.future != null) {
            ++assignedCount;
          } else {
            // unassigned case
          }
        }
      }
    }
  }
}
