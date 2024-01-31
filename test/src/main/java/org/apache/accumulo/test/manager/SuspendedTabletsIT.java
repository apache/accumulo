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
package org.apache.accumulo.test.manager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessNotFoundException;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.test.functional.TabletResourceGroupBalanceIT;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HostAndPort;

public class SuspendedTabletsIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(SuspendedTabletsIT.class);
  private static ExecutorService THREAD_POOL;
  private static final String TEST_GROUP_NAME = "SUSPEND_TEST";

  public static final int TSERVERS = 3;
  public static final long SUSPEND_DURATION = 80;
  public static final int TABLETS = 30;

  private String defaultGroup;
  private Set<String> testGroup = new HashSet<>();
  private List<ProcessReference> tabletServerProcesses;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.MANAGER_STARTUP_TSERVER_AVAIL_MIN_COUNT, "2");
    cfg.setProperty(Property.MANAGER_STARTUP_TSERVER_AVAIL_MAX_WAIT, "10s");
    cfg.setProperty(Property.TSERV_MIGRATE_MAXCONCURRENT, "50");
    cfg.setProperty(Property.TABLE_SUSPEND_DURATION, SUSPEND_DURATION + "s");
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "5s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
  }

  @BeforeEach
  public void setUp() throws Exception {

    MiniAccumuloClusterImpl mac = (MiniAccumuloClusterImpl) getCluster();
    ProcessReference defaultTabletServer =
        mac.getProcesses().get(ServerType.TABLET_SERVER).iterator().next();
    assertNotNull(defaultTabletServer);

    mac.getConfig().getClusterServerConfiguration().addTabletServerResourceGroup(TEST_GROUP_NAME,
        2);
    getCluster().start();

    tabletServerProcesses = mac.getProcesses().get(ServerType.TABLET_SERVER).stream()
        .filter(p -> !p.equals(defaultTabletServer)).collect(Collectors.toList());

    Map<String,String> hostAndGroup = TabletResourceGroupBalanceIT.getTServerGroups(mac);
    hostAndGroup.forEach((k, v) -> {
      if (v.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME)) {
        defaultGroup = k;
      } else {
        testGroup.add(k);
      }
    });

    assertNotNull(defaultGroup);
    assertEquals(2, testGroup.size());

    log.info("TabletServers in default group: {}", defaultGroup);
    log.info("TabletServers in {} group: {}", TEST_GROUP_NAME, testGroup);
  }

  @Test
  public void crashAndResumeTserver() throws Exception {
    // Run the test body. When we get to the point where we need a tserver to go away, get rid of it
    // via crashing
    suspensionTestBody((ctx, locs, count) -> {
      tabletServerProcesses.forEach(proc -> {
        try {
          log.info("Killing processes: {}", proc);
          ((MiniAccumuloClusterImpl) getCluster()).getClusterControl()
              .killProcess(ServerType.TABLET_SERVER, proc);
        } catch (ProcessNotFoundException | InterruptedException e) {
          throw new RuntimeException("Error killing process: " + proc, e);
        }
      });
    });
  }

  @Test
  public void shutdownAndResumeTserver() throws Exception {
    // Run the test body. When we get to the point where we need tservers to go away, stop them via
    // a clean shutdown.
    suspensionTestBody((ctx, locs, count) -> {

      testGroup.forEach(ts -> {
        try {
          ThriftClientTypes.MANAGER.executeVoid(ctx, client -> {
            log.info("Sending shutdown command to {} via ManagerClientService", ts);
            client.shutdownTabletServer(null, ctx.rpcCreds(), ts, false);
          });
        } catch (AccumuloSecurityException | AccumuloException e) {
          throw new RuntimeException("Error calling shutdownTabletServer for " + ts, e);
        }
      });

      try (AccumuloClient client =
          Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
        Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1);
      }

    });
  }

  /**
   * Main test body for suspension tests.
   *
   * @param serverStopper callback which shuts down some tablet servers.
   */
  private void suspensionTestBody(TServerKiller serverStopper) throws Exception {
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      ClientContext ctx = (ClientContext) client;

      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splitPoints = new TreeSet<>();
      for (int i = 1; i < TABLETS; ++i) {
        splitPoints.add(new Text("" + i));
      }
      log.info("Creating table " + tableName);
      Map<String,String> properties = new HashMap<>();
      properties.put("table.custom.assignment.group", TEST_GROUP_NAME);

      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splitPoints)
          .withInitialTabletAvailability(TabletAvailability.HOSTED);
      ntc.setProperties(properties);
      ctx.tableOperations().create(tableName, ntc);

      // Wait for all of the tablets to hosted ...
      log.info("Waiting on hosting and balance");
      TabletLocations ds;
      for (ds = TabletLocations.retrieve(ctx, tableName); ds.hostedCount != TABLETS;
          ds = TabletLocations.retrieve(ctx, tableName)) {
        Thread.sleep(1000);
      }
      log.info("Tablets hosted");

      // ... and balanced.
      ctx.instanceOperations().waitForBalance();
      log.info("Tablets balanced.");
      do {
        // Keep checking until all tablets are hosted and spread out across the tablet servers
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      } while (ds.hostedCount != TABLETS || ds.hosted.keySet().size() != (TSERVERS - 1));

      // Given the loop exit condition above, at this point we're sure that all tablets are hosted
      // and some are hosted on each of the tablet servers other than the one reserved for hosting
      // the metadata table.
      assertEquals(TSERVERS - 1, ds.hosted.keySet().size());
      log.info("Tablet balance verified.");

      // Kill two tablet servers hosting our tablets. This should put tablets into suspended state,
      // and thus halt balancing.
      TabletLocations beforeDeathState = ds;
      log.info("Eliminating tablet servers");
      serverStopper.eliminateTabletServers(ctx, beforeDeathState, TSERVERS - 1);

      // All tablets should be either hosted or suspended.
      log.info("Waiting on suspended tablets");
      do {
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      } while (ds.suspended.keySet().size() != (TSERVERS - 1)
          || (ds.suspendedCount + ds.hostedCount) != TABLETS);

      SetMultimap<HostAndPort,KeyExtent> deadTabletsByServer = ds.suspended;

      // All suspended tablets should "belong" to the dead tablet servers, and should be in exactly
      // the same place as before any tserver death.
      for (HostAndPort server : deadTabletsByServer.keySet()) {
        // Comparing pre-death, hosted tablets to suspended tablets on a server
        assertEquals(beforeDeathState.hosted.get(server), deadTabletsByServer.get(server));
      }
      assertEquals(TABLETS, ds.hostedCount + ds.suspendedCount);
      // Restart the first tablet server, making sure it ends up on the same port
      HostAndPort restartedServer = deadTabletsByServer.keySet().iterator().next();
      log.info("Restarting " + restartedServer);
      ((MiniAccumuloClusterImpl) getCluster())._exec(TabletServer.class, ServerType.TABLET_SERVER,
          Map.of(Property.TSERV_CLIENTPORT.getKey(), "" + restartedServer.getPort(),
              Property.TSERV_PORTSEARCH.getKey(), "false"),
          "-o", Property.TSERV_GROUP_NAME.getKey() + "=" + TEST_GROUP_NAME);

      // Eventually, the suspended tablets should be reassigned to the newly alive tserver.
      log.info("Awaiting tablet unsuspension for tablets belonging to " + restartedServer);
      while (ds.suspended.containsKey(restartedServer) || ds.assignedCount != 0) {
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      }
      assertEquals(deadTabletsByServer.get(restartedServer), ds.hosted.get(restartedServer));

      // Finally, after much longer, remaining suspended tablets should be reassigned.
      log.info("Awaiting tablet reassignment for remaining tablets (suspension timeout)");
      while (ds.hostedCount != TABLETS) {
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      }
    }
  }

  private interface TServerKiller {
    void eliminateTabletServers(ClientContext ctx, TabletLocations locs, int count)
        throws Exception;
  }

  private static final AtomicInteger threadCounter = new AtomicInteger(0);

  @BeforeAll
  public static void init() {
    THREAD_POOL = Executors.newCachedThreadPool(
        r -> new Thread(r, "Scanning deadline thread #" + threadCounter.incrementAndGet()));
  }

  @AfterAll
  public static void cleanup() {
    THREAD_POOL.shutdownNow();
  }

  private static class TabletLocations {
    public final Map<KeyExtent,TabletMetadata> locationStates = new HashMap<>();
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
          THREAD_POOL.execute(tlsFuture);
          return tlsFuture.get(5, SECONDS);
        } catch (TimeoutException ex) {
          log.debug("Retrieval timed out.");
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
      var level = Ample.DataLevel.of(TableId.of(tableId));
      try (var tablets = ctx.getAmple().readTablets().forLevel(level).build()) {
        var scanner = tablets.iterator();
        while (scanner.hasNext()) {
          final TabletMetadata tm = scanner.next();
          final KeyExtent ke = tm.getExtent();

          if (!tm.getTableId().canonical().equals(tableId)) {
            continue;
          }
          locationStates.put(ke, tm);
          if (tm.getSuspend() != null) {
            suspended.put(tm.getSuspend().server, ke);
            ++suspendedCount;
          } else if (tm.hasCurrent()) {
            hosted.put(tm.getLocation().getHostAndPort(), ke);
            ++hostedCount;
          } else if (tm.getLocation() != null
              && tm.getLocation().getType().equals(LocationType.FUTURE)) {
            ++assignedCount;
          }
        }
      }
    }
  }
}
