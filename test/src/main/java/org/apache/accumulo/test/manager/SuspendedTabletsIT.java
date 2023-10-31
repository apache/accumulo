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
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientTabletCache;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.spi.balancer.HostRegexTableLoadBalancer;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HostAndPort;

@Disabled // ELASTICITY_TODO
public class SuspendedTabletsIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(SuspendedTabletsIT.class);
  private static ExecutorService THREAD_POOL;

  public static final int TSERVERS = 3;
  public static final long SUSPEND_DURATION = 80;
  public static final int TABLETS = 30;

  private ProcessReference metadataTserverProcess;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.TABLE_SUSPEND_DURATION, SUSPEND_DURATION + "s");
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "5s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    // Start with 1 tserver, we'll increase that later
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    // config custom balancer to keep all metadata on one server
    @SuppressWarnings("deprecation")
    String p = HostRegexTableLoadBalancer.HOST_BALANCER_OOB_CHECK_KEY;
    cfg.setProperty(p, "1ms");
    cfg.setProperty(Property.MANAGER_TABLET_BALANCER.getKey(),
        HostAndPortRegexTableLoadBalancer.class.getName());
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // Wait for all tablet servers to come online and then choose the first server in the list.
      // Update the balancer configuration to assign all metadata tablets to that server (and
      // everything else to other servers).
      InstanceOperations iops = client.instanceOperations();
      List<String> tservers = iops.getTabletServers();
      while (tservers == null || tservers.size() < 1) {
        Thread.sleep(1000L);
        tservers = client.instanceOperations().getTabletServers();
      }
      HostAndPort metadataServer = HostAndPort.fromString(tservers.get(0));
      log.info("Configuring balancer to assign all metadata tablets to {}", metadataServer);
      @SuppressWarnings("deprecation")
      String p = HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX;
      iops.setProperty(p + MetadataTable.NAME, metadataServer.toString());

      // Wait for the balancer to assign all metadata tablets to the chosen server.
      ClientContext ctx = (ClientContext) client;
      TabletLocations tl = TabletLocations.retrieve(ctx, MetadataTable.NAME);
      while (tl.hosted.keySet().size() != 1 || !tl.hosted.containsKey(metadataServer)) {
        log.info("Metadata tablets are not hosted on the correct server. Waiting for balancer...");
        Thread.sleep(1000L);
        tl = TabletLocations.retrieve(ctx, MetadataTable.NAME);
      }
      log.info("Metadata tablets are now hosted on {}", metadataServer);
    }

    // Since we started only a single tablet server, we know it's the one hosting the
    // metadata table. Save its process reference off so we can exclude it later when
    // killing tablet servers.
    Collection<ProcessReference> procs = getCluster().getProcesses().get(ServerType.TABLET_SERVER);
    assertEquals(1, procs.size(), "Expected a single tserver process");
    metadataTserverProcess = procs.iterator().next();

    // Update the number of tservers and start the new tservers.
    getCluster().getConfig().getClusterServerConfiguration().setNumDefaultTabletServers(TSERVERS);
    getCluster().start();
  }

  @Test
  public void crashAndResumeTserver() throws Exception {
    // Run the test body. When we get to the point where we need a tserver to go away, get rid of it
    // via crashing
    suspensionTestBody((ctx, locs, count) -> {
      // Exclude the tablet server hosting the metadata table from the list and only
      // kill tablet servers that are not hosting the metadata table.
      List<ProcessReference> procs = getCluster().getProcesses().get(ServerType.TABLET_SERVER)
          .stream().filter(p -> !metadataTserverProcess.equals(p)).collect(Collectors.toList());
      Collections.shuffle(procs, RANDOM.get());
      assertEquals(TSERVERS - 1, procs.size(), "Not enough tservers exist");
      assertTrue(procs.size() >= count, "Attempting to kill more tservers (" + count
          + ") than exist in the cluster (" + procs.size() + ")");

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
      Set<TServerInstance> tserverSet = new HashSet<>();
      Set<TServerInstance> metadataServerSet = new HashSet<>();

      ClientTabletCache tl = ClientTabletCache.getInstance(ctx, MetadataTable.ID);
      for (TabletMetadata tm : locs.locationStates.values()) {
        if (tm.hasCurrent()) {
          // add to set of all servers
          tserverSet.add(tm.getLocation().getServerInstance());

          // get server that the current tablets metadata is on
          ClientTabletCache.CachedTablet tab = tl.findTablet(ctx, tm.getExtent().toMetaRow(), false,
              ClientTabletCache.LocationNeed.REQUIRED);
          // add it to the set of servers with metadata
          metadataServerSet.add(new TServerInstance(tab.getTserverLocation().orElseThrow(),
              Long.valueOf(tab.getTserverSession().orElseThrow(), 16)));
        }
      }

      // remove servers with metadata on them from the list of servers to be shutdown
      assertEquals(1, metadataServerSet.size(), "Expecting a single tServer in metadataServerSet");
      tserverSet.removeAll(metadataServerSet);

      assertEquals(TSERVERS - 1, tserverSet.size(),
          "Expecting " + (TSERVERS - 1) + " tServers in shutdown-list");

      List<TServerInstance> tserversList = new ArrayList<>(tserverSet);
      Collections.shuffle(tserversList, RANDOM.get());

      for (int i1 = 0; i1 < count; ++i1) {
        final String tserverName = tserversList.get(i1).getHostPortSession();
        ThriftClientTypes.MANAGER.executeVoid(ctx, client -> {
          log.info("Sending shutdown command to {} via ManagerClientService", tserverName);
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
          Thread.sleep(SECONDS.toMillis(2));
        }
      }
      throw new IllegalStateException("Tablet servers didn't die!");
    });
  }

  /**
   * Main test body for suspension tests.
   *
   * @param serverStopper callback which shuts down some tablet servers.
   */
  private void suspensionTestBody(TServerKiller serverStopper) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      ClientContext ctx = (ClientContext) client;

      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splitPoints = new TreeSet<>();
      for (int i = 1; i < TABLETS; ++i) {
        splitPoints.add(new Text("" + i));
      }
      log.info("Creating table " + tableName);
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splitPoints);
      ctx.tableOperations().create(tableName, ntc);

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
        // Keep checking until all tablets are hosted and spread out across the tablet servers
        Thread.sleep(1000);
        ds = TabletLocations.retrieve(ctx, tableName);
      } while (ds.hostedCount != TABLETS || ds.hosted.keySet().size() != (TSERVERS - 1));

      // Given the loop exit condition above, at this point we're sure that all tablets are hosted
      // and some are hosted on each of the tablet servers other than the one reserved for hosting
      // the metadata table.
      assertEquals(TSERVERS - 1, ds.hosted.keySet().size());

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

  /**
   * A version of {@link HostRegexTableLoadBalancer} that includes the tablet server port in
   * addition to the host name when checking regular expressions. This is useful for testing when
   * multiple tablet servers are running on the same host and one wishes to make pools from the
   * tablet servers on that host.
   */
  @SuppressWarnings("deprecation")
  public static class HostAndPortRegexTableLoadBalancer extends HostRegexTableLoadBalancer {
    private static final Logger LOG =
        LoggerFactory.getLogger(HostAndPortRegexTableLoadBalancer.class.getName());

    @Override
    protected List<String> getPoolNamesForHost(TabletServerId tabletServerId) {
      final String host = tabletServerId.getHost();
      String test = host;
      if (!isIpBasedRegex()) {
        try {
          test = getNameFromIp(host);
        } catch (UnknownHostException e1) {
          LOG.error("Unable to determine host name for IP: " + host + ", setting to default pool",
              e1);
          return Collections.singletonList(DEFAULT_POOL);
        }
      }

      // Add the port on the end
      final String hostString = test + ":" + tabletServerId.getPort();
      List<String> pools = getPoolNameToRegexPattern().entrySet().stream()
          .filter(e -> e.getValue().matcher(hostString).matches()).map(Map.Entry::getKey)
          .collect(Collectors.toList());
      if (pools.isEmpty()) {
        pools.add(DEFAULT_POOL);
      }
      return pools;
    }

    @Override
    public long balance(BalanceParameters params) {
      super.balance(params);
      return 1000L; // Balance once per second during the test
    }
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
          } else {
            // unassigned case
          }
        }
      }
    }
  }
}
