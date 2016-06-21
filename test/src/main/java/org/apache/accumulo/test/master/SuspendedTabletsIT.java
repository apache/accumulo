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
package org.apache.accumulo.test.master;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HostAndPort;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuspendedTabletsIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(SuspendedTabletsIT.class);
  private static ExecutorService threadPool;

  public static final int TSERVERS = 5;
  public static final long SUSPEND_DURATION = MILLISECONDS.convert(2, MINUTES);
  public static final int TABLETS = 100;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.TABLE_SUSPEND_DURATION, SUSPEND_DURATION + "ms");
    cfg.setNumTservers(TSERVERS);
  }

  @Test
  public void suspendAndResumeTserver() throws Exception {
    String tableName = getUniqueNames(1)[0];

    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));
    Instance instance = new ZooKeeperInstance(getCluster().getClientConfig());
    ClientContext ctx = new ClientContext(instance, creds, getCluster().getClientConfig());

    Connector conn = ctx.getConnector();

    // Create a table with a bunch of splits
    log.info("Creating table " + tableName);
    conn.tableOperations().create(tableName);
    SortedSet<Text> splitPoints = new TreeSet<>();
    for (int i = 1; i < TABLETS; ++i) {
      splitPoints.add(new Text("" + i));
    }
    conn.tableOperations().addSplits(tableName, splitPoints);

    // Wait for all of the tablets to hosted ...
    log.info("Waiting on hosting and balance");
    TabletLocations ds;
    for (ds = TabletLocations.retrieve(ctx, tableName); ds.hostedCount != TABLETS; ds = TabletLocations.retrieve(ctx, tableName)) {
      Thread.sleep(1000);
    }

    // ... and balanced.
    conn.instanceOperations().waitForBalance();
    do {
      // Give at least another 5 seconds for migrations to finish up
      Thread.sleep(5000);
      ds = TabletLocations.retrieve(ctx, tableName);
    } while (ds.hostedCount != TABLETS);

    // Pray all of our tservers have at least 1 tablet.
    Assert.assertEquals(TSERVERS, ds.hosted.keySet().size());

    // Kill two tablet servers hosting our tablets. This should put tablets into suspended state, and thus halt balancing.
    log.info("Killing tservers");

    TabletLocations beforeDeathState = ds;
    {
      Iterator<ProcessReference> prIt = getCluster().getProcesses().get(ServerType.TABLET_SERVER).iterator();
      ProcessReference first = prIt.next();
      ProcessReference second = prIt.next();
      getCluster().killProcess(ServerType.TABLET_SERVER, first);
      getCluster().killProcess(ServerType.TABLET_SERVER, second);
    }

    // Eventually some tablets will be suspended.
    log.info("Waiting on suspended tablets");
    ds = TabletLocations.retrieve(ctx, tableName);
    // Until we can scan the metadata table, the master probably can't either, so won't have been able to suspend the tablets.
    // So we note the time that we were first able to successfully scan the metadata table.
    long killTime = System.nanoTime();
    while (ds.suspended.keySet().size() != 2) {
      Thread.sleep(1000);
      ds = TabletLocations.retrieve(ctx, tableName);
    }

    SetMultimap<HostAndPort,KeyExtent> deadTabletsByServer = ds.suspended;

    // By this point, all tablets should be either hosted or suspended. All suspended tablets should
    // "belong" to the dead tablet servers, and should be in exactly the same place as before any tserver death.
    for (HostAndPort server : deadTabletsByServer.keySet()) {
      Assert.assertEquals(deadTabletsByServer.get(server), beforeDeathState.hosted.get(server));
    }
    Assert.assertEquals(TABLETS, ds.hostedCount + ds.suspendedCount);

    // Restart the first tablet server, making sure it ends up on the same port
    HostAndPort restartedServer = deadTabletsByServer.keySet().iterator().next();
    log.info("Restarting " + restartedServer);
    getCluster().getClusterControl().start(ServerType.TABLET_SERVER, null,
        ImmutableMap.of(Property.TSERV_CLIENTPORT.getKey(), "" + restartedServer.getPort(), Property.TSERV_PORTSEARCH.getKey(), "false"), 1);

    // Eventually, the suspended tablets should be reassigned to the newly alive tserver.
    log.info("Awaiting tablet unsuspension for tablets belonging to " + restartedServer);
    for (ds = TabletLocations.retrieve(ctx, tableName); ds.suspended.containsKey(restartedServer) || ds.assignedCount != 0; ds = TabletLocations.retrieve(ctx,
        tableName)) {
      Thread.sleep(1000);
    }
    Assert.assertEquals(deadTabletsByServer.get(restartedServer), ds.hosted.get(restartedServer));

    // Finally, after much longer, remaining suspended tablets should be reassigned.
    log.info("Awaiting tablet reassignment for remaining tablets");
    for (ds = TabletLocations.retrieve(ctx, tableName); ds.hostedCount != TABLETS; ds = TabletLocations.retrieve(ctx, tableName)) {
      Thread.sleep(1000);
    }

    long recoverTime = System.nanoTime();
    Assert.assertTrue(recoverTime - killTime >= NANOSECONDS.convert(SUSPEND_DURATION, MILLISECONDS));
    Assert.assertTrue(recoverTime - killTime <= NANOSECONDS.convert(SUSPEND_DURATION, MILLISECONDS) + NANOSECONDS.convert(3, MINUTES));
  }

  private static final AtomicInteger threadCounter = new AtomicInteger(0);

  @BeforeClass
  public static void init() {
    threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "Scanning deadline thread #" + threadCounter.incrementAndGet());
      }
    });
  }

  @AfterClass
  public static void cleanup() {
    threadPool.shutdownNow();
  }

  private static class TabletLocations {
    public final Map<KeyExtent,TabletLocationState> locationStates = new HashMap<>();
    public final SetMultimap<HostAndPort,KeyExtent> hosted = HashMultimap.create();
    public final SetMultimap<HostAndPort,KeyExtent> suspended = HashMultimap.create();
    public int hostedCount = 0;
    public int assignedCount = 0;
    public int suspendedCount = 0;
    public int unassignedCount = 0;

    private TabletLocations() {}

    public static TabletLocations retrieve(final ClientContext ctx, final String tableName) throws Exception {
      int sleepTime = 200;
      int remainingAttempts = 30;

      while (true) {
        try {
          FutureTask<TabletLocations> tlsFuture = new FutureTask<>(new Callable<TabletLocations>() {
            @Override
            public TabletLocations call() throws Exception {
              TabletLocations answer = new TabletLocations();
              answer.scan(ctx, tableName);
              return answer;
            }
          });
          threadPool.submit(tlsFuture);
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
          Assert.fail("Scanning of metadata failed, aborting");
        }
      }
    }

    private void scan(ClientContext ctx, String tableName) throws Exception {
      Map<String,String> idMap = ctx.getConnector().tableOperations().tableIdMap();
      String tableId = Objects.requireNonNull(idMap.get(tableName));
      try (MetaDataTableScanner scanner = new MetaDataTableScanner(ctx, new Range())) {
        while (scanner.hasNext()) {
          TabletLocationState tls = scanner.next();

          if (!tls.extent.getTableId().equals(tableId)) {
            continue;
          }
          locationStates.put(tls.extent, tls);
          if (tls.suspend != null) {
            suspended.put(tls.suspend.server, tls.extent);
            ++suspendedCount;
          } else if (tls.current != null) {
            hosted.put(tls.current.getLocation(), tls.extent);
            ++hostedCount;
          } else if (tls.future != null) {
            ++assignedCount;
          } else {
            unassignedCount += 1;
          }
        }
      }
    }
  }
}
