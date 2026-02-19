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
package org.apache.accumulo.test;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FastFate;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Sets;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

/**
 * {@link ComprehensiveMultiManagerIT} runs multiple managers with lots of Accumulo APIs, however
 * that does not actually verify that fate operations actually run on multiple managers. This test
 * runs a smaller set of Accumulo API operations and does the following.
 *
 * <ul>
 * <li>Starts new manager processes and verifies fate operations start running on them</li>
 * <li>Kills assistant/non-primary manager processes and verifies the system recovers</li>
 * <li>Kills primary manager process and verifies the system recovers</li>
 * <li>Verifies that Accumulo API calls are not impacted by managers starting/stopping</li>
 * </ul>
 *
 */
public class MultipleManagerIT extends ConfigurableMacBase {

  // A manager that will quickly clean up fate reservations held by dead managers
  public static class FastFateCleanupManager extends Manager {
    protected FastFateCleanupManager(ServerOpts opts, String[] args) throws IOException {
      super(opts, ServerContext::new, args);
    }

    @Override
    protected Fate<FateEnv> createFateInstance(FateEnv env, FateStore<FateEnv> store,
        ServerContext context) {
      LoggerFactory.getLogger(FastFateCleanupManager.class)
          .info("Creating Fast fate cleanup manager for {}", store.type());
      return new FastFate<>(env, store, true, TraceRepo::toLogString, getConfiguration());
    }

    public static void main(String[] args) throws Exception {
      try (FastFateCleanupManager manager = new FastFateCleanupManager(new ServerOpts(), args)) {
        manager.runServer();
      }
    }
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // TODO add a way to start multiple managers to mini
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(8);
    // Set this lower so that locks timeout faster
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.setServerClass(ServerType.MANAGER, r -> FastFateCleanupManager.class);
    super.configure(cfg, hadoopCoreSite);
  }

  @Test
  public void testFate() throws Exception {

    List<Process> managerWorkers = new ArrayList<>();
    var executor = Executors.newCachedThreadPool();

    // Start a lot of background threads that should cause fate operations to run.
    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {
      // Create a table in order to wait for the single manager to become the primary manager
      client.tableOperations().create("waitTable");

      // start more manager processes, should be assigned fate work
      managerWorkers.add(exec(FastFateCleanupManager.class));
      managerWorkers.add(exec(FastFateCleanupManager.class));

      AtomicBoolean stop = new AtomicBoolean(false);

      var splits = IntStream.range(1, 10).mapToObj(i -> String.format("%03d", i)).map(Text::new)
          .collect(Collectors.toCollection(TreeSet::new));
      var tableOpFutures = new ArrayList<Future<Integer>>();
      for (int i = 0; i < 10; i++) {
        var table = "t" + i;

        // FOLLOW_ON its hard to find everything related to a table id in the logs across processes,
        // especially when the
        // table id is like "b". Was trying to follow a single table across multiple manager workers
        // processes.
        var tableOpsFuture = executor.submit(() -> {
          int loops = 0;
          while (!stop.get() || loops == 0) {
            client.tableOperations().create(table);
            log.info("Created table {}", table);
            if (stop.get() && loops > 0) {
              break;
            }
            var expectedRows = new HashSet<String>();
            try (var writer = client.createBatchWriter(table)) {
              for (int r = 0; r < 10; r++) {
                var row = String.format("%03d", r);
                expectedRows.add(row);
                Mutation m = new Mutation(row);
                m.put("f", "q", "v");
                writer.addMutation(m);
              }
            }
            log.info("Wrote data to table {}", table);
            if (stop.get() && loops > 0) {
              break;
            }
            client.tableOperations().addSplits(table, splits);
            log.info("Split table {}", table);
            if (stop.get() && loops > 0) {
              break;
            }
            client.tableOperations().compact(table, new CompactionConfig().setWait(true));
            log.info("Compacted table {}", table);
            if (stop.get() && loops > 0) {
              break;
            }
            client.tableOperations().merge(table, null, null);
            log.info("Merged table {}", table);
            if (stop.get() && loops > 0) {
              break;
            }
            try (var scanner = client.createScanner(table)) {
              var rowsSeen =
                  scanner.stream().map(e -> e.getKey().getRowData().toString()).collect(toSet());
              assertEquals(expectedRows, rowsSeen);
              log.info("verified table {}", table);
            }
            client.tableOperations().delete(table);
            log.info("Deleted table {}", table);
            loops++;
          }
          return loops;
        });
        tableOpFutures.add(tableOpsFuture);
      }

      var ctx = getServerContext();

      var store = new UserFateStore<FateEnv>(ctx, SystemTables.FATE.tableName(), null, null);

      // Wait until three different manager are seen running fate operations.
      waitToSeeManagers(ctx, 3, store, false);

      // Start two new manager processes and wait until 5 managers are seen running fate operations
      managerWorkers.add(exec(FastFateCleanupManager.class));
      managerWorkers.add(exec(FastFateCleanupManager.class));
      waitToSeeManagers(ctx, 5, store, false);

      // Kill two assistant manager processes. Any fate operations that were running should resume
      // elsewhere. Should see three manager running operations after that.
      managerWorkers.get(2).destroy();
      managerWorkers.get(3).destroy();
      log.debug("Killed 2 managers");
      waitToSeeManagers(ctx, 3, store, true);

      // Delete the lock of the primary manager which should cause it to halt. Then wait to see two
      // assistant managers.
      var primaryManager = ctx.getServerPaths().getManager(true);
      ServiceLock.deleteLock(ctx.getZooSession().asReaderWriter(), primaryManager);
      log.debug("Deleted lock of primary manager");
      waitToSeeManagers(ctx, 2, store, true);

      stop.set(true);
      // Wait for the background operations to complete and ensure that none had errors. Managers
      // stoppping/starting should not cause any problems for Accumulo API operations.
      for (var tof : tableOpFutures) {
        int loops = tof.get();
        log.debug("Background thread loops {}", loops);
        // Check that each background thread made a least one loop over all its table operations.
        assertTrue(loops > 0);
      }
    }

    executor.shutdown();

    managerWorkers.forEach(Process::destroy);
  }

  private static void waitToSeeManagers(ClientContext context, int expectedManagers,
      UserFateStore<FateEnv> store, boolean managersKilled) {

    // Track what reservations exist when entering, want to see new reservations created during this
    // function call.
    var existingReservationUUIDs =
        store.getActiveReservations(Set.of(FatePartition.all(FateInstanceType.USER))).values()
            .stream().map(FateStore.FateReservation::getReservationUUID).collect(toSet());
    log.debug("existingReservationUUIDs {}", existingReservationUUIDs);

    var assistants =
        context.getServerPaths().getAssistantManagers(ServiceLockPaths.AddressSelector.all(), true);
    // Wait for there to be the expected number of managers in zookeeper. After manager processes
    // are kill these entries in zookeeper may persist for a bit.
    while (assistants.size() != expectedManagers) {
      UtilWaitThread.sleep(1);
      assistants = context.getServerPaths()
          .getAssistantManagers(ServiceLockPaths.AddressSelector.all(), true);
    }

    var expectedServers = assistants.stream().map(ServiceLockPath::getServer)
        .map(HostAndPort::fromString).collect(toSet());
    log.debug("managers seen in zookeeper :{}", expectedServers);

    Set<HostAndPort> reservationsSeen = new HashSet<>();
    Set<HostAndPort> extraSeen = new HashSet<>();
    Set<Character> expectedPrefixes =
        Set.of('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f');
    // Track fate uuid prefixes seen. This is done because fate is partitioned across managers by
    // uuid ranges. If all uuid prefixes are seen then it is an indication that fate ids are being
    // processed. After new manager processes are started or stopped the partitions should be
    // reassigned.
    Set<Character> seenPrefixes = new HashSet<>();

    while (reservationsSeen.size() < expectedManagers || !seenPrefixes.equals(expectedPrefixes)) {
      var reservations =
          store.getActiveReservations(Set.of(FatePartition.all(FateInstanceType.USER)));
      reservations.forEach((fateId, reservation) -> {
        var slp = ServiceLockPaths.parse(Optional.empty(), reservation.getLockID().path);
        if (slp.getType().equals(Constants.ZMANAGER_ASSISTANT_LOCK)) {
          var hostPort = HostAndPort.fromString(slp.getServer());
          if (expectedServers.contains(hostPort)) {
            if (!existingReservationUUIDs.contains(reservation.getReservationUUID())) {
              reservationsSeen.add(hostPort);
              Character prefix = fateId.getTxUUIDStr().charAt(0);
              if (seenPrefixes.add(prefix)) {
                log.debug("Saw fate uuid prefix {} in id {} still waiting for {}", prefix, fateId,
                    Sets.difference(expectedPrefixes, seenPrefixes));
              }
            }
          } else if (!managersKilled) {
            fail("Saw unexpected extra manager " + slp);
          } else {
            extraSeen.add(hostPort);
          }
        }
      });
      UtilWaitThread.sleep(1);
    }

    log.debug("managers seen in fate reservations :{}", reservationsSeen);
    if (managersKilled) {
      log.debug("killed managers seen in fate reservations : {}", extraSeen);
    }
    assertEquals(expectedManagers, reservationsSeen.size());
  }
}
