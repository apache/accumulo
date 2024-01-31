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
package org.apache.accumulo.test.functional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterControl;
import org.apache.accumulo.server.manager.state.MetaDataTableScanner;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ManagerAssignmentIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);

      // wait for the table to be online
      Wait.waitFor(() -> getTabletLocationState(c, tableId) != null, SECONDS.toMillis(60), 250);

      TabletLocationState newTablet = getTabletLocationState(c, tableId);
      assertNull(newTablet.last);
      assertNull(newTablet.future);

      // put something in it
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("a");
        m.put("b", "c", "d");
        bw.addMutation(m);
      }
      // give it a last location
      c.tableOperations().flush(tableName, null, null, true);

      TabletLocationState flushed = getTabletLocationState(c, tableId);
      assertEquals(newTablet.current, flushed.current);
      assertEquals(flushed.getCurrentServer(), flushed.getLastServer());
      assertNull(newTablet.future);

      // take the tablet offline
      c.tableOperations().offline(tableName, true);
      TabletLocationState offline = getTabletLocationState(c, tableId);
      assertNull(offline.future);
      assertNull(offline.current);
      assertEquals(flushed.getCurrentServer(), offline.getLastServer());

      // put it back online
      c.tableOperations().online(tableName, true);
      TabletLocationState online = getTabletLocationState(c, tableId);
      assertNull(online.future);
      assertNotNull(online.current);
      assertEquals(online.getCurrentServer(), online.getLastServer());
    }
  }

  @Test
  public void testShutdownOnlyTServerWithUserTable() throws Exception {

    // 2 TabletServers started for this test, shut them down so we only have 1.
    getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
    ((MiniAccumuloClusterControl) getClusterControl()).start(ServerType.TABLET_SERVER,
        Collections.emptyMap(), 1);

    String tableName = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1,
          SECONDS.toMillis(60), SECONDS.toMillis(2));

      client.tableOperations().create(tableName);

      // wait for everything to be hosted and balanced
      client.instanceOperations().waitForBalance();

      try (var writer = client.createBatchWriter(tableName)) {
        for (int i = 0; i < 1000000; i++) {
          Mutation m = new Mutation(String.format("%08d", i));
          m.put("", "", "");
          writer.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName, null, null, true);

      final CountDownLatch latch = new CountDownLatch(10);

      Runnable task = () -> {
        while (true) {
          try (var scanner = new IsolatedScanner(client.createScanner(tableName))) {
            // TODO maybe do not close scanner? The following limit was placed on the stream to
            // avoid reading all the data possibly leaving a scan session active on the tserver
            AtomicInteger count = new AtomicInteger(0);
            scanner.forEach(e -> {
              // let the test thread know that this thread has read some data
              if (count.incrementAndGet() == 1_000) {
                latch.countDown();
              }
            });
          } catch (Exception e) {
            e.printStackTrace();
            break;
          }
        }
      };

      ExecutorService service = Executors.newFixedThreadPool(10);
      for (int i = 0; i < 10; i++) {
        service.execute(task);
      }

      // Wait until all threads are reading some data
      latch.await();

      // getClusterControl().stopAllServers(ServerType.TABLET_SERVER)
      // could potentially send a kill -9 to the process. Shut the tablet
      // servers down in a more graceful way.

      Locations locs = client.tableOperations().locate(tableName,
          Collections.singletonList(TabletsSection.getRange()));
      locs.groupByTablet().keySet().stream().map(locs::getTabletLocation).forEach(location -> {
        HostAndPort address = HostAndPort.fromString(location);
        String addressWithSession = address.toString();
        var zLockPath = ServiceLock.path(getCluster().getServerContext().getZooKeeperRoot()
            + Constants.ZTSERVERS + "/" + address);
        long sessionId =
            ServiceLock.getSessionId(getCluster().getServerContext().getZooCache(), zLockPath);
        if (sessionId != 0) {
          addressWithSession = address + "[" + Long.toHexString(sessionId) + "]";
        }

        final String finalAddress = addressWithSession;
        System.out.println("Attempting to shutdown TabletServer at: " + address);
        try {
          ThriftClientTypes.MANAGER.executeVoid((ClientContext) client,
              c -> c.shutdownTabletServer(TraceUtil.traceInfo(),
                  getCluster().getServerContext().rpcCreds(), finalAddress, false));
        } catch (AccumuloException | AccumuloSecurityException e) {
          fail("Error shutting down TabletServer", e);
        }

      });

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 0);

    }
  }

  @Test
  public void testShutdownOnlyTServerWithoutUserTable() throws Exception {

    // 2 TabletServers started for this test, shut them down so we only have 1.
    getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
    ((MiniAccumuloClusterControl) getClusterControl()).start(ServerType.TABLET_SERVER,
        Collections.emptyMap(), 1);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1,
          SECONDS.toMillis(60), SECONDS.toMillis(2));

      client.instanceOperations().waitForBalance();

      // getClusterControl().stopAllServers(ServerType.TABLET_SERVER)
      // could potentially send a kill -9 to the process. Shut the tablet
      // servers down in a more graceful way.

      Locations locs = client.tableOperations().locate(AccumuloTable.ROOT.tableName(),
          Collections.singletonList(TabletsSection.getRange()));
      locs.groupByTablet().keySet().stream().map(locs::getTabletLocation).forEach(location -> {
        HostAndPort address = HostAndPort.fromString(location);
        String addressWithSession = address.toString();
        var zLockPath = ServiceLock.path(getCluster().getServerContext().getZooKeeperRoot()
            + Constants.ZTSERVERS + "/" + address);
        long sessionId =
            ServiceLock.getSessionId(getCluster().getServerContext().getZooCache(), zLockPath);
        if (sessionId != 0) {
          addressWithSession = address + "[" + Long.toHexString(sessionId) + "]";
        }

        final String finalAddress = addressWithSession;
        System.out.println("Attempting to shutdown TabletServer at: " + address);
        try {
          ThriftClientTypes.MANAGER.executeVoid((ClientContext) client,
              c -> c.shutdownTabletServer(TraceUtil.traceInfo(),
                  getCluster().getServerContext().rpcCreds(), finalAddress, false));
        } catch (AccumuloException | AccumuloSecurityException e) {
          fail("Error shutting down TabletServer", e);
        }

      });

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 0);

    }
  }

  private TabletLocationState getTabletLocationState(AccumuloClient c, String tableId) {
    try (MetaDataTableScanner s = new MetaDataTableScanner((ClientContext) c,
        new Range(TabletsSection.encodeRow(TableId.of(tableId), null)),
        AccumuloTable.METADATA.tableName())) {
      return s.next();
    }
  }
}
