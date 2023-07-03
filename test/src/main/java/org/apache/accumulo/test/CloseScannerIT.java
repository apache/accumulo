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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

public class CloseScannerIT extends AccumuloClusterHarness {

  static final int ROWS = 1000;
  static final int COLS = 1000;

  @Test
  public void testManyScans() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, ROWS, COLS, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      for (int i = 0; i < 200; i++) {
        try (Scanner scanner = createScanner(client, tableName, i)) {
          scanner.setRange(new Range());
          scanner.setReadaheadThreshold(i % 2 == 0 ? 0 : 3);

          for (int j = 0; j < i % 7 + 1; j++) {
            // only read a little data and quit, this should leave a session open on the tserver
            scanner.stream().limit(10).forEach(e -> {});
          }
        } // when the scanner is closed, all open sessions should be closed
      }

      List<String> tservers = client.instanceOperations().getTabletServers();
      int activeScans = 0;
      for (String tserver : tservers) {
        activeScans += client.instanceOperations().getActiveScans(tserver).size();
      }

      assertTrue(activeScans < 3);
    }
  }

  @Test
  public void testIsolatedScannerPreventsTServerShutdown() throws Exception {

    // 2 TabletServers started for this test, shut them down so we only have 1.
//    getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
//    ((MiniAccumuloClusterControl) getClusterControl()).start(ServerType.TABLET_SERVER,
//        Collections.emptyMap(), 1);

    String tableName = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

//      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1);
      
//      Thread.sleep(120_000);
      
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

      Runnable task = new Runnable() {
        @Override
        public void run() {
          while (true) {
            try (var scanner = new IsolatedScanner(client.createScanner(tableName))) {
              // TODO maybe do not close scanner? The following limit was placed on the stream to
              // avoid reading all the data possibly leaving a scan session active on the tserver
              int count = 0;
              for (Entry<Key,Value> e : scanner) {
                count++;
                // let the test thread know that this thread has read some data
                if (count == 1_000) {
                  latch.countDown();
                }
              }
            } catch (Exception e) {
              e.printStackTrace();
              break;
            }
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
      
      Locations locs = client.tableOperations().locate(tableName, Collections.singletonList(TabletsSection.getRange()));
      locs.groupByTablet().keySet().stream().map(tid -> locs.getTabletLocation(tid)).forEach(location -> {
        HostAndPort address = HostAndPort.fromString(location);
        String addressWithSession = address.toString();
        var zLockPath = ServiceLock.path(getCluster().getServerContext().getZooKeeperRoot()
            + Constants.ZTSERVERS + "/" + address.toString());
        long sessionId =
            ServiceLock.getSessionId(getCluster().getServerContext().getZooCache(), zLockPath);
        if (sessionId != 0) {
          addressWithSession = address.toString() + "[" + Long.toHexString(sessionId) + "]";
        }

        final String finalAddress = addressWithSession;
        System.out.println("Attempting to shutdown TabletServer at: " + address.toString());
        try {
          ThriftClientTypes.MANAGER.executeVoid((ClientContext) client,
              c -> c.shutdownTabletServer(TraceUtil.traceInfo(),
                  getCluster().getServerContext().rpcCreds(), finalAddress, false));
        } catch (AccumuloException | AccumuloSecurityException e) {
          fail("Error shutting down TabletServer", e);
        }
        
      });

      Wait.waitFor(() -> client.instanceOperations().getTabletServers().size() == 1);

    }
  }

  private static Scanner createScanner(AccumuloClient client, String tableName, int i)
      throws Exception {
    Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
    if (i % 2 == 0) {
      scanner = new IsolatedScanner(scanner);
    }
    return scanner;
  }
}
