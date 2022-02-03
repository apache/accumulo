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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanTimedOutException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ScanServerIT extends SharedMiniClusterBase {

  private static class ScanServerITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(1);
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");
    }
  }

  @BeforeClass
  public static void start() throws Exception {
    ScanServerITConfiguration c = new ScanServerITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
    SharedMiniClusterBase.getCluster().getClusterControl().start(ServerType.SCAN_SERVER,
        "localhost");

    String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    String scanServerRoot = zooRoot + Constants.ZSSERVERS;

    while (zrw.getChildren(scanServerRoot).size() == 0) {
      Thread.sleep(500);
    }
  }

  @AfterClass
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  // @Test
  // public void testScanServerDiscovery() throws Exception {
  // String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
  // ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
  // String discoveryRootPath = zooRoot + Constants.ZSSERVERS_DISCOVERY;
  //
  // List<String> children = zrw.getChildren(discoveryRootPath);
  // assertEquals(1, children.size());
  //
  // String scanServerAddress = ScanServerDiscovery.reserve(zooRoot, zrw);
  // assertNotNull(scanServerAddress);
  // assertEquals(scanServerAddress, children.get(0));
  //
  // String scanServerAddress2 = ScanServerDiscovery.reserve(zooRoot, zrw);
  // assertNull(scanServerAddress2);
  // }

  @Test
  public void testScan() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        int count = 0;
        for (@SuppressWarnings("unused")
        Entry<Key,Value> entry : scanner) {
          count++;
        }
        assertEquals(100, count);
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  @Test
  public void testBatchScan() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singletonList(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        int count = 0;
        for (@SuppressWarnings("unused")
        Entry<Key,Value> entry : scanner) {
          count++;
        }
        assertEquals(100, count);
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  // TODO: This test currently fails, but we could change the client code to make it work.
  @Test
  public void testScanOfflineTable() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);
      client.tableOperations().offline(tableName, true);

      assertThrows(TableOfflineException.class, () -> {
        try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRange(new Range());
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          int count = 0;
          for (@SuppressWarnings("unused")
          Entry<Key,Value> entry : scanner) {
            count++;
          }
          assertEquals(100, count);
        } // when the scanner is closed, all open sessions should be closed
      });
    }
  }

  @Ignore
  @Test
  public void testScanServerBusy() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      // We only inserted 100 rows, default batch size is 1000. If we don't set the
      // batch size lower, then the server side code will automatically close the
      // scanner on the first call to continueScan. We want to keep it open, so lower the batch
      // size.
      scanner.setBatchSize(10);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      iter.next();
      iter.next();
      // At this point the tablet server will time out this scan after TSERV_SESSION_MAXIDLE
      // Start up another scanner and set it to time out in 1s. It should fail because there
      // is no scan server available to run the scan.
      Scanner scanner2 = client.createScanner(tableName, Authorizations.EMPTY);
      scanner2.setRange(new Range());
      scanner2.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      scanner2.setTimeout(1, TimeUnit.SECONDS);
      Iterator<Entry<Key,Value>> iter2 = scanner2.iterator();
      try {
        iter2.hasNext();
        assertNotNull(iter2.next());
        fail("Expecting ScanTimedOutException");
      } catch (RuntimeException e) {
        if (e.getCause() instanceof ScanTimedOutException) {
          // success
        } else {
          fail("Expecting ScanTimedOutException");
        }
      } finally {
        scanner.close();
        scanner2.close();
        // The close happens asynchronously, wait for the scan server
        // to become available after the close.
      }
    }
  }

  @Ignore
  @Test
  public void testServerTimesOut() throws Exception {
    // String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    // ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    // String discoveryRootPath = zooRoot + Constants.ZSSERVERS_DISCOVERY;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      // We only inserted 100 rows, default batch size is 1000. If we don't set the
      // batch size lower, then the ThriftScanner code will automatically close the
      // scanner. We want to keep it open, so lower the batch size.
      scanner.setBatchSize(2);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      assertTrue(iter.hasNext());
      Key k1 = iter.next().getKey();
      assertNotNull(k1);
      assertTrue(iter.hasNext());
      Key k2 = iter.next().getKey();
      assertNotNull(k2);
      assertEquals("Expecting key 2 to be after key 1", 1, k2.compareTo(k1));

      // TSERV_SESSION_MAXIDLE is set to 3s. The server side code should check
      // every 1.5s to see if the session is idle and close the scan if it is
      // idle for 3s or more. Wait for 2x the idle time and then check to see
      // if the scan server is available in ZK
      Thread.sleep(6000);
      // assertEquals("Expecting scan server to be closed on server side", 1,
      // zrw.getChildren(discoveryRootPath).size());
      // The server side scan was closed and the ScanServer released back into
      // the pool of available scan servers. This next call to iter.next() is
      // going to reallocate the scan server for the purposes of continuing the
      // scan.
      assertTrue(iter.hasNext());
      Key k3 = iter.next().getKey();
      assertNotNull(k3);
      assertEquals("Expecting key 3 to be after key 2", 1, k3.compareTo(k2));
      // assertEquals("Expecting scan server to be allocated", 0,
      // zrw.getChildren(discoveryRootPath).size());
      scanner.close();
      // The close happens asynchronously, wait for the scan server
      // to become available after the close.
      // while (zrw.getChildren(discoveryRootPath).size() == 0) {
      // Thread.sleep(50);
      // }
      // assertEquals("Expecting scan server to be available", 1,
      // zrw.getChildren(discoveryRootPath).size());

      Scanner scanner2 = client.createScanner(tableName, Authorizations.EMPTY);
      scanner2.setRange(new Range());
      scanner2.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      Iterator<Entry<Key,Value>> iter2 = scanner2.iterator();
      assertNotNull(iter2.next());
      assertNotNull(iter2.next());
      scanner2.close();
      // The close happens asynchronously, wait for the scan server
      // to become available after the close.
      // while (zrw.getChildren(discoveryRootPath).size() == 0) {
      // Thread.sleep(50);
      // }
      // assertEquals(1, zrw.getChildren(discoveryRootPath).size());
    }
  }

}
