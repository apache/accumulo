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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ScanServerDiscovery;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ScanServerLocator.NoAvailableScanServerException;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
    String discoveryRootPath = zooRoot + Constants.ZSSERVERS_DISCOVERY;

    while (zrw.getChildren(discoveryRootPath).size() == 0) {
      Thread.sleep(500);
    }
  }

  @AfterClass
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testScanServerDiscovery() throws Exception {
    String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    String discoveryRootPath = zooRoot + Constants.ZSSERVERS_DISCOVERY;

    List<String> children = zrw.getChildren(discoveryRootPath);
    assertEquals(1, children.size());

    String scanServerAddress = ScanServerDiscovery.reserve(zooRoot, zrw);
    assertNotNull(scanServerAddress);
    assertEquals(scanServerAddress, children.get(0));

    String scanServerAddress2 = ScanServerDiscovery.reserve(zooRoot, zrw);
    assertNull(scanServerAddress2);
  }

  @Test
  public void testScan() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setUseScanServer(true);
        int count = 0;
        for (Entry<Key,Value> entry : scanner) {
          count++;
        }
        assertEquals(100, count);
      } // when the scanner is closed, all open sessions should be closed

      List<String> tservers = client.instanceOperations().getTabletServers();
      int activeScans = 0;
      for (String tserver : tservers) {
        activeScans += client.instanceOperations().getActiveScans(tserver).size();
      }
      assertTrue(activeScans == 0);
    }

  }

  @Test
  public void testScanServerBusy() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build();
        AccumuloClient client2 = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      scanner.setUseScanServer(true);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      iter.next();
      iter.next();

      Scanner scanner2 = client2.createScanner(tableName, Authorizations.EMPTY);
      scanner2.setRange(new Range());
      scanner2.setUseScanServer(true);
      Iterator<Entry<Key,Value>> iter2 = scanner2.iterator();
      try {
        iter2.next();
        fail("Expecting NoAvailableScanServerException");
      } catch (RuntimeException e) {
        if (e.getCause() instanceof NoAvailableScanServerException) {
          // success
        } else {
          fail("Expecting NoAvailableScanServerException");
        }
      }
    }
  }

  @Test
  public void testClientTimesOut() throws Exception {
    String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    String discoveryRootPath = zooRoot + Constants.ZSSERVERS_DISCOVERY;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      scanner.setUseScanServer(true);
      // We only inserted 100 rows, default batch size is 1000. If we don't set the
      // batch size lower, then the ThriftScanner code will automatically close the
      // scanner. We want to keep it open, so lower the batch size.
      scanner.setBatchSize(2);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      assertNotNull(iter.next());
      assertNotNull(iter.next());

      // TSERV_SESSION_MAXIDLE is set to 3s. The server side code should check
      // every 1.5s to see if the session is idle and close the scan if it is
      // idle for 3s or more. Wait for 2x the idle time and then check to see
      // if the scan server is available in ZK
      Thread.sleep(6000);
      assertEquals("Expecting scan server to be closed on server side", 1,
          zrw.getChildren(discoveryRootPath).size());
      // The server side scan was closed and the ScanServer released back into
      // the pool of available scan servers. This next call to iter.next() is
      // going to reallocate the scan server for the purposes of continuing the
      // scan.
      iter.next();
      assertEquals("Expecting scan server to be allocated", 0,
          zrw.getChildren(discoveryRootPath).size());
      scanner.close();
      // The close happens asynchronously, wait for the scan server
      // to become available after the close.
      while (zrw.getChildren(discoveryRootPath).size() == 0) {
        Thread.sleep(50);
      }
      assertEquals("Expecting scan server to be available", 1,
          zrw.getChildren(discoveryRootPath).size());

      Scanner scanner2 = client.createScanner(tableName, Authorizations.EMPTY);
      scanner2.setRange(new Range());
      scanner2.setUseScanServer(true);
      Iterator<Entry<Key,Value>> iter2 = scanner2.iterator();
      assertNotNull(iter2.next());
      assertNotNull(iter2.next());
      scanner2.close();
      // The close happens asynchronously, wait for the scan server
      // to become available after the close.
      while (zrw.getChildren(discoveryRootPath).size() == 0) {
        Thread.sleep(50);
      }
      assertEquals(1, zrw.getChildren(discoveryRootPath).size());
    }
  }

}
