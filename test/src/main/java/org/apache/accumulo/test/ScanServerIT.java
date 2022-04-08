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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.conf.ClientProperty;
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
import org.apache.accumulo.test.functional.SlowIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.collect.Iterables;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerIT extends SharedMiniClusterBase {

  private static class ScanServerITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(1);

      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");

      // Configure the scan server to only have 1 scan executor thread. This means
      // that the scan server will run scans serially, not concurrently.
      cfg.setProperty(Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS, "1");
    }
  }

  @BeforeAll
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

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
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
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(100, Iterables.size(scanner));
        // if scanning against tserver would see the following, but should not on scan server
        ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 10, tableName);
        assertEquals(100, Iterables.size(scanner));
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(200, Iterables.size(scanner));
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
        assertEquals(100, Iterables.size(scanner));
        ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 10, tableName);
        assertEquals(100, Iterables.size(scanner));
        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(200, Iterables.size(scanner));
      } // when the scanner is closed, all open sessions should be closed
    }
  }

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
          assertEquals(100, Iterables.size(scanner));
        } // when the scanner is closed, all open sessions should be closed
      });
    }
  }

  @Test
  @Timeout(value = 20)
  public void testScannerTimeout() throws Exception {
    // Configure the client to use different scan server dispatcher property values
    Properties props = getClientProps();
    props.put(ClientProperty.SCAN_SERVER_DISPATCHER_OPTS_PREFIX.getKey() + "initialBusyTimeout",
        "PT0.100S");
    props.put(ClientProperty.SCAN_SERVER_DISPATCHER_OPTS_PREFIX.getKey() + "maxBusyTimeout",
        "PT1.000S");

    String tName = null;
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      tName = getUniqueNames(1)[0];
      client.tableOperations().create(tName);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tName);
      client.tableOperations().flush(tName, null, null, true);

      Scanner scanner = client.createScanner(tName, Authorizations.EMPTY);
      IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
      SlowIterator.setSleepTime(slow, 30000);
      SlowIterator.setSeekSleepTime(slow, 30000);
      scanner.addScanIterator(slow);
      scanner.setRange(new Range());
      scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      scanner.setTimeout(10, TimeUnit.SECONDS);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      if (iter.hasNext()) {
        fail("Should not get here");
      }
    }
  }

  @Test
  @Timeout(value = 20)
  public void testBatchScannerTimeout() throws Exception {
    // Configure the client to use different scan server dispatcher property values
    Properties props = getClientProps();
    props.put(ClientProperty.SCAN_SERVER_DISPATCHER_OPTS_PREFIX.getKey() + "initialBusyTimeout",
        "PT0.100S");
    props.put(ClientProperty.SCAN_SERVER_DISPATCHER_OPTS_PREFIX.getKey() + "maxBusyTimeout",
        "PT1.000S");

    String tName = null;
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      tName = getUniqueNames(1)[0];
      client.tableOperations().create(tName);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tName);
      client.tableOperations().flush(tName, null, null, true);

      try (BatchScanner bs = client.createBatchScanner(tName)) {
        bs.setRanges(Collections.singletonList(new Range()));
        bs.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        // should not timeout
        for (Entry<Key,Value> entry : bs) {
          assertNotNull(entry.getKey());
        }

        bs.setTimeout(5, TimeUnit.SECONDS);
        IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
        iterSetting.addOption("sleepTime", 2000 + "");
        bs.addScanIterator(iterSetting);

        assertThrows(TimedOutException.class, () -> bs.iterator().next(),
            "batch scanner did not time out");
      }
    }
  }
}
