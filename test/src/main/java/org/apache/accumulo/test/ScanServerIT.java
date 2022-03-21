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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanTimedOutException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.DefaultScanServerDispatcher;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
      cfg.setProperty(Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS, "1");
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

  public static class TimeOutEarlyScanServerDispatcher extends DefaultScanServerDispatcher {

    @Override
    public void init(InitParameters params) {
      super.init(params);
      var opts = params.getOptions();
      initialBusyTimeout = Duration.parse(opts.getOrDefault("initialBusyTimeout", "PT0.100S"));
      maxBusyTimeout = Duration.parse(opts.getOrDefault("maxBusyTimeout", "PT1.000S"));
    }

  }

  @Test
  public void testScanServerBusy() throws Exception {

    // Configure the client to use a different scan server dispatcher class that has
    // lower timeout values
    Properties props = getClientProps();
    props.put(ClientProperty.SCAN_SERVER_DISPATCHER_OPTS_PREFIX.getKey(),
        TimeOutEarlyScanServerDispatcher.class.getName());

    String tName = null;
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      tName = getUniqueNames(1)[0];
      client.tableOperations().create(tName);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tName);
      client.tableOperations().flush(tName, null, null, true);
    }

    final String tableName = tName;
    Thread t1 = new Thread(() -> {
      try (AccumuloClient c = Accumulo.newClient().from(props).build()) {
        Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
        IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
        SlowIterator.setSleepTime(slow, 30000);
        scanner.addScanIterator(slow);
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        // We only inserted 100 rows, default batch size is 1000. If we don't set the
        // batch size lower, then the server side code will automatically close the
        // scanner on the first call to continueScan because it will have consumed all
        // of the data. We want to keep it open, so lower the batch size.
        scanner.setBatchSize(1);
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        while (iter.hasNext()) {
          assertNotNull(iter.next());
        }
      } catch (TableNotFoundException e) {
        fail("Table not found");
      }
    }, "first-scan");

    Thread t2 = new Thread(() -> {
      try (AccumuloClient c = Accumulo.newClient().from(props).build()) {
        // At this point the tablet server will time out this scan after TSERV_SESSION_MAXIDLE
        // Start up another scanner and set it to time out in 1s. It should fail because there
        // is no scan server available to run the scan.
        Scanner scanner2 = c.createScanner(tableName, Authorizations.EMPTY);
        scanner2.setRange(new Range());
        scanner2.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner2.setBatchSize(1);
        Iterator<Entry<Key,Value>> iter2 = scanner2.iterator();
        try {
          while (iter2.hasNext()) {
            assertNotNull(iter2.next());
          }
          fail("Expecting ScanTimedOutException");
        } catch (RuntimeException e) {
          if (e.getCause() instanceof ScanTimedOutException) {
            // success
          } else {
            fail("Expecting ScanTimedOutException");
          }
        }
      } catch (TableNotFoundException e) {
        fail("Table not found");
      }
    }, "second-scan");

    t1.start();
    t2.start();

    t2.join();
    t1.interrupt();
    t1.join();
  }

}
