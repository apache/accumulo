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
package org.apache.accumulo.test;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AllowScansToBeInterruptedIT {
  private static final Logger log = Logger.getLogger(AllowScansToBeInterruptedIT.class);

  public static TemporaryFolder folder = new TemporaryFolder();
  private MiniAccumuloCluster accumulo;
  private String secret = "secret";

  @Before
  public void setUp() throws Exception {
    folder.create();
    log.info("Using MAC at " + folder.getRoot());
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.getRoot(), secret);
    cfg.setNumTservers(1);
    accumulo = new MiniAccumuloCluster(cfg);
    accumulo.start();
  }

  @After
  public void tearDown() throws Exception {
    accumulo.stop();
    folder.delete();
  }

  Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    ZooKeeperInstance zki = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    return zki.getConnector("root", new PasswordToken(secret));
  }

  @Test(timeout = 60 * 1000)
  public void test() throws Exception {
    // make a table
    final String tableName = "test";
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);
    // make the world's slowest scanner
    final Scanner scanner = conn.createScanner(tableName, Constants.NO_AUTHS);
    final IteratorSetting cfg = new IteratorSetting(100, SlowIterator.class);
    SlowIterator.setSeekSleepTime(cfg, 99999 * 1000);
    scanner.addScanIterator(cfg);
    // create a thread to interrupt the slow scan
    final Thread scanThread = Thread.currentThread();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          // ensure the scan is running: not perfect, the metadata tables could be scanned, too.
          String tserver = conn.instanceOperations().getTabletServers().iterator().next();
          List<ActiveScan> scans = null;
          while (null == scans) {
            try {
              // Sometimes getting errors the first time around
              scans = conn.instanceOperations().getActiveScans(tserver);
            } catch (Exception e) {
              log.warn("Could not connect to tserver " + tserver, e);
            }
          }
          while (scans.size() < 1) {
            UtilWaitThread.sleep(1000);
            scans = conn.instanceOperations().getActiveScans(tserver);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        // BAM!
        scanThread.interrupt();
      }
    };
    thread.start();
    try {
      // Use the scanner, expect problems
      for (@SuppressWarnings("unused")
      Entry<Key,Value> entry : scanner) {}
      Assert.fail("Scan should not succeed");
    } catch (Exception ex) {} finally {
      thread.join();
    }
  }

}
