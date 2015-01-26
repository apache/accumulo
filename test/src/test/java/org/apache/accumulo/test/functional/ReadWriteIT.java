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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.PrintInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestMultiTableIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class ReadWriteIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(ReadWriteIT.class);

  static final int ROWS = 200000;
  static final int COLS = 1;
  static final String COLF = "colf";

  @Override
  protected int defaultTimeoutSeconds() {
    return 6 * 60;
  }

  @Test(expected = RuntimeException.class)
  public void invalidInstanceName() throws Exception {
    final Connector conn = getConnector();
    new ZooKeeperInstance("fake_instance_name", conn.getInstance().getZooKeepers());
  }

  @Test
  public void sunnyDay() throws Exception {
    // Start accumulo, create a table, insert some data, verify we can read it out.
    // Shutdown cleanly.
    log.debug("Starting Monitor");
    cluster.getClusterControl().startAllServers(ClusterServerType.MONITOR);
    Connector connector = getConnector();
    String tableName = getUniqueNames(1)[0];
    ingest(connector, ROWS, COLS, 50, 0, tableName);
    verify(connector, ROWS, COLS, 50, 0, tableName);
    String monitorLocation = null;
    while (null == monitorLocation) {
      monitorLocation = MonitorUtil.getLocation(getConnector().getInstance());
      if (null == monitorLocation) {
        log.debug("Could not fetch monitor HTTP address from zookeeper");
        Thread.sleep(2000);
      }
    }
    URL url = new URL("http://" + monitorLocation);
    log.debug("Fetching web page " + url);
    String result = FunctionalTestUtils.readAll(url.openStream());
    assertTrue(result.length() > 100);
    log.debug("Stopping accumulo cluster");
    ClusterControl control = cluster.getClusterControl();
    control.adminStopAll();
    ZooReader zreader = new ZooReader(connector.getInstance().getZooKeepers(), connector.getInstance().getZooKeepersSessionTimeOut());
    ZooCache zcache = new ZooCache(zreader, null);
    byte[] masterLockData;
    do {
      masterLockData = ZooLock.getLockData(zcache, ZooUtil.getRoot(connector.getInstance()) + Constants.ZMASTER_LOCK, null);
      if (null != masterLockData) {
        log.info("Master lock is still held");
        Thread.sleep(1000);
      }
    } while (null != masterLockData);

    control.stopAllServers(ClusterServerType.GARBAGE_COLLECTOR);
    control.stopAllServers(ClusterServerType.MONITOR);
    control.stopAllServers(ClusterServerType.TRACER);
    log.debug("success!");
    // Restarting everything
    cluster.start();
  }

  public static void ingest(Connector connector, int rows, int cols, int width, int offset, String tableName) throws Exception {
    ingest(connector, rows, cols, width, offset, COLF, tableName);
  }

  public static void ingest(Connector connector, int rows, int cols, int width, int offset, String colf, String tableName) throws Exception {
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = rows;
    opts.cols = cols;
    opts.dataSize = width;
    opts.startRow = offset;
    opts.columnFamily = colf;
    opts.createTable = true;
    opts.tableName = tableName;

    TestIngest.ingest(connector, opts, new BatchWriterOpts());
  }

  public static void verify(Connector connector, int rows, int cols, int width, int offset, String tableName) throws Exception {
    verify(connector, rows, cols, width, offset, COLF, tableName);
  }

  private static void verify(Connector connector, int rows, int cols, int width, int offset, String colf, String tableName) throws Exception {
    ScannerOpts scannerOpts = new ScannerOpts();
    VerifyIngest.Opts opts = new VerifyIngest.Opts();
    opts.rows = rows;
    opts.cols = cols;
    opts.dataSize = width;
    opts.startRow = offset;
    opts.columnFamily = colf;
    opts.tableName = tableName;
    VerifyIngest.verifyIngest(connector, opts, scannerOpts);
  }

  public static String[] args(String... args) {
    return args;
  }

  @Test
  public void multiTableTest() throws Exception {
    // Write to multiple tables
    final String instance = cluster.getInstanceName();
    final String keepers = cluster.getZooKeepers();
    final ClusterControl control = cluster.getClusterControl();
    final String prefix = getClass().getSimpleName() + "_" + testName.getMethodName();
    ExecutorService svc = Executors.newFixedThreadPool(2);
    Future<Integer> p1 = svc.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        try {
          return control.exec(
              TestMultiTableIngest.class,
              args("--count", "" + ROWS, "-u", "root", "-i", instance, "-z", keepers, "-p", new String(((PasswordToken) getToken()).getPassword(),
                  Charsets.UTF_8), "--tablePrefix", prefix));
        } catch (IOException e) {
          log.error("Error running MultiTableIngest", e);
          return -1;
        }
      }
    });
    Future<Integer> p2 = svc.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        try {
          return control.exec(
              TestMultiTableIngest.class,
              args("--count", "" + ROWS, "--readonly", "-u", "root", "-i", instance, "-z", keepers, "-p", new String(
                  ((PasswordToken) getToken()).getPassword(), Charsets.UTF_8), "--tablePrefix", prefix));
        } catch (IOException e) {
          log.error("Error running MultiTableIngest", e);
          return -1;
        }
      }
    });
    svc.shutdown();
    while (!svc.isTerminated()) {
      svc.awaitTermination(15, TimeUnit.SECONDS);
    }
    assertEquals(0, p1.get().intValue());
    assertEquals(0, p2.get().intValue());
  }

  @Test
  public void largeTest() throws Exception {
    // write a few large values
    Connector connector = getConnector();
    String table = getUniqueNames(1)[0];
    ingest(connector, 2, 1, 500000, 0, table);
    verify(connector, 2, 1, 500000, 0, table);
  }

  @Test
  public void interleaved() throws Exception {
    // read and write concurrently
    final Connector connector = getConnector();
    final String tableName = getUniqueNames(1)[0];
    interleaveTest(connector, tableName);
  }

  static void interleaveTest(final Connector connector, final String tableName) throws Exception {
    final AtomicBoolean fail = new AtomicBoolean(false);
    final int CHUNKSIZE = ROWS / 10;
    ingest(connector, CHUNKSIZE, 1, 50, 0, tableName);
    int i;
    for (i = 0; i < ROWS; i += CHUNKSIZE) {
      final int start = i;
      Thread verify = new Thread() {
        @Override
        public void run() {
          try {
            verify(connector, CHUNKSIZE, 1, 50, start, tableName);
          } catch (Exception ex) {
            fail.set(true);
          }
        }
      };
      verify.start();
      ingest(connector, CHUNKSIZE, 1, 50, i + CHUNKSIZE, tableName);
      verify.join();
      assertFalse(fail.get());
    }
    verify(connector, CHUNKSIZE, 1, 50, i, tableName);
  }

  public static Text t(String s) {
    return new Text(s);
  }

  public static Mutation m(String row, String cf, String cq, String value) {
    Mutation m = new Mutation(t(row));
    m.put(t(cf), t(cq), new Value(value.getBytes()));
    return m;
  }

  @Test
  public void localityGroupPerf() throws Exception {
    // verify that locality groups can make look-ups faster
    final Connector connector = getConnector();
    final String tableName = getUniqueNames(1)[0];
    connector.tableOperations().create(tableName);
    connector.tableOperations().setProperty(tableName, "table.group.g1", "colf");
    connector.tableOperations().setProperty(tableName, "table.groups.enabled", "g1");
    ingest(connector, 2000, 1, 50, 0, tableName);
    connector.tableOperations().compact(tableName, null, null, true, true);
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    bw.addMutation(m("zzzzzzzzzzz", "colf2", "cq", "value"));
    bw.close();
    long now = System.currentTimeMillis();
    Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    scanner.fetchColumnFamily(new Text("colf"));
    FunctionalTestUtils.count(scanner);
    long diff = System.currentTimeMillis() - now;
    now = System.currentTimeMillis();
    scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    scanner.fetchColumnFamily(new Text("colf2"));
    FunctionalTestUtils.count(scanner);
    bw.close();
    long diff2 = System.currentTimeMillis() - now;
    assertTrue(diff2 < diff);
  }

  @Test
  public void sunnyLG() throws Exception {
    // create a locality group, write to it and ensure it exists in the RFiles that result
    final Connector connector = getConnector();
    final String tableName = getUniqueNames(1)[0];
    connector.tableOperations().create(tableName);
    Map<String,Set<Text>> groups = new TreeMap<String,Set<Text>>();
    groups.put("g1", Collections.singleton(t("colf")));
    connector.tableOperations().setLocalityGroups(tableName, groups);
    ingest(connector, 2000, 1, 50, 0, tableName);
    verify(connector, 2000, 1, 50, 0, tableName);
    connector.tableOperations().flush(tableName, null, null, true);
    BatchScanner bscanner = connector.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 1);
    String tableId = connector.tableOperations().tableIdMap().get(tableName);
    bscanner.setRanges(Collections.singletonList(new Range(new Text(tableId + ";"), new Text(tableId + "<"))));
    bscanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    boolean foundFile = false;
    for (Entry<Key,Value> entry : bscanner) {
      foundFile = true;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream newOut = new PrintStream(baos);
      PrintStream oldOut = System.out;
      try {
        System.setOut(newOut);
        PrintInfo.main(new String[] {entry.getKey().getColumnQualifier().toString()});
        newOut.flush();
        String stdout = baos.toString();
        assertTrue(stdout.contains("Locality group         : g1"));
        assertTrue(stdout.contains("families      : [colf]"));
      } finally {
        newOut.close();
        System.setOut(oldOut);
      }
    }
    bscanner.close();
    assertTrue(foundFile);
  }

  @Test
  public void localityGroupChange() throws Exception {
    // Make changes to locality groups and ensure nothing is lostssh
    final Connector connector = getConnector();
    String table = getUniqueNames(1)[0];
    TableOperations to = connector.tableOperations();
    to.create(table);
    String[] config = new String[] {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf,xyz;lg2:c1,c2"};
    int i = 0;
    for (String cfg : config) {
      to.setLocalityGroups(table, getGroups(cfg));
      ingest(connector, ROWS * (i + 1), 1, 50, ROWS * i, table);
      to.flush(table, null, null, true);
      verify(connector, 0, 1, 50, ROWS * (i + 1), table);
      i++;
    }
    to.delete(table);
    to.create(table);
    config = new String[] {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf;lg2:colf",};
    i = 1;
    for (String cfg : config) {
      ingest(connector, ROWS * i, 1, 50, 0, table);
      ingest(connector, ROWS * i, 1, 50, 0, "xyz", table);
      to.setLocalityGroups(table, getGroups(cfg));
      to.flush(table, null, null, true);
      verify(connector, ROWS * i, 1, 50, 0, table);
      verify(connector, ROWS * i, 1, 50, 0, "xyz", table);
      i++;
    }
  }

  private Map<String,Set<Text>> getGroups(String cfg) {
    Map<String,Set<Text>> groups = new TreeMap<String,Set<Text>>();
    if (cfg != null) {
      for (String group : cfg.split(";")) {
        String[] parts = group.split(":");
        Set<Text> cols = new HashSet<Text>();
        for (String col : parts[1].split(",")) {
          cols.add(t(col));
        }
        groups.put(parts[1], cols);
      }
    }
    return groups;
  }

}
