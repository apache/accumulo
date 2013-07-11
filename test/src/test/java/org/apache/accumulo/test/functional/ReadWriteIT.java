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

import java.net.InetAddress;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.PrintInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestMultiTableIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ReadWriteIT extends MacTest {
  
  static final int ROWS = 200000;
  static final int COLS = 1;
  static final String COLF = "colf";
  
  @Test(timeout = 60 * 1000)
  public void sunnyDay() throws Exception {
    // Start accumulo, create a table, insert some data, verify we can read it out.
    // Shutdown cleanly.
    log.debug("Starting Monitor");
    Process monitor = cluster.exec(Monitor.class);
    Connector connector = getConnector();
    ingest(connector, ROWS, COLS, 50, 0);
    verify(connector, ROWS, COLS, 50, 0);
    URL url = new URL("http://" + InetAddress.getLocalHost().getHostName() + ":" + cluster.getConfig().getSiteConfig().get(Property.MONITOR_PORT.getKey()));
    log.debug("Fetching web page " + url);
    String result = FunctionalTestUtils.readAll(url.openStream());
    assertTrue(result.length() > 100);
    log.debug("Stopping mini accumulo cluster");
    Process shutdown = cluster.exec(Admin.class, "stopAll");
    shutdown.waitFor();
    assertTrue(shutdown.exitValue() == 0);
    log.debug("success!");
    monitor.destroy();
  }
  
  public static void ingest(Connector connector, int rows, int cols, int width, int offset) throws Exception {
    ingest(connector, rows, cols, width, offset, COLF);
  }
  
  public static void ingest(Connector connector, int rows, int cols, int width, int offset, String colf) throws Exception {
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = rows;
    opts.cols = cols;
    opts.dataSize = width;
    opts.startRow = offset;
    opts.columnFamily = colf;
    opts.createTable = true;
    TestIngest.ingest(connector, opts, new BatchWriterOpts());
  }
  
  private static void verify(Connector connector, int rows, int cols, int width, int offset) throws Exception {
    verify(connector, rows, cols, width, offset, COLF);
  }
  
  private static void verify(Connector connector, int rows, int cols, int width, int offset, String colf) throws Exception {
    ScannerOpts scannerOpts = new ScannerOpts();
    VerifyIngest.Opts opts = new VerifyIngest.Opts();
    opts.rows = rows;
    opts.cols = cols;
    opts.dataSize = width;
    opts.startRow = offset;
    opts.columnFamily = colf;
    VerifyIngest.verifyIngest(connector, opts, scannerOpts);
  }
  
  public static String[] args(String... args) {
    return args;
  }
  
  @Test(timeout = 60 * 1000)
  public void multiTableTest() throws Exception {
    // Write to multiple tables
    String instance = cluster.getInstanceName();
    String keepers = cluster.getZooKeepers();
    TestMultiTableIngest.main(args("--count", "" + ROWS, "-u", "root", "-i", instance, "-z", keepers, "-p", PASSWORD));
    TestMultiTableIngest.main(args("--count", "" + ROWS, "--readonly", "-u", "root", "-i", instance, "-z", keepers, "-p", PASSWORD));
  }
  
  @Test(timeout = 60 * 1000)
  public void largeTest() throws Exception {
    // write a few large values
    Connector connector = getConnector();
    ingest(connector, 2, 1, 500000, 0);
    verify(connector, 2, 1, 500000, 0);
  }
  
  @Test(timeout = 60 * 1000)
  public void interleaved() throws Exception {
    // read and write concurrently
    final Connector connector = getConnector();
    interleaveTest(connector);
  }
  
  static void interleaveTest(final Connector connector) throws Exception {
    final AtomicBoolean fail = new AtomicBoolean(false);
    final int CHUNKSIZE = ROWS / 10;
    ingest(connector, CHUNKSIZE, 1, 50, 0);
    int i;
    for (i = 0; i < ROWS; i += CHUNKSIZE) {
      final int start = i;
      Thread verify = new Thread() {
        @Override
        public void run() {
          try {
            verify(connector, CHUNKSIZE, 1, 50, start);
          } catch (Exception ex) {
            fail.set(true);
          }
        }
      };
      ingest(connector, CHUNKSIZE, 1, 50, i + CHUNKSIZE);
      verify.join();
      assertFalse(fail.get());
    }
    verify(connector, CHUNKSIZE, 1, 50, i);
  }
  
  public static Text t(String s) {
    return new Text(s);
  }
  
  public static Mutation m(String row, String cf, String cq, String value) {
    Mutation m = new Mutation(t(row));
    m.put(t(cf), t(cq), new Value(value.getBytes()));
    return m;
  }
  
  @Test(timeout = 60 * 1000)
  public void localityGroupPerf() throws Exception {
    // verify that locality groups can make look-ups faster
    final Connector connector = getConnector();
    connector.tableOperations().create("test_ingest");
    connector.tableOperations().setProperty("test_ingest", "table.group.g1", "colf");
    connector.tableOperations().setProperty("test_ingest", "table.groups.enabled", "g1");
    ingest(connector, 2000, 1, 50, 0);
    connector.tableOperations().compact("test_ingest", null, null, true, true);
    BatchWriter bw = connector.createBatchWriter("test_ingest", new BatchWriterConfig());
    bw.addMutation(m("zzzzzzzzzzz", "colf2", "cq", "value"));
    bw.close();
    long now = System.currentTimeMillis();
    Scanner scanner = connector.createScanner("test_ingest", Authorizations.EMPTY);
    scanner.fetchColumnFamily(new Text("colf"));
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner)
      ;
    long diff = System.currentTimeMillis() - now;
    now = System.currentTimeMillis();
    scanner = connector.createScanner("test_ingest", Authorizations.EMPTY);
    scanner.fetchColumnFamily(new Text("colf2"));
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner)
      ;
    bw.close();
    long diff2 = System.currentTimeMillis() - now;
    assertTrue(diff2 < diff);
  }
  
  @Test(timeout = 60 * 1000)
  public void sunnyLG() throws Exception {
    // create a locality group, write to it and ensure it exists in the RFiles that result
    final Connector connector = getConnector();
    connector.tableOperations().create("test_ingest");
    Map<String,Set<Text>> groups = new TreeMap<String,Set<Text>>();
    groups.put("g1", Collections.singleton(t("colf")));
    connector.tableOperations().setLocalityGroups("test_ingest", groups);
    ingest(connector, 2000, 1, 50, 0);
    verify(connector, 2000, 1, 50, 0);
    connector.tableOperations().flush("test_ingest", null, null, true);
    BatchScanner bscanner = connector.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 1);
    String tableId = connector.tableOperations().tableIdMap().get("test_ingest");
    bscanner.setRanges(Collections.singletonList(new Range(new Text(tableId + ";"), new Text(tableId + "<"))));
    bscanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    boolean foundFile = false;
    for (Entry<Key,Value> entry : bscanner) {
      foundFile = true;
      Process info = cluster.exec(PrintInfo.class, entry.getKey().getColumnQualifier().toString());
      assertEquals(0, info.waitFor());
      String out = FunctionalTestUtils.readAll(cluster, PrintInfo.class, info);
      assertTrue(out.contains("Locality group         : g1"));
      assertTrue(out.contains("families      : [colf]"));
    }
    bscanner.close();
    assertTrue(foundFile);
  }
  
  @Test(timeout = 2* 60 * 1000)
  public void localityGroupChange() throws Exception {
    // Make changes to locality groups and ensure nothing is lostssh
    final Connector connector = getConnector();
    TableOperations to = connector.tableOperations();
    to.create("test_ingest");
    String[] config = new String[] {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf,xyz;lg2:c1,c2"};
    int i = 0;
    for (String cfg : config) {
      to.setLocalityGroups("test_ingest", getGroups(cfg));
      ingest(connector, ROWS * (i + 1), 1, 50, ROWS * i);
      to.flush("test_ingest", null, null, true);
      verify(connector, 0, 1, 50, ROWS * (i + 1));
      i++;
    }
    to.delete("test_ingest");
    to.create("test_ingest");
    config = new String[] {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf;lg2:colf",};
    i = 1;
    for (String cfg : config) {
      ingest(connector, ROWS * i, 1, 50, 0);
      ingest(connector, ROWS * i, 1, 50, 0, "xyz");
      to.setLocalityGroups("test_ingest", getGroups(cfg));
      to.flush("test_ingest", null, null, true);
      verify(connector, ROWS * i, 1, 50, 0);
      verify(connector, ROWS * i, 1, 50, 0, "xyz");
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
