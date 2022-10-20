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

import static org.apache.accumulo.harness.AccumuloITBase.STANDALONE_CAPABLE_CLUSTER;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.standalone.StandaloneAccumuloCluster;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.file.rfile.PrintInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.TestMultiTableIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Tag(STANDALONE_CAPABLE_CLUSTER)
@Tag(SUNNY_DAY)
public class ReadWriteIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(6);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  private static final Logger log = LoggerFactory.getLogger(ReadWriteIT.class);

  static final int ROWS = 100000;
  static final int COLS = 1;
  static final String COLF = "colf";

  @Test
  public void invalidInstanceName() {
    try (var client = Accumulo.newClient().to("fake_instance_name", cluster.getZooKeepers())
        .as(getAdminPrincipal(), getAdminToken()).build()) {
      assertThrows(RuntimeException.class, () -> client.instanceOperations().getTabletServers());
    }
  }

  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "URLCONNECTION_SSRF_FD"},
      justification = "path provided by test; url provided by test")
  @Test
  public void sunnyDay() throws Exception {
    // Start accumulo, create a table, insert some data, verify we can read it out.
    // Shutdown cleanly.
    log.debug("Starting Monitor");
    cluster.getClusterControl().startAllServers(ServerType.MONITOR);
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      ingest(accumuloClient, ROWS, COLS, 50, 0, tableName);
      verify(accumuloClient, ROWS, COLS, 50, 0, tableName);
      String monitorLocation = null;
      while (monitorLocation == null) {
        monitorLocation = MonitorUtil.getLocation((ClientContext) accumuloClient);
        if (monitorLocation == null) {
          log.debug("Could not fetch monitor HTTP address from zookeeper");
          Thread.sleep(2000);
        }
      }
      if (getCluster() instanceof StandaloneAccumuloCluster) {
        String monitorSslKeystore =
            getCluster().getSiteConfiguration().get(Property.MONITOR_SSL_KEYSTORE);
        if (monitorSslKeystore != null && !monitorSslKeystore.isEmpty()) {
          log.info(
              "Using HTTPS since monitor ssl keystore configuration was observed in accumulo configuration");
          SSLContext ctx = SSLContext.getInstance("TLSv1.2");
          TrustManager[] tm = {new TestTrustManager()};
          ctx.init(new KeyManager[0], tm, random);
          SSLContext.setDefault(ctx);
          HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
          HttpsURLConnection.setDefaultHostnameVerifier(new TestHostnameVerifier());
        }
      }
      URL url = new URL(monitorLocation);
      log.debug("Fetching web page {}", url);
      String result = FunctionalTestUtils.readWebPage(url).body();
      assertTrue(result.length() > 100);
      log.debug("Stopping accumulo cluster");
      ClusterControl control = cluster.getClusterControl();
      control.adminStopAll();
      ZooCache zcache = cluster.getServerContext().getZooCache();
      var zLockPath =
          ServiceLock.path(ZooUtil.getRoot(accumuloClient.instanceOperations().getInstanceId())
              + Constants.ZMANAGER_LOCK);
      byte[] managerLockData;
      do {
        managerLockData = ServiceLock.getLockData(zcache, zLockPath, null);
        if (managerLockData != null) {
          log.info("Manager lock is still held");
          Thread.sleep(1000);
        }
      } while (managerLockData != null);
      control.stopAllServers(ServerType.MANAGER);
      control.stopAllServers(ServerType.TABLET_SERVER);
      control.stopAllServers(ServerType.GARBAGE_COLLECTOR);
      control.stopAllServers(ServerType.MONITOR);
      log.debug("success!");
      // Restarting everything
      cluster.start();
    }
  }

  public static void ingest(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String tableName) throws Exception {
    ingest(accumuloClient, rows, cols, width, offset, COLF, tableName);
  }

  public static void ingest(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String colf, String tableName) throws Exception {
    IngestParams params = new IngestParams(accumuloClient.properties(), tableName, rows);
    params.cols = cols;
    params.dataSize = width;
    params.startRow = offset;
    params.columnFamily = colf;
    params.createTable = true;
    TestIngest.ingest(accumuloClient, params);
  }

  public static void verify(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String tableName) throws Exception {
    verify(accumuloClient, rows, cols, width, offset, COLF, tableName);
  }

  private static void verify(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String colf, String tableName) throws Exception {
    VerifyParams params = new VerifyParams(accumuloClient.properties(), tableName, rows);
    params.rows = rows;
    params.dataSize = width;
    params.startRow = offset;
    params.columnFamily = colf;
    params.cols = cols;
    VerifyIngest.verifyIngest(accumuloClient, params);
  }

  public static String[] args(String... args) {
    return args;
  }

  @Test
  public void multiTableTest() throws Exception {
    // Write to multiple tables
    final ClusterControl control = cluster.getClusterControl();
    final String prefix = getClass().getSimpleName() + "_" + testName();
    ExecutorService svc = Executors.newFixedThreadPool(2);
    Future<Integer> p1 = svc.submit(() -> {
      try {
        return control.exec(TestMultiTableIngest.class, args("--count", Integer.toString(ROWS),
            "-c", cluster.getClientPropsPath(), "--tablePrefix", prefix));
      } catch (IOException e) {
        log.error("Error running MultiTableIngest", e);
        return -1;
      }
    });
    Future<Integer> p2 = svc.submit(() -> {
      try {
        return control.exec(TestMultiTableIngest.class, args("--count", Integer.toString(ROWS),
            "--readonly", "-c", cluster.getClientPropsPath(), "--tablePrefix", prefix));
      } catch (IOException e) {
        log.error("Error running MultiTableIngest", e);
        return -1;
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
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];
      ingest(accumuloClient, 2, 1, 500000, 0, table);
      verify(accumuloClient, 2, 1, 500000, 0, table);
    }
  }

  @Test
  public void interleaved() throws Exception {
    // read and write concurrently
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      interleaveTest(accumuloClient, tableName);
    }
  }

  static void interleaveTest(final AccumuloClient accumuloClient, final String tableName)
      throws Exception {
    final AtomicBoolean fail = new AtomicBoolean(false);
    final int CHUNKSIZE = ROWS / 10;
    ingest(accumuloClient, CHUNKSIZE, 1, 50, 0, tableName);
    int i;
    for (i = 0; i < ROWS; i += CHUNKSIZE) {
      final int start = i;
      Thread verify = new Thread(() -> {
        try {
          verify(accumuloClient, CHUNKSIZE, 1, 50, start, tableName);
        } catch (Exception ex) {
          fail.set(true);
        }
      });
      verify.start();
      ingest(accumuloClient, CHUNKSIZE, 1, 50, i + CHUNKSIZE, tableName);
      verify.join();
      assertFalse(fail.get());
    }
    verify(accumuloClient, CHUNKSIZE, 1, 50, i, tableName);
  }

  public static Text t(String s) {
    return new Text(s);
  }

  public static Mutation m(String row, String cf, String cq, String value) {
    Mutation m = new Mutation(t(row));
    m.put(t(cf), t(cq), new Value(value));
    return m;
  }

  @Test
  public void localityGroupPerf() throws Exception {
    // verify that locality groups can make look-ups faster
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      accumuloClient.tableOperations().create(tableName);
      accumuloClient.tableOperations().setProperty(tableName, "table.group.g1", "colf");
      accumuloClient.tableOperations().setProperty(tableName, "table.groups.enabled", "g1");
      ingest(accumuloClient, 2000, 1, 50, 0, tableName);
      accumuloClient.tableOperations().compact(tableName, null, null, true, true);
      try (BatchWriter bw = accumuloClient.createBatchWriter(tableName)) {
        bw.addMutation(m("zzzzzzzzzzz", "colf2", "cq", "value"));
      }
      long now = System.currentTimeMillis();
      try (Scanner scanner = accumuloClient.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.fetchColumnFamily(new Text("colf"));
        scanner.forEach((k, v) -> {});
      }
      long diff = System.currentTimeMillis() - now;
      now = System.currentTimeMillis();

      try (Scanner scanner = accumuloClient.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.fetchColumnFamily(new Text("colf2"));
        scanner.forEach((k, v) -> {});
      }
      long diff2 = System.currentTimeMillis() - now;
      assertTrue(diff2 < diff);
    }
  }

  /**
   * create a locality group, write to it and ensure it exists in the RFiles that result
   */
  @Test
  public void sunnyLG() throws Exception {
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      accumuloClient.tableOperations().create(tableName);
      Map<String,Set<Text>> groups = new TreeMap<>();
      groups.put("g1", Collections.singleton(t("colf")));
      accumuloClient.tableOperations().setLocalityGroups(tableName, groups);
      verifyLocalityGroupsInRFile(accumuloClient, tableName);
    }
  }

  /**
   * Pretty much identical to sunnyLG, but verifies locality groups are created when configured in
   * NewTableConfiguration prior to table creation.
   */
  @Test
  public void sunnyLGUsingNewTableConfiguration() throws Exception {
    // create a locality group, write to it and ensure it exists in the RFiles that result
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      Map<String,Set<Text>> groups = new HashMap<>();
      groups.put("g1", Collections.singleton(t("colf")));
      ntc.setLocalityGroups(groups);
      accumuloClient.tableOperations().create(tableName, ntc);
      verifyLocalityGroupsInRFile(accumuloClient, tableName);
    }
  }

  private void verifyLocalityGroupsInRFile(final AccumuloClient accumuloClient,
      final String tableName) throws Exception {
    ingest(accumuloClient, 2000, 1, 50, 0, tableName);
    verify(accumuloClient, 2000, 1, 50, 0, tableName);
    accumuloClient.tableOperations().flush(tableName, null, null, true);
    try (BatchScanner bscanner =
        accumuloClient.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 1)) {
      String tableId = accumuloClient.tableOperations().tableIdMap().get(tableName);
      bscanner.setRanges(
          Collections.singletonList(new Range(new Text(tableId + ";"), new Text(tableId + "<"))));
      bscanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      boolean foundFile = false;
      for (Entry<Key,Value> entry : bscanner) {
        foundFile = true;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream oldOut = System.out;
        try (PrintStream newOut = new PrintStream(baos)) {
          System.setOut(newOut);
          List<String> args = new ArrayList<>();
          args.add(entry.getKey().getColumnQualifier().toString());
          args.add("--props");
          args.add(getCluster().getAccumuloPropertiesPath());
          if (getClusterType() == ClusterType.STANDALONE && saslEnabled()) {
            args.add("--config");
            StandaloneAccumuloCluster sac = (StandaloneAccumuloCluster) cluster;
            String hadoopConfDir = sac.getHadoopConfDir();
            args.add(new Path(hadoopConfDir, "core-site.xml").toString());
            args.add(new Path(hadoopConfDir, "hdfs-site.xml").toString());
          }
          log.info("Invoking PrintInfo with {}", args);
          PrintInfo.main(args.toArray(new String[args.size()]));
          newOut.flush();
          String stdout = baos.toString();
          assertTrue(stdout.contains("Locality group           : g1"));
          assertTrue(stdout.contains("families        : [colf]"));
        } finally {
          System.setOut(oldOut);
        }
      }
      assertTrue(foundFile);
    }
  }

  @Test
  public void localityGroupChange() throws Exception {
    // Make changes to locality groups and ensure nothing is lost
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];
      TableOperations to = accumuloClient.tableOperations();
      to.create(table);
      String[] config = {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf,xyz;lg2:c1,c2"};
      int i = 0;
      for (String cfg : config) {
        to.setLocalityGroups(table, getGroups(cfg));
        ingest(accumuloClient, ROWS * (i + 1), 1, 50, ROWS * i, table);
        to.flush(table, null, null, true);
        verify(accumuloClient, 0, 1, 50, ROWS * (i + 1), table);
        i++;
      }
      to.delete(table);
      to.create(table);
      config = new String[] {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf;lg2:colf",};
      i = 1;
      for (String cfg : config) {
        ingest(accumuloClient, ROWS * i, 1, 50, 0, table);
        ingest(accumuloClient, ROWS * i, 1, 50, 0, "xyz", table);
        to.setLocalityGroups(table, getGroups(cfg));
        to.flush(table, null, null, true);
        verify(accumuloClient, ROWS * i, 1, 50, 0, table);
        verify(accumuloClient, ROWS * i, 1, 50, 0, "xyz", table);
        i++;
      }
    }
  }

  private Map<String,Set<Text>> getGroups(String cfg) {
    Map<String,Set<Text>> groups = new TreeMap<>();
    if (cfg != null) {
      for (String group : cfg.split(";")) {
        String[] parts = group.split(":");
        Set<Text> cols = new HashSet<>();
        for (String col : parts[1].split(",")) {
          cols.add(t(col));
        }
        groups.put(parts[1], cols);
      }
    }
    return groups;
  }

  @SuppressFBWarnings(value = "WEAK_TRUST_MANAGER",
      justification = "trust manager is okay for testing")
  private static class TestTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] arg0, String arg1) {}

    @Override
    public void checkServerTrusted(X509Certificate[] arg0, String arg1) {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }

  @SuppressFBWarnings(value = "WEAK_HOSTNAME_VERIFIER", justification = "okay for test")
  private static class TestHostnameVerifier implements HostnameVerifier {
    @Override
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

}
