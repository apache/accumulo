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

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.apache.accumulo.harness.AccumuloITBase.STANDALONE_CAPABLE_CLUSTER;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Optional;
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
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
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
          var ctx = SSLContext.getInstance(Property.RPC_SSL_CLIENT_PROTOCOL.getDefaultValue());
          TrustManager[] tm = {new TestTrustManager()};
          ctx.init(new KeyManager[0], tm, RANDOM.get());
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
      var zLockPath = getServerContext().getServerPaths().createManagerPath();
      Optional<ServiceLockData> managerLockData;
      do {
        managerLockData = ServiceLock.getLockData(zcache, zLockPath, null);
        if (managerLockData.isPresent()) {
          log.info("Manager lock is still held");
          Thread.sleep(1000);
        }
      } while (managerLockData.isPresent());
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
    ingest(accumuloClient, rows, cols, width, offset, colf, tableName, 0);
  }

  public static void ingest(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String colf, String tableName, int flushAfterRows) throws Exception {
    IngestParams params = new IngestParams(accumuloClient.properties(), tableName, rows);
    params.cols = cols;
    params.dataSize = width;
    params.startRow = offset;
    params.columnFamily = colf;
    params.createTable = true;
    params.flushAfterRows = flushAfterRows;
    TestIngest.ingest(accumuloClient, params);
  }

  public static void verify(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String tableName) throws Exception {
    verify(accumuloClient, rows, cols, width, offset, COLF, tableName);
  }

  public static void verify(AccumuloClient accumuloClient, int rows, int cols, int width,
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
