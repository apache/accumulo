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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Check SSL for the Monitor
 */
public class MonitorSslIT extends AccumuloClusterHarness {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorSslIT.class);
  private static SSLContext ctx;

  @BeforeAll
  public static void initHttps() throws NoSuchAlgorithmException, KeyManagementException {
    ctx = SSLContext.getInstance("TLSv1.3");
    TrustManager[] tm = {new TestTrustManager()};
    ctx.init(new KeyManager[0], tm, RANDOM.get());
    SSLContext.setDefault(ctx);
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

  private static boolean sslEnabled = false;

  @AfterEach
  public void after() throws Exception {
    cluster.getClusterControl().stopAllServers(ServerType.MONITOR);
    sslEnabled = !sslEnabled;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    LOG.info("*** Configuring server, SSL Enabled: {}", sslEnabled);
    if (sslEnabled) {
      File baseDir = createTestDir(this.getClass().getName() + "_" + this.testName());
      ConfigurableMacBase.configureForSsl(cfg, getSslDir(baseDir));
      Map<String,String> siteConfig = cfg.getSiteConfig();
      siteConfig.put(Property.MONITOR_SSL_KEYSTORE.getKey(),
          siteConfig.get(Property.RPC_SSL_KEYSTORE_PATH.getKey()));
      siteConfig.put(Property.MONITOR_SSL_KEYSTOREPASS.getKey(),
          siteConfig.get(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey()));
      if (siteConfig.containsKey(Property.RPC_SSL_KEYSTORE_TYPE.getKey())) {
        siteConfig.put(Property.MONITOR_SSL_KEYSTORETYPE.getKey(),
            siteConfig.get(Property.RPC_SSL_KEYSTORE_TYPE.getKey()));
      } else {
        siteConfig.put(Property.MONITOR_SSL_KEYSTORETYPE.getKey(),
            Property.RPC_SSL_KEYSTORE_TYPE.getDefaultValue());
      }
      siteConfig.put(Property.MONITOR_SSL_TRUSTSTORE.getKey(),
          siteConfig.get(Property.RPC_SSL_TRUSTSTORE_PATH.getKey()));
      siteConfig.put(Property.MONITOR_SSL_TRUSTSTOREPASS.getKey(),
          siteConfig.get(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey()));
      if (siteConfig.containsKey(Property.RPC_SSL_TRUSTSTORE_TYPE.getKey())) {
        siteConfig.put(Property.MONITOR_SSL_TRUSTSTORETYPE.getKey(),
            siteConfig.get(Property.RPC_SSL_TRUSTSTORE_TYPE.getKey()));
      } else {
        siteConfig.put(Property.MONITOR_SSL_TRUSTSTORETYPE.getKey(),
            Property.RPC_SSL_TRUSTSTORE_TYPE.getDefaultValue());
      }
      cfg.setSiteConfig(siteConfig);
    }
  }

  @Test
  public void test1() throws Exception {
    test();
  }

  @Test
  public void test2() throws Exception {
    test();
  }

  public void test() throws Exception {
    LOG.info("*** Running test, SSL Enabled: {}", sslEnabled);

    LOG.debug("Starting Monitor");
    cluster.getClusterControl().startAllServers(ServerType.MONITOR);
    String monitorLocation = null;
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      while (monitorLocation == null) {
        try {
          monitorLocation = MonitorUtil.getLocation((ClientContext) client);
        } catch (Exception e) {
          // ignored
        }
        if (monitorLocation == null) {
          LOG.debug("Could not fetch monitor HTTP address from zookeeper");
          Thread.sleep(2000);
        }
      }
    }

    SslContextFactory.Client sslContextFactory = null;
    if (sslEnabled) {
      sslContextFactory = new SslContextFactory.Client();
      sslContextFactory.setSslContext(ctx);
      // Don't validate server host name
      sslContextFactory.setEndpointIdentificationAlgorithm(null);
    }

    HttpClient h1 = createHttp11(sslContextFactory);
    h1.start();
    ContentResponse body = h1.GET(monitorLocation);
    assertTrue(body.getContentAsString().length() > 100);
    assertTrue(body.getContentAsString().indexOf("Accumulo Overview") >= 0);
    h1.stop();

    HttpClient h2 = createHttp2(sslContextFactory);
    h2.start();
    body = h2.GET(monitorLocation);
    assertTrue(body.getContentAsString().length() > 100);
    assertTrue(body.getContentAsString().indexOf("Accumulo Overview") >= 0);
    h2.stop();

  }

  private HttpClient createHttp11(SslContextFactory.Client sslContextFactory) throws Exception {

    ClientConnector connector = new ClientConnector();
    if (sslContextFactory != null) {
      connector.setSslContextFactory(sslContextFactory);
    }

    HttpClientTransportOverHTTP transport = new HttpClientTransportOverHTTP(connector);

    return new HttpClient(transport);
  }

  private HttpClient createHttp2(SslContextFactory.Client sslContextFactory) throws Exception {

    ClientConnector connector = new ClientConnector();
    if (sslContextFactory != null) {
      connector.setSslContextFactory(sslContextFactory);
    }

    HTTP2Client h2Client = new HTTP2Client(connector);
    h2Client.setInitialSessionRecvWindow(64 * 1024 * 1024);
    h2Client.setMaxFrameSize(14 * 1024 * 1024);

    // Create and configure the HTTP/2 transport.
    HttpClientTransportOverHTTP2 transport = new HttpClientTransportOverHTTP2(h2Client);

    return new HttpClient(transport);
  }

}
