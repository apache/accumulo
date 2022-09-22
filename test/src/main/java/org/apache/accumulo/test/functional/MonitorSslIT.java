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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Check SSL for the Monitor
 */
public class MonitorSslIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(6);
  }

  @BeforeAll
  public static void initHttps() throws NoSuchAlgorithmException, KeyManagementException {
    SSLContext ctx = SSLContext.getInstance("TLSv1.2");
    TrustManager[] tm = {new TestTrustManager()};
    ctx.init(new KeyManager[0], tm, random);
    SSLContext.setDefault(ctx);
    HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(new TestHostnameVerifier());
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

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);
    File baseDir = createTestDir(this.getClass().getName() + "_" + this.testName());
    configureForSsl(cfg, getSslDir(baseDir));
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

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD", justification = "url provided by test")
  @Test
  public void test() throws Exception {
    log.debug("Starting Monitor");
    cluster.getClusterControl().startAllServers(ServerType.MONITOR);
    String monitorLocation = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      while (monitorLocation == null) {
        try {
          monitorLocation = MonitorUtil.getLocation((ClientContext) client);
        } catch (Exception e) {
          // ignored
        }
        if (monitorLocation == null) {
          log.debug("Could not fetch monitor HTTP address from zookeeper");
          Thread.sleep(2000);
        }
      }
    }
    URL url = new URL(monitorLocation);
    log.debug("Fetching web page {}", url);
    String result = FunctionalTestUtils.readWebPage(url).body();
    assertTrue(result.length() > 100);
    assertTrue(result.indexOf("Accumulo Overview") >= 0);
  }

}
