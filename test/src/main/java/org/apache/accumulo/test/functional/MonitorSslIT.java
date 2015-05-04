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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Check SSL for the Monitor
 *
 */
public class MonitorSslIT extends ConfigurableMacBase {
  @BeforeClass
  public static void initHttps() throws NoSuchAlgorithmException, KeyManagementException {
    SSLContext ctx = SSLContext.getInstance("SSL");
    TrustManager[] tm = new TrustManager[] {new TestTrustManager()};
    ctx.init(new KeyManager[0], tm, new SecureRandom());
    SSLContext.setDefault(ctx);
    HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(new TestHostnameVerifier());
  }

  private static class TestTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}

    @Override
    public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }

  private static class TestHostnameVerifier implements HostnameVerifier {
    @Override
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 6 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);
    File baseDir = createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName());
    configureForSsl(cfg, getSslDir(baseDir));
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.MONITOR_SSL_KEYSTORE.getKey(), siteConfig.get(Property.RPC_SSL_KEYSTORE_PATH.getKey()));
    siteConfig.put(Property.MONITOR_SSL_KEYSTOREPASS.getKey(), siteConfig.get(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey()));
    if (siteConfig.containsKey(Property.RPC_SSL_KEYSTORE_TYPE.getKey())) {
      siteConfig.put(Property.MONITOR_SSL_KEYSTORETYPE.getKey(), siteConfig.get(Property.RPC_SSL_KEYSTORE_TYPE.getKey()));
    } else {
      siteConfig.put(Property.MONITOR_SSL_KEYSTORETYPE.getKey(), Property.RPC_SSL_KEYSTORE_TYPE.getDefaultValue());
    }
    siteConfig.put(Property.MONITOR_SSL_TRUSTSTORE.getKey(), siteConfig.get(Property.RPC_SSL_TRUSTSTORE_PATH.getKey()));
    siteConfig.put(Property.MONITOR_SSL_TRUSTSTOREPASS.getKey(), siteConfig.get(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey()));
    if (siteConfig.containsKey(Property.RPC_SSL_TRUSTSTORE_TYPE.getKey())) {
      siteConfig.put(Property.MONITOR_SSL_TRUSTSTORETYPE.getKey(), siteConfig.get(Property.RPC_SSL_TRUSTSTORE_TYPE.getKey()));
    } else {
      siteConfig.put(Property.MONITOR_SSL_TRUSTSTORETYPE.getKey(), Property.RPC_SSL_TRUSTSTORE_TYPE.getDefaultValue());
    }
    cfg.setSiteConfig(siteConfig);
  }

  @Test
  public void test() throws Exception {
    log.debug("Starting Monitor");
    cluster.getClusterControl().startAllServers(ServerType.MONITOR);
    String monitorLocation = null;
    while (null == monitorLocation) {
      try {
        monitorLocation = MonitorUtil.getLocation(getConnector().getInstance());
      } catch (Exception e) {
        // ignored
      }
      if (null == monitorLocation) {
        log.debug("Could not fetch monitor HTTP address from zookeeper");
        Thread.sleep(2000);
      }
    }
    URL url = new URL("https://" + monitorLocation);
    log.debug("Fetching web page {}", url);
    String result = FunctionalTestUtils.readAll(url.openStream());
    assertTrue(result.length() > 100);
    assertTrue(result.indexOf("Accumulo Overview") >= 0);
  }

}
