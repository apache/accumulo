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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ZooKeeperBindException;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.util.CertUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General Integration-Test base class that provides access to a {@link MiniAccumuloCluster} for testing. Tests using these typically do very disruptive things
 * to the instance, and require specific configuration. Most tests don't need this level of control and should extend {@link AccumuloClusterHarness} instead.
 */
@Category(MiniClusterOnlyTests.class)
public class ConfigurableMacBase extends AccumuloITBase {
  public static final Logger log = LoggerFactory.getLogger(ConfigurableMacBase.class);

  protected MiniAccumuloClusterImpl cluster;

  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {}

  protected void beforeClusterStart(MiniAccumuloConfigImpl cfg) throws Exception {}

  protected static final String ROOT_PASSWORD = "testRootPassword1";

  public static void configureForEnvironment(MiniAccumuloConfigImpl cfg, Class<?> testClass, File folder) {
    if ("true".equals(System.getProperty("org.apache.accumulo.test.functional.useSslForIT"))) {
      configureForSsl(cfg, folder);
    }
    if ("true".equals(System.getProperty("org.apache.accumulo.test.functional.useCredProviderForIT"))) {
      cfg.setUseCredentialProvider(true);
    }
  }

  protected static void configureForSsl(MiniAccumuloConfigImpl cfg, File sslDir) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    if ("true".equals(siteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      // already enabled; don't mess with it
      return;
    }

    // create parent directories, and ensure sslDir is empty
    assertTrue(sslDir.mkdirs() || sslDir.isDirectory());
    FileUtils.deleteQuietly(sslDir);
    assertTrue(sslDir.mkdir());

    File rootKeystoreFile = new File(sslDir, "root-" + cfg.getInstanceName() + ".jks");
    File localKeystoreFile = new File(sslDir, "local-" + cfg.getInstanceName() + ".jks");
    File publicTruststoreFile = new File(sslDir, "public-" + cfg.getInstanceName() + ".jks");
    final String rootKeystorePassword = "root_keystore_password", truststorePassword = "truststore_password";
    try {
      new CertUtils(Property.RPC_SSL_KEYSTORE_TYPE.getDefaultValue(), "o=Apache Accumulo,cn=MiniAccumuloCluster", "RSA", 2048, "sha1WithRSAEncryption")
          .createAll(rootKeystoreFile, localKeystoreFile, publicTruststoreFile, cfg.getInstanceName(), rootKeystorePassword, cfg.getRootPassword(),
              truststorePassword);
    } catch (Exception e) {
      throw new RuntimeException("error creating MAC keystore", e);
    }

    siteConfig.put(Property.INSTANCE_RPC_SSL_ENABLED.getKey(), "true");
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), localKeystoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey(), cfg.getRootPassword());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PATH.getKey(), publicTruststoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
    cfg.setSiteConfig(siteConfig);
  }

  @Before
  public void setUp() throws Exception {
    createMiniAccumulo();
    Exception lastException = null;
    for (int i = 0; i < 3; i++) {
      try {
        cluster.start();
        return;
      } catch (ZooKeeperBindException e) {
        lastException = e;
        log.warn("Failed to start MiniAccumuloCluster, assumably due to ZooKeeper issues", lastException);
        Thread.sleep(3000);
        createMiniAccumulo();
      }
    }
    throw new RuntimeException("Failed to start MiniAccumuloCluster after three attempts", lastException);
  }

  private void createMiniAccumulo() throws Exception {
    // createTestDir will give us a empty directory, we don't need to clean it up ourselves
    File baseDir = createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName());
    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(baseDir, ROOT_PASSWORD);
    String nativePathInDevTree = NativeMapIT.nativeMapLocation().getAbsolutePath();
    String nativePathInMapReduce = new File(System.getProperty("user.dir")).toString();
    cfg.setNativeLibPaths(nativePathInDevTree, nativePathInMapReduce);
    cfg.setProperty(Property.GC_FILE_ARCHIVE, Boolean.TRUE.toString());
    Configuration coreSite = new Configuration(false);
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.TRUE.toString());
    configure(cfg, coreSite);
    configureForEnvironment(cfg, getClass(), getSslDir(baseDir));
    cluster = new MiniAccumuloClusterImpl(cfg);
    if (coreSite.size() > 0) {
      File csFile = new File(cluster.getConfig().getConfDir(), "core-site.xml");
      if (csFile.exists()) {
        coreSite.addResource(new Path(csFile.getAbsolutePath()));
      }
      File tmp = new File(csFile.getAbsolutePath() + ".tmp");
      OutputStream out = new BufferedOutputStream(new FileOutputStream(tmp));
      coreSite.writeXml(out);
      out.close();
      assertTrue(tmp.renameTo(csFile));
    }
    beforeClusterStart(cfg);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      try {
        cluster.stop();
      } catch (Exception e) {
        // ignored
      }
  }

  protected MiniAccumuloClusterImpl getCluster() {
    return cluster;
  }

  protected Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getCluster().getConnector("root", new PasswordToken(ROOT_PASSWORD));
  }

  protected Process exec(Class<?> clazz, String... args) throws IOException {
    return getCluster().exec(clazz, args);
  }

  protected String getMonitor() throws KeeperException, InterruptedException {
    Instance instance = new ZooKeeperInstance(getCluster().getClientConfig());
    return MonitorUtil.getLocation(instance);
  }

  protected ClientConfiguration getClientConfig() throws Exception {
    return new ClientConfiguration(getCluster().getConfig().getClientConfFile());
  }

}
