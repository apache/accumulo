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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ZooKeeperBindException;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.CertUtils;
import org.apache.accumulo.tserver.memory.NativeMapLoader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * General Integration-Test base class that provides access to a {@link MiniAccumuloCluster} for
 * testing. Tests using these typically do very disruptive things to the instance, and require
 * specific configuration. Most tests don't need this level of control and should extend
 * {@link AccumuloClusterHarness} instead.
 */
@Tag(MINI_CLUSTER_ONLY)
public class ConfigurableMacBase extends AccumuloITBase {
  public static final Logger log = LoggerFactory.getLogger(ConfigurableMacBase.class);

  protected MiniAccumuloClusterImpl cluster;

  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {}

  protected void beforeClusterStart(MiniAccumuloConfigImpl cfg) {}

  protected static final String ROOT_PASSWORD = "testRootPassword1";

  public static void configureForEnvironment(MiniAccumuloConfigImpl cfg, File folder) {
    if ("true".equals(System.getProperty("org.apache.accumulo.test.functional.useSslForIT"))) {
      configureForSsl(cfg, folder);
    }
    if ("true"
        .equals(System.getProperty("org.apache.accumulo.test.functional.useCredProviderForIT"))) {
      cfg.setUseCredentialProvider(true);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
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
    final String rootKeystorePassword = "root_keystore_password",
        truststorePassword = "truststore_password";
    try {
      String hostname = InetAddress.getLocalHost().getHostName();
      new CertUtils(Property.RPC_SSL_KEYSTORE_TYPE.getDefaultValue(),
          "o=Apache Accumulo,cn=" + hostname, "RSA", 4096, "SHA512WITHRSA").createAll(
              rootKeystoreFile, localKeystoreFile, publicTruststoreFile, cfg.getInstanceName(),
              rootKeystorePassword, cfg.getRootPassword(), truststorePassword);
    } catch (Exception e) {
      throw new RuntimeException("error creating MAC keystore", e);
    }

    siteConfig.put(Property.INSTANCE_RPC_SSL_ENABLED.getKey(), "true");
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), localKeystoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey(), cfg.getRootPassword());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PATH.getKey(),
        publicTruststoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
    cfg.setSiteConfig(siteConfig);

    Map<String,String> clientProps = cfg.getClientProps();
    clientProps.put(ClientProperty.SSL_ENABLED.getKey(), "true");
    clientProps.put(ClientProperty.SSL_KEYSTORE_PATH.getKey(), localKeystoreFile.getAbsolutePath());
    clientProps.put(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey(), cfg.getRootPassword());
    clientProps.put(ClientProperty.SSL_TRUSTSTORE_PATH.getKey(),
        publicTruststoreFile.getAbsolutePath());
    clientProps.put(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
    cfg.setClientProps(clientProps);
  }

  @BeforeEach
  public void setUp() throws Exception {
    createMiniAccumulo();
    Exception lastException = null;
    for (int i = 0; i < 3; i++) {
      try {
        cluster.start();
        return;
      } catch (ZooKeeperBindException e) {
        lastException = e;
        log.warn("Failed to start MiniAccumuloCluster, assumably due to ZooKeeper issues",
            lastException);
        Thread.sleep(3000);
        createMiniAccumulo();
      }
    }
    throw new RuntimeException("Failed to start MiniAccumuloCluster after three attempts",
        lastException);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  private void createMiniAccumulo() throws Exception {
    // createTestDir will give us a empty directory, we don't need to clean it up ourselves
    File baseDir = createTestDir(this.getClass().getName() + "_" + this.testName());
    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(baseDir, ROOT_PASSWORD);
    File nativePathInDevTree = NativeMapIT.nativeMapLocation();
    File nativePathInMapReduce = new File(System.getProperty("user.dir"));
    cfg.setNativeLibPaths(nativePathInDevTree.getAbsolutePath(), nativePathInMapReduce.toString());
    Configuration coreSite = new Configuration(false);
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.TRUE.toString());
    configure(cfg, coreSite);
    configureForEnvironment(cfg, getSslDir(baseDir));
    if (Boolean.parseBoolean(cfg.getSiteConfig().get(Property.TSERV_NATIVEMAP_ENABLED.getKey()))) {
      NativeMapLoader.loadForTest(List.of(nativePathInDevTree, nativePathInMapReduce), () -> {
        throw new IllegalStateException("Native maps were configured, but not available");
      });
    }
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

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      try {
        cluster.stop();
      } catch (Exception e) {
        // ignored
      }
    }
  }

  protected MiniAccumuloClusterImpl getCluster() {
    return cluster;
  }

  protected Properties getClientProperties() {
    return cluster.getClientProperties();
  }

  protected ServerContext getServerContext() {
    return getCluster().getServerContext();
  }

  protected Process exec(Class<?> clazz, String... args) throws IOException {
    return getCluster().exec(clazz, args).getProcess();
  }
}
