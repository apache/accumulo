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

import java.io.File;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.CertUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class AbstractMacIT {
  public static final Logger log = Logger.getLogger(AbstractMacIT.class);

  public static final String ROOT_PASSWORD = "testRootPassword1";
  public static final ScannerOpts SOPTS = new ScannerOpts();
  public static final BatchWriterOpts BWOPTS = new BatchWriterOpts();

  @Rule
  public TestName testName = new TestName();

  protected static void cleanUp(MiniAccumuloClusterImpl cluster) {
    if (cluster != null)
      try {
        cluster.stop();
      } catch (Exception e) {}
  }

  protected static File createSharedTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    if (name != null)
      baseDir = new File(baseDir, name);
    File testDir = new File(baseDir, System.currentTimeMillis() + "_" + new Random().nextInt(Short.MAX_VALUE));
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    return testDir;
  }

  protected static File createTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    if (name == null)
      return baseDir;
    File testDir = new File(baseDir, name);
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    return testDir;
  }

  public String[] getUniqueNames(int num) {
    String[] names = new String[num];
    for (int i = 0; i < num; i++)
      names[i] = this.getClass().getSimpleName() + "_" + testName.getMethodName() + i;
    return names;
  }

  protected static void configureForEnvironment(MiniAccumuloConfigImpl cfg, Class<?> testClass, File folder) {
    if ("true".equals(System.getProperty("org.apache.accumulo.test.functional.useSslForIT"))) {
      configureForSsl(cfg, folder);
    }
  }

  protected static void configureForSsl(MiniAccumuloConfigImpl cfg, File folder) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    if ("true".equals(siteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      // already enabled; don't mess with it
      return;
    }

    File sslDir = new File(folder, "ssl");
    sslDir.mkdirs();
    File rootKeystoreFile = new File(sslDir, "root-" + cfg.getInstanceName() + ".jks");
    File localKeystoreFile = new File(sslDir, "local-" + cfg.getInstanceName() + ".jks");
    File publicTruststoreFile = new File(sslDir, "public-" + cfg.getInstanceName() + ".jks");
    try {
      new CertUtils(Property.RPC_SSL_KEYSTORE_TYPE.getDefaultValue(), "o=Apache Accumulo,cn=MiniAccumuloCluster", "RSA", 2048, "sha1WithRSAEncryption")
          .createAll(rootKeystoreFile, localKeystoreFile, publicTruststoreFile, cfg.getInstanceName(), cfg.getRootPassword());
    } catch (Exception e) {
      throw new RuntimeException("error creating MAC keystore", e);
    }

    siteConfig.put(Property.INSTANCE_RPC_SSL_ENABLED.getKey(), "true");
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), localKeystoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey(), cfg.getRootPassword());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PATH.getKey(), publicTruststoreFile.getAbsolutePath());
    cfg.setSiteConfig(siteConfig);
  }

  public abstract Connector getConnector() throws AccumuloException, AccumuloSecurityException;

  public abstract String rootPath();
}
