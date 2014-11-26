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
import java.io.FileNotFoundException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.harness.AccumuloIT;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ZooKeeperBindException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * An implementation of {@link AbstractMacIT} for test cases that do not need to know any special details of {@link MiniAccumuloCluster}. Tests which extend
 * this class should be runnable on any instance of Accumulo, given a root connector.
 *
 * New tests should be written using {@link AccumuloClusterIT}. To obtain the same semantics (shared instance across methods in a class), extend
 * {@link AccumuloIT} and use {@link MiniClusterHarness} to create a MiniAccumuloCluster properly configured for the environment.
 */
@Deprecated
public class SimpleMacIT extends AbstractMacIT {
  protected static final Logger log = Logger.getLogger(SimpleMacIT.class);

  private static final String INSTANCE_NAME = "instance1";

  private static File folder;
  private static MiniAccumuloClusterImpl cluster = null;

  /**
   * Try to get a common instance to run against. Fall back on creating a MiniAccumuloCluster. Subclasses should not care what kind of instance they get, as
   * they should only use the API, given a root connector.
   */
  @BeforeClass
  public static synchronized void setUp() throws Exception {
    if (getInstanceOneConnector() == null && cluster == null) {
      log.info("No shared instance available, falling back to creating MAC");
      createMiniAccumulo();
      Exception lastException = null;
      for (int i = 0; i < 3; i++) {
        try {
          cluster.start();

          Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
              cleanUp(cluster);
            }
          });

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
  }

  private static void createMiniAccumulo() throws Exception {
    folder = createSharedTestDir(SimpleMacIT.class.getName());
    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(folder, ROOT_PASSWORD);
    cfg.setNativeLibPaths(NativeMapIT.nativeMapLocation().getAbsolutePath());
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.TRUE.toString());
    cfg.setProperty(Property.GC_FILE_ARCHIVE, Boolean.TRUE.toString());
    configureForEnvironment(cfg, SimpleMacIT.class, createSharedTestDir(SimpleMacIT.class.getName() + "-ssl"));
    cluster = new MiniAccumuloClusterImpl(cfg);
  }

  @Before
  public void logTestInfo() {
    if (cluster != null)
      log.debug("Running " + this.getClass().getSimpleName() + "." + testName.getMethodName() + "() in " + getFolder().getAbsolutePath());
  }

  @Override
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    Connector conn = getInstanceOneConnector();
    return conn == null ? cluster.getConnector("root", ROOT_PASSWORD) : conn;
  }

  @Override
  public String rootPath() {
    return (getInstanceOneConnector() == null ? cluster.getConfig().getDir() : getInstanceOnePath()).getAbsolutePath();
  }

  public static MiniAccumuloClusterImpl getStaticCluster() {
    return cluster;
  }

  public static File getFolder() {
    return folder;
  }

  @After
  public void cleanUp() throws Exception {}

  @AfterClass
  public static void tearDown() throws Exception {}

  /**
   * Try to get a common instance to connect to. (For example, one started in the pre-integration-test phase.) This may not be a MiniAccumuloCluster instance.
   */
  private static Connector getInstanceOneConnector() {
    try {
      return new MiniAccumuloInstance(INSTANCE_NAME, getInstanceOnePath()).getConnector("root", new PasswordToken(ROOT_PASSWORD));
    } catch (Exception e) {
      return null;
    }
  }

  private static File getInstanceOnePath() {
    return new File(System.getProperty("user.dir") + "/accumulo-maven-plugin/instance1");
  }

  @Override
  protected ClientConfiguration getClientConfig() throws FileNotFoundException, ConfigurationException {
    if (getInstanceOneConnector() == null) {
      return new ClientConfiguration(new PropertiesConfiguration(cluster.getConfig().getClientConfFile()));
    } else {
      File directory = getInstanceOnePath();
      return new ClientConfiguration(MiniAccumuloInstance.getConfigProperties(directory)).withInstance(INSTANCE_NAME).withZkHosts(
          MiniAccumuloInstance.getZooKeepersFromDir(directory));
    }
  }

}
