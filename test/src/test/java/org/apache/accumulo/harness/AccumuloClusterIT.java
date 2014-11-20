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
package org.apache.accumulo.harness;

import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.cluster.standalone.StandaloneAccumuloCluster;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.conf.AccumuloClusterPropertyConfiguration;
import org.apache.accumulo.harness.conf.AccumuloStandaloneClusterConfiguration;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * General Integration-Test base class that provides an Accumulo instance
 */
public abstract class AccumuloClusterIT extends AccumuloIT implements MiniClusterConfigurationCallback {
  private static final Logger log = LoggerFactory.getLogger(AccumuloClusterIT.class);

  public static enum ClusterType {
    MINI, STANDALONE;

    public boolean isDynamic() {
      return this == MINI;
    }
  }

  private static boolean initialized = false;

  protected static AccumuloCluster cluster;
  protected static ClusterType type;
  protected static AccumuloClusterPropertyConfiguration clusterConf;

  @BeforeClass
  public static void setUp() throws Exception {
    clusterConf = AccumuloClusterPropertyConfiguration.get();
    type = clusterConf.getClusterType();

    initialized = true;
  }

  @Before
  public void setupCluster() throws Exception {
    // Before we try to instantiate the cluster, check to see if the test even wants to run against this type of cluster
    Assume.assumeTrue(canRunTest(type));

    switch (type) {
      case MINI:
        MiniClusterHarness miniClusterHarness = new MiniClusterHarness();
        // Intrinsically performs the callback to let tests alter MiniAccumuloConfig and core-site.xml
        cluster = miniClusterHarness.create(this, getToken());
        break;
      case STANDALONE:
        AccumuloStandaloneClusterConfiguration conf = (AccumuloStandaloneClusterConfiguration) clusterConf;
        cluster = new StandaloneAccumuloCluster(conf.getInstance());
        break;
      default:
        throw new RuntimeException("Unhandled type");
    }

    if (type.isDynamic()) {
      cluster.start();
    } else {
      log.info("Removing tables which appear to be from a previous test run");
      cleanupTables();
      log.info("Removing users which appear to be from a previous test run");
      cleanupUsers();
    }
  }

  public void cleanupTables() throws Exception {
    final String tablePrefix = this.getClass().getSimpleName() + "_";
    final TableOperations tops = getConnector().tableOperations();
    for (String table : tops.list()) {
      if (table.startsWith(tablePrefix)) {
        log.debug("Removing table {}", table);
        tops.delete(table);
      }
    }
  }

  public void cleanupUsers() throws Exception {
    final String userPrefix = this.getClass().getSimpleName();
    final SecurityOperations secOps = getConnector().securityOperations();
    for (String user : secOps.listLocalUsers()) {
      if (user.startsWith(userPrefix)) {
        log.info("Dropping local user {}", user);
        secOps.dropLocalUser(user);
      }
    }
  }

  @After
  public void teardownCluster() throws Exception {
    if (null != cluster) {
      if (type.isDynamic()) {
        cluster.stop();
      } else {
        log.info("Removing tables which appear to be from the current test");
        cleanupTables();
        log.info("Removing users which appear to be from the current test");
        cleanupUsers();
      }
    }
  }

  public static AccumuloCluster getCluster() {
    Preconditions.checkState(initialized);
    return cluster;
  }

  public static ClusterType getClusterType() {
    Preconditions.checkState(initialized);
    return type;
  }

  public static String getPrincipal() {
    Preconditions.checkState(initialized);
    return clusterConf.getPrincipal();
  }

  public static AuthenticationToken getToken() {
    Preconditions.checkState(initialized);
    return clusterConf.getToken();
  }

  public Connector getConnector() {
    try {
      return cluster.getConnector(getPrincipal(), getToken());
    } catch (Exception e) {
      log.error("Could not connect to Accumulo", e);
      fail("Could not connect to Accumulo");

      throw new RuntimeException("Could not connect to Accumulo", e);
    }
  }

  @SuppressWarnings("deprecation")
  public FileSystem getFileSystem() {
    try {
      Connector conn = getConnector();
      Map<String,String> conf = conn.instanceOperations().getSystemConfiguration();
      String uri;
      if (conf.containsKey(Property.INSTANCE_VOLUMES.getKey())) {
        String instanceVolumes = conf.get(Property.INSTANCE_VOLUMES.getKey());
        uri = StringUtils.split(instanceVolumes, ',')[0];
      } else if (conf.containsKey(Property.INSTANCE_DFS_URI.getKey())) {
        uri = conf.get(Property.INSTANCE_DFS_URI.getKey());
      } else {
        throw new RuntimeException("No DFS configuraiton present in system configuration");
      }

      return FileSystem.get(new Path(uri).toUri(), new Configuration());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // TODO Really don't want this here. Will ultimately need to abstract configuration method away from MAConfig
  // and change over to something more generic
  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {}

  /**
   * A test may not be capable of running against a given AccumuloCluster. Implementations can override this method to advertise that they cannot (or perhaps do
   * not) want to run the test.
   */
  public boolean canRunTest(ClusterType type) {
    return true;
  }
}
