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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.cluster.ClusterUsers;
import org.apache.accumulo.cluster.standalone.StandaloneAccumuloCluster;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.conf.AccumuloClusterConfiguration;
import org.apache.accumulo.harness.conf.AccumuloClusterPropertyConfiguration;
import org.apache.accumulo.harness.conf.AccumuloMiniClusterConfiguration;
import org.apache.accumulo.harness.conf.StandaloneAccumuloClusterConfiguration;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.categories.StandaloneCapableClusterTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General Integration-Test base class that provides access to an Accumulo instance for testing. This instance could be MAC or a standalone instance.
 */
@Category(StandaloneCapableClusterTests.class)
public abstract class AccumuloClusterHarness extends AccumuloITBase implements MiniClusterConfigurationCallback, ClusterUsers {
  private static final Logger log = LoggerFactory.getLogger(AccumuloClusterHarness.class);
  private static final String TRUE = Boolean.toString(true);

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
  protected static TestingKdc krb;

  @BeforeClass
  public static void setUp() throws Exception {
    clusterConf = AccumuloClusterPropertyConfiguration.get();
    type = clusterConf.getClusterType();

    if (ClusterType.MINI == type && TRUE.equals(System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION))) {
      krb = new TestingKdc();
      krb.start();
      log.info("MiniKdc started");
    }

    initialized = true;
  }

  @AfterClass
  public static void tearDownKdc() throws Exception {
    if (null != krb) {
      krb.stop();
    }
  }

  /**
   * The {@link TestingKdc} used for this {@link AccumuloCluster}. Might be null.
   */
  public static TestingKdc getKdc() {
    return krb;
  }

  @Before
  public void setupCluster() throws Exception {
    // Before we try to instantiate the cluster, check to see if the test even wants to run against this type of cluster
    Assume.assumeTrue(canRunTest(type));

    switch (type) {
      case MINI:
        MiniClusterHarness miniClusterHarness = new MiniClusterHarness();
        // Intrinsically performs the callback to let tests alter MiniAccumuloConfig and core-site.xml
        MiniAccumuloClusterImpl impl = miniClusterHarness.create(this, getAdminToken(), krb);
        cluster = impl;
        // MAC makes a ClientConf for us, just set it
        ((AccumuloMiniClusterConfiguration) clusterConf).setClientConf(impl.getClientConfig());
        // Login as the "root" user
        if (null != krb) {
          ClusterUser rootUser = krb.getRootUser();
          // Log in the 'client' user
          UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
        }
        break;
      case STANDALONE:
        StandaloneAccumuloClusterConfiguration conf = (StandaloneAccumuloClusterConfiguration) clusterConf;
        ClientConfiguration clientConf = conf.getClientConf();
        StandaloneAccumuloCluster standaloneCluster = new StandaloneAccumuloCluster(conf.getInstance(), clientConf, conf.getTmpDirectory(), conf.getUsers());
        // If these are provided in the configuration, pass them into the cluster
        standaloneCluster.setAccumuloHome(conf.getAccumuloHome());
        standaloneCluster.setClientAccumuloConfDir(conf.getClientAccumuloConfDir());
        standaloneCluster.setServerAccumuloConfDir(conf.getServerAccumuloConfDir());
        standaloneCluster.setHadoopConfDir(conf.getHadoopConfDir());
        standaloneCluster.setServerCmdPrefix(conf.getServerCmdPrefix());
        standaloneCluster.setClientCmdPrefix(conf.getClientCmdPrefix());

        // For SASL, we need to get the Hadoop configuration files as well otherwise UGI will log in as SIMPLE instead of KERBEROS
        Configuration hadoopConfiguration = standaloneCluster.getHadoopConfiguration();
        if (clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
          UserGroupInformation.setConfiguration(hadoopConfiguration);
          // Login as the admin user to start the tests
          UserGroupInformation.loginUserFromKeytab(conf.getAdminPrincipal(), conf.getAdminKeytab().getAbsolutePath());
        }

        // Set the implementation
        cluster = standaloneCluster;
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

    switch (type) {
      case MINI:
        if (null != krb) {
          final String traceTable = Property.TRACE_TABLE.getDefaultValue();
          final ClusterUser systemUser = krb.getAccumuloServerUser(), rootUser = krb.getRootUser();

          // Login as the trace user
          UserGroupInformation.loginUserFromKeytab(systemUser.getPrincipal(), systemUser.getKeytab().getAbsolutePath());

          // Open a connector as the system user (ensures the user will exist for us to assign permissions to)
          UserGroupInformation.loginUserFromKeytab(systemUser.getPrincipal(), systemUser.getKeytab().getAbsolutePath());
          Connector conn = cluster.getConnector(systemUser.getPrincipal(), new KerberosToken());

          // Then, log back in as the "root" user and do the grant
          UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
          conn = getConnector();

          // Create the trace table
          conn.tableOperations().create(traceTable);

          // Trace user (which is the same kerberos principal as the system user, but using a normal KerberosToken) needs
          // to have the ability to read, write and alter the trace table
          conn.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable, TablePermission.READ);
          conn.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable, TablePermission.WRITE);
          conn.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable, TablePermission.ALTER_TABLE);
        }
        break;
      default:
        // do nothing
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
    checkState(initialized);
    return cluster;
  }

  public static ClusterControl getClusterControl() {
    checkState(initialized);
    return cluster.getClusterControl();
  }

  public static ClusterType getClusterType() {
    checkState(initialized);
    return type;
  }

  public static String getAdminPrincipal() {
    checkState(initialized);
    return clusterConf.getAdminPrincipal();
  }

  public static AuthenticationToken getAdminToken() {
    checkState(initialized);
    return clusterConf.getAdminToken();
  }

  @Override
  public ClusterUser getAdminUser() {
    switch (type) {
      case MINI:
        if (null == krb) {
          PasswordToken passwordToken = (PasswordToken) getAdminToken();
          return new ClusterUser(getAdminPrincipal(), new String(passwordToken.getPassword(), UTF_8));
        }
        return krb.getRootUser();
      case STANDALONE:
        return new ClusterUser(getAdminPrincipal(), ((StandaloneAccumuloClusterConfiguration) clusterConf).getAdminKeytab());
      default:
        throw new RuntimeException("Unknown cluster type");
    }
  }

  @Override
  public ClusterUser getUser(int offset) {
    switch (type) {
      case MINI:
        if (null != krb) {
          // Defer to the TestingKdc when kerberos is on so we can get the keytab instead of a password
          return krb.getClientPrincipal(offset);
        } else {
          // Come up with a mostly unique name
          String principal = getClass().getSimpleName() + "_" + testName.getMethodName() + "_" + offset;
          // Username and password are the same
          return new ClusterUser(principal, principal);
        }
      case STANDALONE:
        return ((StandaloneAccumuloCluster) cluster).getUser(offset);
      default:
        throw new RuntimeException("Unknown cluster type");
    }
  }

  public static FileSystem getFileSystem() throws IOException {
    checkState(initialized);
    return cluster.getFileSystem();
  }

  public static AccumuloClusterConfiguration getClusterConfiguration() {
    checkState(initialized);
    return clusterConf;
  }

  public Connector getConnector() {
    try {
      String princ = getAdminPrincipal();
      AuthenticationToken token = getAdminToken();
      log.debug("Creating connector as {} with {}", princ, token);
      return cluster.getConnector(princ, token);
    } catch (Exception e) {
      log.error("Could not connect to Accumulo", e);
      fail("Could not connect to Accumulo: " + e.getMessage());

      throw new RuntimeException("Could not connect to Accumulo", e);
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

  /**
   * Tries to give a reasonable directory which can be used to create temporary files for the test. Makes a basic attempt to create the directory if it does not
   * already exist.
   *
   * @return A directory which can be expected to exist on the Cluster's FileSystem
   */
  public Path getUsableDir() throws IllegalArgumentException, IOException {
    return cluster.getTemporaryPath();
  }
}
