/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.harness;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.cluster.ClusterUsers;
import org.apache.accumulo.cluster.standalone.StandaloneAccumuloCluster;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.conf.AccumuloClusterConfiguration;
import org.apache.accumulo.harness.conf.AccumuloClusterPropertyConfiguration;
import org.apache.accumulo.harness.conf.StandaloneAccumuloClusterConfiguration;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.categories.StandaloneCapableClusterTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
 * Integration-Test base class that provides a MAC instance per test. WARNING: This IT type will
 * setup and teardown an entire cluster for every test annotated with @Test and is reserved for more
 * advanced ITs that do crazy things. For more typical, expected behavior of a cluster see
 * {@link SharedMiniClusterBase}. This instance can be MAC or a standalone instance.
 */
@Category(StandaloneCapableClusterTests.class)
public abstract class AccumuloClusterHarness extends AccumuloITBase
    implements MiniClusterConfigurationCallback, ClusterUsers {
  private static final Logger log = LoggerFactory.getLogger(AccumuloClusterHarness.class);
  private static final String TRUE = Boolean.toString(true);

  public enum ClusterType {
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
  public static void setUpHarness() throws Exception {
    clusterConf = AccumuloClusterPropertyConfiguration.get();
    type = clusterConf.getClusterType();

    if (type == ClusterType.MINI
        && TRUE.equals(System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION))) {
      krb = new TestingKdc();
      krb.start();
      log.info("MiniKdc started");
    }

    initialized = true;
  }

  @AfterClass
  public static void tearDownHarness() {
    if (krb != null) {
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
    // Before we try to instantiate the cluster, check to see if the test even wants to run against
    // this type of cluster
    Assume.assumeTrue(canRunTest(type));

    switch (type) {
      case MINI:
        MiniClusterHarness miniClusterHarness = new MiniClusterHarness();
        // Intrinsically performs the callback to let tests alter MiniAccumuloConfig and
        // core-site.xml
        cluster = miniClusterHarness.create(this, getAdminToken(), krb, this);
        // Login as the "root" user
        if (krb != null) {
          ClusterUser rootUser = krb.getRootUser();
          // Log in the 'client' user
          UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(),
              rootUser.getKeytab().getAbsolutePath());
        }
        break;
      case STANDALONE:
        StandaloneAccumuloClusterConfiguration conf =
            (StandaloneAccumuloClusterConfiguration) clusterConf;
        StandaloneAccumuloCluster standaloneCluster =
            new StandaloneAccumuloCluster(conf.getClientInfo(), conf.getTmpDirectory(),
                conf.getUsers(), conf.getServerAccumuloConfDir());
        // If these are provided in the configuration, pass them into the cluster
        standaloneCluster.setAccumuloHome(conf.getAccumuloHome());
        standaloneCluster.setClientAccumuloConfDir(conf.getClientAccumuloConfDir());
        standaloneCluster.setHadoopConfDir(conf.getHadoopConfDir());
        // If these were not provided then ensure they are not null
        standaloneCluster
            .setServerCmdPrefix(conf.getServerCmdPrefix() == null ? "" : conf.getServerCmdPrefix());
        standaloneCluster
            .setClientCmdPrefix(conf.getClientCmdPrefix() == null ? "" : conf.getClientCmdPrefix());
        cluster = standaloneCluster;

        // For SASL, we need to get the Hadoop configuration files as well otherwise UGI will log in
        // as SIMPLE instead of KERBEROS
        if (saslEnabled()) {
          // Note that getting the Hadoop config creates a servercontext which wacks up the
          // AccumuloClientIT test so if SASL is enabled then the testclose() will fail
          Configuration hadoopConfiguration = standaloneCluster.getHadoopConfiguration();
          UserGroupInformation.setConfiguration(hadoopConfiguration);
          // Login as the admin user to start the tests
          UserGroupInformation.loginUserFromKeytab(conf.getAdminPrincipal(),
              conf.getAdminKeytab().getAbsolutePath());
        }
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
        if (krb != null) {
          final String traceTable = Property.TRACE_TABLE.getDefaultValue();
          final ClusterUser systemUser = krb.getAccumuloServerUser(), rootUser = krb.getRootUser();

          // Login as the trace user
          UserGroupInformation.loginUserFromKeytab(systemUser.getPrincipal(),
              systemUser.getKeytab().getAbsolutePath());

          // Create client as the system user (ensures the user will exist for us to assign
          // permissions to)
          UserGroupInformation.loginUserFromKeytab(systemUser.getPrincipal(),
              systemUser.getKeytab().getAbsolutePath());
          AccumuloClient c =
              cluster.createAccumuloClient(systemUser.getPrincipal(), new KerberosToken());
          c.close();

          // Then, log back in as the "root" user and do the grant
          UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(),
              rootUser.getKeytab().getAbsolutePath());

          try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
            // Create the trace table
            client.tableOperations().create(traceTable);

            // Trace user (which is the same kerberos principal as the system user, but using a
            // normal KerberosToken) needs to be able to read, write and alter the trace table
            client.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable,
                TablePermission.READ);
            client.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable,
                TablePermission.WRITE);
            client.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable,
                TablePermission.ALTER_TABLE);
          }
        }
        break;
      default:
        // do nothing
    }
  }

  public void cleanupTables() throws Exception {
    final String tablePrefix = this.getClass().getSimpleName() + "_";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final TableOperations tops = client.tableOperations();
      for (String table : tops.list()) {
        if (table.startsWith(tablePrefix)) {
          log.debug("Removing table {}", table);
          tops.delete(table);
        }
      }
    }
  }

  public void cleanupUsers() throws Exception {
    final String userPrefix = this.getClass().getSimpleName();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final SecurityOperations secOps = client.securityOperations();
      for (String user : secOps.listLocalUsers()) {
        if (user.startsWith(userPrefix)) {
          log.info("Dropping local user {}", user);
          secOps.dropLocalUser(user);
        }
      }
    }
  }

  @After
  public void teardownCluster() throws Exception {
    if (cluster != null) {
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

  public static Properties getClientProps() {
    checkState(initialized);
    return getCluster().getClientProperties();
  }

  public static ClientInfo getClientInfo() {
    checkState(initialized);
    return ClientInfo.from(getCluster().getClientProperties());
  }

  public static ServerContext getServerContext() {
    return getCluster().getServerContext();
  }

  public static boolean saslEnabled() {
    if (initialized) {
      return getClientInfo().saslEnabled();
    }
    return false;
  }

  public static AuthenticationToken getAdminToken() {
    checkState(initialized);
    return clusterConf.getAdminToken();
  }

  @Override
  public ClusterUser getAdminUser() {
    switch (type) {
      case MINI:
        if (krb == null) {
          PasswordToken passwordToken = (PasswordToken) getAdminToken();
          return new ClusterUser(getAdminPrincipal(),
              new String(passwordToken.getPassword(), UTF_8));
        }
        return krb.getRootUser();
      case STANDALONE:
        return new ClusterUser(getAdminPrincipal(),
            ((StandaloneAccumuloClusterConfiguration) clusterConf).getAdminKeytab());
      default:
        throw new RuntimeException("Unknown cluster type");
    }
  }

  @Override
  public ClusterUser getUser(int offset) {
    switch (type) {
      case MINI:
        if (krb != null) {
          // Defer to the TestingKdc when kerberos is on so we can get the keytab instead of a
          // password
          return krb.getClientPrincipal(offset);
        } else {
          // Come up with a mostly unique name
          String principal =
              getClass().getSimpleName() + "_" + testName.getMethodName() + "_" + offset;
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

  // TODO Really don't want this here. Will ultimately need to abstract configuration method away
  // from MAConfig
  // and change over to something more generic
  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {}

  /**
   * A test may not be capable of running against a given AccumuloCluster. Implementations can
   * override this method to advertise that they cannot (or perhaps do not) want to run the test.
   */
  public boolean canRunTest(ClusterType type) {
    return true;
  }

}
