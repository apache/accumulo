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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.cluster.ClusterUsers;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class which starts a single MAC instance for a test to leverage.
 *
 * There isn't a good way to build this off of the {@link AccumuloClusterHarness} (as would be the logical place) because we need to start the
 * MiniAccumuloCluster in a static BeforeClass-annotated method. Because it is static and invoked before any other BeforeClass methods in the implementation,
 * the actual test classes can't expose any information to tell the base class that it is to perform the one-MAC-per-class semantics.
 *
 * Implementations of this class must be sure to invoke {@link #startMiniCluster()} or {@link #startMiniClusterWithConfig(MiniClusterConfigurationCallback)} in
 * a method annotated with the {@link org.junit.BeforeClass} JUnit annotation and {@link #stopMiniCluster()} in a method annotated with the
 * {@link org.junit.AfterClass} JUnit annotation.
 */
@Category(MiniClusterOnlyTests.class)
public abstract class SharedMiniClusterBase extends AccumuloITBase implements ClusterUsers {
  private static final Logger log = LoggerFactory.getLogger(SharedMiniClusterBase.class);
  public static final String TRUE = Boolean.toString(true);

  private static String principal = "root";
  private static String rootPassword;
  private static AuthenticationToken token;
  private static MiniAccumuloClusterImpl cluster;
  private static TestingKdc krb;

  /**
   * Starts a MiniAccumuloCluster instance with the default configuration.
   */
  public static void startMiniCluster() throws Exception {
    startMiniClusterWithConfig(MiniClusterConfigurationCallback.NO_CALLBACK);
  }

  /**
   * Starts a MiniAccumuloCluster instance with the default configuration but also provides the caller the opportunity to update the configuration before the
   * MiniAccumuloCluster is started.
   *
   * @param miniClusterCallback
   *          A callback to configure the minicluster before it is started.
   */
  public static void startMiniClusterWithConfig(MiniClusterConfigurationCallback miniClusterCallback) throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());

    // Make a shared MAC instance instead of spinning up one per test method
    MiniClusterHarness harness = new MiniClusterHarness();

    if (TRUE.equals(System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION))) {
      krb = new TestingKdc();
      krb.start();
      // Enabled krb auth
      Configuration conf = new Configuration(false);
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(conf);
      // Login as the client
      ClusterUser rootUser = krb.getRootUser();
      // Get the krb token
      UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
      token = new KerberosToken();
    } else {
      rootPassword = "rootPasswordShared1";
      token = new PasswordToken(rootPassword);
    }

    cluster = harness.create(SharedMiniClusterBase.class.getName(), System.currentTimeMillis() + "_" + new Random().nextInt(Short.MAX_VALUE), token,
        miniClusterCallback, krb);
    cluster.start();

    if (null != krb) {
      final String traceTable = Property.TRACE_TABLE.getDefaultValue();
      final ClusterUser systemUser = krb.getAccumuloServerUser(), rootUser = krb.getRootUser();
      // Login as the trace user
      // Open a connector as the system user (ensures the user will exist for us to assign permissions to)
      UserGroupInformation.loginUserFromKeytab(systemUser.getPrincipal(), systemUser.getKeytab().getAbsolutePath());
      Connector conn = cluster.getConnector(systemUser.getPrincipal(), new KerberosToken());

      // Then, log back in as the "root" user and do the grant
      UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
      conn = cluster.getConnector(principal, token);

      // Create the trace table
      conn.tableOperations().create(traceTable);

      // Trace user (which is the same kerberos principal as the system user, but using a normal KerberosToken) needs
      // to have the ability to read, write and alter the trace table
      conn.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable, TablePermission.READ);
      conn.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable, TablePermission.WRITE);
      conn.securityOperations().grantTablePermission(systemUser.getPrincipal(), traceTable, TablePermission.ALTER_TABLE);
    }
  }

  /**
   * Stops the MiniAccumuloCluster and related services if they are running.
   */
  public static void stopMiniCluster() throws Exception {
    if (null != cluster) {
      try {
        cluster.stop();
      } catch (Exception e) {
        log.error("Failed to stop minicluster", e);
      }
    }
    if (null != krb) {
      try {
        krb.stop();
      } catch (Exception e) {
        log.error("Failed to stop KDC", e);
      }
    }
  }

  public static String getRootPassword() {
    return rootPassword;
  }

  public static AuthenticationToken getToken() {
    if (token instanceof KerberosToken) {
      try {
        UserGroupInformation.loginUserFromKeytab(getPrincipal(), krb.getRootUser().getKeytab().getAbsolutePath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to login", e);
      }
    }
    return token;
  }

  public static String getPrincipal() {
    return principal;
  }

  public static MiniAccumuloClusterImpl getCluster() {
    return cluster;
  }

  public static File getMiniClusterDir() {
    return cluster.getConfig().getDir();
  }

  public static Connector getConnector() {
    try {
      return getCluster().getConnector(principal, getToken());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static TestingKdc getKdc() {
    return krb;
  }

  @Override
  public ClusterUser getAdminUser() {
    if (null == krb) {
      return new ClusterUser(getPrincipal(), getRootPassword());
    } else {
      return krb.getRootUser();
    }
  }

  @Override
  public ClusterUser getUser(int offset) {
    if (null == krb) {
      String user = SharedMiniClusterBase.class.getName() + "_" + testName.getMethodName() + "_" + offset;
      // Password is the username
      return new ClusterUser(user, user);
    } else {
      return krb.getClientPrincipal(offset);
    }
  }
}
