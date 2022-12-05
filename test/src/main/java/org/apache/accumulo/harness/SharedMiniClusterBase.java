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
package org.apache.accumulo.harness;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.StackWalker.StackFrame;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.cluster.ClusterUsers;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration-Test base class which starts one MAC for the entire Integration Test. This IT type is
 * faster and more geared for testing typical, expected behavior of a cluster. For more advanced
 * testing see {@link AccumuloClusterHarness}
 *
 * There isn't a good way to build this off of the {@link AccumuloClusterHarness} (as would be the
 * logical place) because we need to start the MiniAccumuloCluster in a static BeforeAll-annotated
 * method. Because it is static and invoked before any other BeforeAll methods in the
 * implementation, the actual test classes can't expose any information to tell the base class that
 * it is to perform the one-MAC-per-class semantics.
 *
 * Implementations of this class must be sure to invoke {@link #startMiniCluster()} or
 * {@link #startMiniClusterWithConfig(MiniClusterConfigurationCallback)} in a method annotated with
 * the {@link org.junit.jupiter.api.BeforeAll} JUnit annotation and {@link #stopMiniCluster()} in a
 * method annotated with the {@link org.junit.jupiter.api.AfterAll} JUnit annotation.
 */
@Tag(MINI_CLUSTER_ONLY)
public abstract class SharedMiniClusterBase extends AccumuloITBase implements ClusterUsers {
  private static final Logger log = LoggerFactory.getLogger(SharedMiniClusterBase.class);
  public static final String TRUE = Boolean.toString(true);

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
   * Starts a MiniAccumuloCluster instance with the default configuration but also provides the
   * caller the opportunity to update the configuration before the MiniAccumuloCluster is started.
   *
   * @param miniClusterCallback A callback to configure the minicluster before it is started.
   */
  public static void startMiniClusterWithConfig(
      MiniClusterConfigurationCallback miniClusterCallback) throws Exception {
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
      UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(),
          rootUser.getKeytab().getAbsolutePath());
      token = new KerberosToken();
    } else {
      rootPassword = "rootPasswordShared1";
      token = new PasswordToken(rootPassword);
    }

    cluster = harness.create(getTestClassName(), SharedMiniClusterBase.class.getSimpleName(), token,
        miniClusterCallback, krb);
    cluster.start();

  }

  private static String getTestClassName() {
    Predicate<Class<?>> findITClass = c -> c.getSimpleName().endsWith("IT");
    Function<Stream<StackFrame>,Optional<? extends Class<?>>> findCallerITClass =
        frames -> frames.map(StackFrame::getDeclaringClass).filter(findITClass).findFirst();
    Optional<String> callerClassName =
        StackWalker.getInstance(RETAIN_CLASS_REFERENCE).walk(findCallerITClass).map(Class::getName);
    // use the calling class name, or default to a unique name if IT class can't be found
    return callerClassName.orElse(String.format("UnknownITClass-%d-%d", System.currentTimeMillis(),
        random.nextInt(Short.MAX_VALUE)));
  }

  /**
   * Stops the MiniAccumuloCluster and related services if they are running.
   */
  public static void stopMiniCluster() {
    if (cluster != null) {
      try {
        cluster.stop();
      } catch (Exception e) {
        log.error("Failed to stop minicluster", e);
      }
    }
    if (krb != null) {
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
    return ClientProperty.getAuthenticationToken(getClientProps());
  }

  public static String getPrincipal() {
    return ClientProperty.AUTH_PRINCIPAL.getValue(getClientProps());
  }

  public static MiniAccumuloClusterImpl getCluster() {
    return cluster;
  }

  public static File getMiniClusterDir() {
    return cluster.getConfig().getDir();
  }

  public static Properties getClientProps() {
    return getCluster().getClientProperties();
  }

  public static TestingKdc getKdc() {
    return krb;
  }

  @Override
  public ClusterUser getAdminUser() {
    if (krb == null) {
      return new ClusterUser(getPrincipal(), getRootPassword());
    } else {
      return krb.getRootUser();
    }
  }

  @Override
  public ClusterUser getUser(int offset) {
    if (krb == null) {
      String user = SharedMiniClusterBase.class.getName() + "_" + testName() + "_" + offset;
      // Password is the username
      return new ClusterUser(user, user);
    } else {
      return krb.getClientPrincipal(offset);
    }
  }

  public static ClientInfo getClientInfo() {
    return ClientInfo.from(cluster.getClientProperties());
  }

  public static boolean saslEnabled() {
    return getClientInfo().saslEnabled();
  }

  public static String getAdminPrincipal() {
    return cluster.getConfig().getRootUserName();
  }
}
