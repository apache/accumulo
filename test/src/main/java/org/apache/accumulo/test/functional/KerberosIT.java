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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * MAC test which uses {@link MiniKdc} to simulate ta secure environment. Can be used as a sanity
 * check for Kerberos/SASL testing.
 */
@Tag(MINI_CLUSTER_ONLY)
public class KerberosIT extends AccumuloITBase {
  private static final Logger log = LoggerFactory.getLogger(KerberosIT.class);

  private static TestingKdc kdc;
  private static String krbEnabledForITs = null;
  private static ClusterUser rootUser;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @BeforeAll
  public static void startKdc() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
    krbEnabledForITs = System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION);
    if (krbEnabledForITs == null || !Boolean.parseBoolean(krbEnabledForITs)) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    }
    rootUser = kdc.getRootUser();
  }

  @AfterAll
  public static void stopKdc() {
    if (kdc != null) {
      kdc.stop();
    }
    if (krbEnabledForITs != null) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, krbEnabledForITs);
    }
    UserGroupInformation.setConfiguration(new Configuration(false));
  }

  private MiniAccumuloClusterImpl mac;

  @BeforeEach
  public void startMac() throws Exception {
    MiniClusterHarness harness = new MiniClusterHarness();
    mac = harness.create(this, new PasswordToken("unused"), kdc,
        new MiniClusterConfigurationCallback() {

          @Override
          public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
            Map<String,String> site = cfg.getSiteConfig();
            site.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
            cfg.setSiteConfig(site);
          }

        });

    mac.getConfig().setNumTservers(1);
    mac.start();
    // Enabled kerberos auth
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  @AfterEach
  public void stopMac() throws Exception {
    if (mac != null) {
      mac.stop();
    }
  }

  @Test
  public void testAdminUser() throws Exception {
    // Login as the client (provided to `accumulo init` as the "root" user)
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      final AccumuloClient client =
          mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());

      // The "root" user should have all system permissions
      for (SystemPermission perm : SystemPermission.values()) {
        assertTrue(client.securityOperations().hasSystemPermission(client.whoami(), perm),
            "Expected user to have permission: " + perm);
      }

      // and the ability to modify the root and metadata tables
      for (String table : Arrays.asList(RootTable.NAME, MetadataTable.NAME)) {
        assertTrue(client.securityOperations().hasTablePermission(client.whoami(), table,
            TablePermission.ALTER_TABLE));
      }
      return null;
    });
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void testNewUser() throws Exception {
    String newUser = testName();
    final File newUserKeytab = new File(kdc.getKeytabDir(), newUser + ".keytab");
    if (newUserKeytab.exists() && !newUserKeytab.delete()) {
      log.warn("Unable to delete {}", newUserKeytab);
    }

    // Create a new user
    kdc.createPrincipal(newUserKeytab, newUser);

    final String newQualifiedUser = kdc.qualifyUser(newUser);
    final HashSet<String> users = Sets.newHashSet(rootUser.getPrincipal());

    // Login as the "root" user
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      AccumuloClient client =
          mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());
      log.info("Created client as {}", rootUser.getPrincipal());
      assertEquals(rootUser.getPrincipal(), client.whoami());

      // Make sure the system user doesn't exist -- this will force some RPC to happen server-side
      createTableWithDataAndCompact(client);

      assertEquals(users, client.securityOperations().listLocalUsers());

      return null;
    });
    // Switch to a new user
    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(newQualifiedUser,
        newUserKeytab.getAbsolutePath());
    log.info("Logged in as {}", newQualifiedUser);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      AccumuloClient client = mac.createAccumuloClient(newQualifiedUser, new KerberosToken());
      log.info("Created client as {}", newQualifiedUser);
      assertEquals(newQualifiedUser, client.whoami());

      // The new user should have no system permissions
      for (SystemPermission perm : SystemPermission.values()) {
        assertFalse(client.securityOperations().hasSystemPermission(newQualifiedUser, perm));
      }

      users.add(newQualifiedUser);

      // Same users as before, plus the new user we just created
      assertEquals(users, client.securityOperations().listLocalUsers());
      return null;
    });
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void testUserPrivilegesThroughGrant() throws Exception {
    String user1 = testName();
    final File user1Keytab = new File(kdc.getKeytabDir(), user1 + ".keytab");
    if (user1Keytab.exists() && !user1Keytab.delete()) {
      log.warn("Unable to delete {}", user1Keytab);
    }

    // Create some new users
    kdc.createPrincipal(user1Keytab, user1);

    final String qualifiedUser1 = kdc.qualifyUser(user1);

    // Log in as user1
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(user1, user1Keytab.getAbsolutePath());
    log.info("Logged in as {}", user1);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      // Indirectly creates this user when we use it
      AccumuloClient client = mac.createAccumuloClient(qualifiedUser1, new KerberosToken());
      log.info("Created client as {}", qualifiedUser1);

      // The new user should have no system permissions
      for (SystemPermission perm : SystemPermission.values()) {
        assertFalse(client.securityOperations().hasSystemPermission(qualifiedUser1, perm));
      }

      return null;
    });

    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(),
        rootUser.getKeytab().getAbsolutePath());
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      AccumuloClient client =
          mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());
      client.securityOperations().grantSystemPermission(qualifiedUser1,
          SystemPermission.CREATE_TABLE);
      return null;
    });

    // Switch back to the original user
    ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(user1, user1Keytab.getAbsolutePath());
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      AccumuloClient client = mac.createAccumuloClient(qualifiedUser1, new KerberosToken());

      // Shouldn't throw an exception since we granted the create table permission
      final String table = testName() + "_user_table";
      client.tableOperations().create(table);

      // Make sure we can actually use the table we made
      try (BatchWriter bw = client.createBatchWriter(table)) {
        Mutation m = new Mutation("a");
        m.put("b", "c", "d");
        bw.addMutation(m);
      }

      client.tableOperations().compact(table, new CompactionConfig().setWait(true).setFlush(true));
      return null;
    });
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void testUserPrivilegesForTable() throws Exception {
    String user1 = testName();
    final File user1Keytab = new File(kdc.getKeytabDir(), user1 + ".keytab");
    if (user1Keytab.exists() && !user1Keytab.delete()) {
      log.warn("Unable to delete {}", user1Keytab);
    }

    // Create some new users -- cannot contain realm
    kdc.createPrincipal(user1Keytab, user1);

    final String qualifiedUser1 = kdc.qualifyUser(user1);

    // Log in as user1
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(qualifiedUser1,
        user1Keytab.getAbsolutePath());
    log.info("Logged in as {}", user1);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      // Indirectly creates this user when we use it
      AccumuloClient client = mac.createAccumuloClient(qualifiedUser1, new KerberosToken());
      log.info("Created client as {}", qualifiedUser1);

      // The new user should have no system permissions
      for (SystemPermission perm : SystemPermission.values()) {
        assertFalse(client.securityOperations().hasSystemPermission(qualifiedUser1, perm));
      }
      return null;
    });

    final String table = testName() + "_user_table";
    final String viz = "viz";

    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(),
        rootUser.getKeytab().getAbsolutePath());

    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      AccumuloClient client =
          mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());
      client.tableOperations().create(table);
      // Give our unprivileged user permission on the table we made for them
      client.securityOperations().grantTablePermission(qualifiedUser1, table, TablePermission.READ);
      client.securityOperations().grantTablePermission(qualifiedUser1, table,
          TablePermission.WRITE);
      client.securityOperations().grantTablePermission(qualifiedUser1, table,
          TablePermission.ALTER_TABLE);
      client.securityOperations().grantTablePermission(qualifiedUser1, table,
          TablePermission.DROP_TABLE);
      client.securityOperations().changeUserAuthorizations(qualifiedUser1, new Authorizations(viz));
      return null;
    });

    // Switch back to the original user
    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(qualifiedUser1,
        user1Keytab.getAbsolutePath());
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      AccumuloClient client = mac.createAccumuloClient(qualifiedUser1, new KerberosToken());

      // Make sure we can actually use the table we made

      // Write data
      final long ts = 1000L;
      try (BatchWriter bw = client.createBatchWriter(table)) {
        Mutation m = new Mutation("a");
        m.put("b", "c", new ColumnVisibility(viz.getBytes()), ts, "d");
        bw.addMutation(m);
      }

      // Compact
      client.tableOperations().compact(table, new CompactionConfig().setWait(true).setFlush(true));

      // Alter
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "true");

      // Read (and proper authorizations)
      try (Scanner s = client.createScanner(table, new Authorizations(viz))) {
        Entry<Key,Value> entry = getOnlyElement(s);
        assertEquals(new Key("a", "b", "c", viz, ts), entry.getKey());
        assertEquals(new Value("d"), entry.getValue());
        return null;
      }
    });
  }

  @Test
  public void testDelegationToken() throws Exception {
    final String tableName = getUniqueNames(1)[0];

    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    final int numRows = 100, numColumns = 10;

    // As the "root" user, open up the connection and get a delegation token
    final AuthenticationToken delegationToken =
        root.doAs((PrivilegedExceptionAction<AuthenticationToken>) () -> {
          AccumuloClient client =
              mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());
          log.info("Created client as {}", rootUser.getPrincipal());
          assertEquals(rootUser.getPrincipal(), client.whoami());

          client.tableOperations().create(tableName);
          try (BatchWriter bw = client.createBatchWriter(tableName)) {
            for (int r = 0; r < numRows; r++) {
              Mutation m = new Mutation(Integer.toString(r));
              for (int c = 0; c < numColumns; c++) {
                String col = Integer.toString(c);
                m.put(col, col, col);
              }
              bw.addMutation(m);
            }
          }

          return client.securityOperations().getDelegationToken(new DelegationTokenConfig());
        });

    // The above login with keytab doesn't have a way to logout, so make a fake user that won't have
    // krb credentials
    UserGroupInformation userWithoutPrivs =
        UserGroupInformation.createUserForTesting("fake_user", new String[0]);
    int recordsSeen = userWithoutPrivs.doAs((PrivilegedExceptionAction<Integer>) () -> {
      AccumuloClient client = mac.createAccumuloClient(rootUser.getPrincipal(), delegationToken);

      try (BatchScanner bs = client.createBatchScanner(tableName)) {
        bs.setRanges(Collections.singleton(new Range()));
        return Iterables.size(bs);
      }
    });

    assertEquals(numRows * numColumns, recordsSeen);
  }

  @Test
  public void testDelegationTokenAsDifferentUser() throws Exception {
    // Login as the "root" user
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    final AuthenticationToken delegationToken;
    try {
      delegationToken = ugi.doAs((PrivilegedExceptionAction<AuthenticationToken>) () -> {
        // As the "root" user, open up the connection and get a delegation token
        AccumuloClient client =
            mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());
        log.info("Created client as {}", rootUser.getPrincipal());
        assertEquals(rootUser.getPrincipal(), client.whoami());
        return client.securityOperations().getDelegationToken(new DelegationTokenConfig());
      });
    } catch (UndeclaredThrowableException ex) {
      throw ex;
    }

    // make a fake user that won't have krb credentials
    UserGroupInformation userWithoutPrivs =
        UserGroupInformation.createUserForTesting("fake_user", new String[0]);
    // Use the delegation token to try to log in as a different user
    var e = assertThrows(UndeclaredThrowableException.class,
        () -> userWithoutPrivs.doAs((PrivilegedExceptionAction<Void>) () -> {
          AccumuloClient client = mac.createAccumuloClient("some_other_user", delegationToken);
          client.securityOperations().authenticateUser("some_other_user", delegationToken);
          return null;
        }), "Using a delegation token as a different user should throw an exception");

    Throwable cause = e.getCause();
    assertNotNull(cause);
    // We should get an AccumuloSecurityException from trying to use a delegation token for the
    // wrong user
    assertTrue(cause instanceof AccumuloSecurityException,
        "Expected cause to be AccumuloSecurityException, but was " + cause.getClass());
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void testGetDelegationTokenDenied() throws Exception {
    String newUser = testName();
    final File newUserKeytab = new File(kdc.getKeytabDir(), newUser + ".keytab");
    if (newUserKeytab.exists() && !newUserKeytab.delete()) {
      log.warn("Unable to delete {}", newUserKeytab);
    }

    // Create a new user
    kdc.createPrincipal(newUserKeytab, newUser);

    final String qualifiedNewUser = kdc.qualifyUser(newUser);

    // Login as a normal user
    UserGroupInformation ugi = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(qualifiedNewUser, newUserKeytab.getAbsolutePath());
    try {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        // As the "root" user, open up the connection and get a delegation token
        AccumuloClient client = mac.createAccumuloClient(qualifiedNewUser, new KerberosToken());
        log.info("Created client as {}", qualifiedNewUser);
        assertEquals(qualifiedNewUser, client.whoami());

        client.securityOperations().getDelegationToken(new DelegationTokenConfig());
        return null;
      });
    } catch (UndeclaredThrowableException ex) {
      assertTrue(ex.getCause() instanceof AccumuloSecurityException);
    }
  }

  @Test
  public void testRestartedManagerReusesSecretKey() throws Exception {
    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    // As the "root" user, open up the connection and get a delegation token
    final AuthenticationToken delegationToken1 =
        root.doAs((PrivilegedExceptionAction<AuthenticationToken>) () -> {
          AccumuloClient client =
              mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());
          log.info("Created client as {}", rootUser.getPrincipal());
          assertEquals(rootUser.getPrincipal(), client.whoami());

          AuthenticationToken token =
              client.securityOperations().getDelegationToken(new DelegationTokenConfig());

          assertTrue(!mac.createAccumuloClient(rootUser.getPrincipal(), token).tableOperations()
              .list().isEmpty(), "Could not get tables with delegation token");

          return token;
        });

    log.info("Stopping manager");
    mac.getClusterControl().stop(ServerType.MANAGER);
    Thread.sleep(5000);
    log.info("Restarting manager");
    mac.getClusterControl().start(ServerType.MANAGER);

    // Make sure our original token is still good
    root.doAs((PrivilegedExceptionAction<Void>) () -> {
      AccumuloClient client = mac.createAccumuloClient(rootUser.getPrincipal(), delegationToken1);

      assertTrue(!client.tableOperations().list().isEmpty(),
          "Could not get tables with delegation token");

      return null;
    });

    // Get a new token, so we can compare the keyId on the second to the first
    final AuthenticationToken delegationToken2 =
        root.doAs((PrivilegedExceptionAction<AuthenticationToken>) () -> {
          AccumuloClient client =
              mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken());
          log.info("Created client as {}", rootUser.getPrincipal());
          assertEquals(rootUser.getPrincipal(), client.whoami());

          AuthenticationToken token =
              client.securityOperations().getDelegationToken(new DelegationTokenConfig());

          assertTrue(!mac.createAccumuloClient(rootUser.getPrincipal(), token).tableOperations()
              .list().isEmpty(), "Could not get tables with delegation token");

          return token;
        });

    // A restarted manager should reuse the same secret key after a restart if the secret key hasn't
    // expired (1day by default)
    DelegationTokenImpl dt1 = (DelegationTokenImpl) delegationToken1;
    DelegationTokenImpl dt2 = (DelegationTokenImpl) delegationToken2;
    assertEquals(dt1.getIdentifier().getKeyId(), dt2.getIdentifier().getKeyId());
  }

  @Test
  public void testDelegationTokenWithInvalidLifetime() throws Throwable {
    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());
    var e = assertThrows(UndeclaredThrowableException.class, () -> {
      // As the "root" user, open up the connection and get a delegation token
      root.doAs((PrivilegedExceptionAction<AuthenticationToken>) () -> {
        try (AccumuloClient client =
            mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken())) {
          log.info("Created client as {}", rootUser.getPrincipal());
          assertEquals(rootUser.getPrincipal(), client.whoami());

          // Should fail
          return client.securityOperations().getDelegationToken(
              new DelegationTokenConfig().setTokenLifetime(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
        }
      });
    });
    assertEquals(AccumuloException.class, e.getCause().getClass());
  }

  @Test
  public void testDelegationTokenWithReducedLifetime() throws Throwable {
    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    // As the "root" user, open up the connection and get a delegation token
    final AuthenticationToken dt =
        root.doAs((PrivilegedExceptionAction<AuthenticationToken>) () -> {
          try (AccumuloClient client =
              mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken())) {
            log.info("Created client as {}", rootUser.getPrincipal());
            assertEquals(rootUser.getPrincipal(), client.whoami());

            return client.securityOperations()
                .getDelegationToken(new DelegationTokenConfig().setTokenLifetime(5, MINUTES));
          }
        });

    AuthenticationTokenIdentifier identifier = ((DelegationTokenImpl) dt).getIdentifier();
    assertTrue(identifier.getExpirationDate() - identifier.getIssueDate() <= (MINUTES.toMillis(5)),
        "Expected identifier to expire in no more than 5 minutes: " + identifier);
  }

  @Test
  public void testRootUserHasIrrevocablePermissions() throws Exception {
    // Login as the client (provided to `accumulo init` as the "root" user)
    UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(),
        rootUser.getKeytab().getAbsolutePath());

    try (AccumuloClient client =
        mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken())) {

      // The server-side implementation should prevent the revocation of the 'root' user's systems
      // permissions because once they're gone, it's possible that they could never be restored.
      assertThrows(AccumuloSecurityException.class, () -> client.securityOperations()
          .revokeSystemPermission(rootUser.getPrincipal(), SystemPermission.GRANT));
    }
  }

  /**
   * Creates a table, adds a record to it, and then compacts the table. A simple way to make sure
   * that the system user exists (since the manager does an RPC to the tserver which will create the
   * system user if it doesn't already exist).
   */
  private void createTableWithDataAndCompact(AccumuloClient client) throws TableNotFoundException,
      AccumuloSecurityException, AccumuloException, TableExistsException {
    final String table = testName() + "_table";
    client.tableOperations().create(table);
    try (BatchWriter bw = client.createBatchWriter(table)) {
      Mutation m = new Mutation("a");
      m.put("b", "c", "d");
      bw.addMutation(m);
    }
    client.tableOperations().compact(table, new CompactionConfig().setFlush(true).setWait(true));
  }
}
