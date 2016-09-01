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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.impl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.client.impl.DelegationTokenImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * MAC test which uses {@link MiniKdc} to simulate ta secure environment. Can be used as a sanity check for Kerberos/SASL testing.
 */
@Category(MiniClusterOnlyTests.class)
public class KerberosIT extends AccumuloITBase {
  private static final Logger log = LoggerFactory.getLogger(KerberosIT.class);

  private static TestingKdc kdc;
  private static String krbEnabledForITs = null;
  private static ClusterUser rootUser;

  @BeforeClass
  public static void startKdc() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
    krbEnabledForITs = System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION);
    if (null == krbEnabledForITs || !Boolean.parseBoolean(krbEnabledForITs)) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    }
    rootUser = kdc.getRootUser();
  }

  @AfterClass
  public static void stopKdc() throws Exception {
    if (null != kdc) {
      kdc.stop();
    }
    if (null != krbEnabledForITs) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, krbEnabledForITs);
    }
    UserGroupInformation.setConfiguration(new Configuration(false));
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60 * 5;
  }

  private MiniAccumuloClusterImpl mac;

  @Before
  public void startMac() throws Exception {
    MiniClusterHarness harness = new MiniClusterHarness();
    mac = harness.create(this, new PasswordToken("unused"), kdc, new MiniClusterConfigurationCallback() {

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

  @After
  public void stopMac() throws Exception {
    if (null != mac) {
      mac.stop();
    }
  }

  @Test
  public void testAdminUser() throws Exception {
    // Login as the client (provided to `accumulo init` as the "root" user)
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());

        // The "root" user should have all system permissions
        for (SystemPermission perm : SystemPermission.values()) {
          assertTrue("Expected user to have permission: " + perm, conn.securityOperations().hasSystemPermission(conn.whoami(), perm));
        }

        // and the ability to modify the root and metadata tables
        for (String table : Arrays.asList(RootTable.NAME, MetadataTable.NAME)) {
          assertTrue(conn.securityOperations().hasTablePermission(conn.whoami(), table, TablePermission.ALTER_TABLE));
        }
        return null;
      }
    });
  }

  @Test
  public void testNewUser() throws Exception {
    String newUser = testName.getMethodName();
    final File newUserKeytab = new File(kdc.getKeytabDir(), newUser + ".keytab");
    if (newUserKeytab.exists() && !newUserKeytab.delete()) {
      log.warn("Unable to delete {}", newUserKeytab);
    }

    // Create a new user
    kdc.createPrincipal(newUserKeytab, newUser);

    final String newQualifiedUser = kdc.qualifyUser(newUser);
    final HashSet<String> users = Sets.newHashSet(rootUser.getPrincipal());

    // Login as the "root" user
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
        log.info("Created connector as {}", rootUser.getPrincipal());
        assertEquals(rootUser.getPrincipal(), conn.whoami());

        // Make sure the system user doesn't exist -- this will force some RPC to happen server-side
        createTableWithDataAndCompact(conn);

        assertEquals(users, conn.securityOperations().listLocalUsers());

        return null;
      }
    });
    // Switch to a new user
    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(newQualifiedUser, newUserKeytab.getAbsolutePath());
    log.info("Logged in as {}", newQualifiedUser);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connector conn = mac.getConnector(newQualifiedUser, new KerberosToken());
        log.info("Created connector as {}", newQualifiedUser);
        assertEquals(newQualifiedUser, conn.whoami());

        // The new user should have no system permissions
        for (SystemPermission perm : SystemPermission.values()) {
          assertFalse(conn.securityOperations().hasSystemPermission(newQualifiedUser, perm));
        }

        users.add(newQualifiedUser);

        // Same users as before, plus the new user we just created
        assertEquals(users, conn.securityOperations().listLocalUsers());
        return null;
      }

    });
  }

  @Test
  public void testUserPrivilegesThroughGrant() throws Exception {
    String user1 = testName.getMethodName();
    final File user1Keytab = new File(kdc.getKeytabDir(), user1 + ".keytab");
    if (user1Keytab.exists() && !user1Keytab.delete()) {
      log.warn("Unable to delete {}", user1Keytab);
    }

    // Create some new users
    kdc.createPrincipal(user1Keytab, user1);

    final String qualifiedUser1 = kdc.qualifyUser(user1);

    // Log in as user1
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user1, user1Keytab.getAbsolutePath());
    log.info("Logged in as {}", user1);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        // Indirectly creates this user when we use it
        Connector conn = mac.getConnector(qualifiedUser1, new KerberosToken());
        log.info("Created connector as {}", qualifiedUser1);

        // The new user should have no system permissions
        for (SystemPermission perm : SystemPermission.values()) {
          assertFalse(conn.securityOperations().hasSystemPermission(qualifiedUser1, perm));
        }

        return null;
      }
    });

    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
        conn.securityOperations().grantSystemPermission(qualifiedUser1, SystemPermission.CREATE_TABLE);
        return null;
      }
    });

    // Switch back to the original user
    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user1, user1Keytab.getAbsolutePath());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connector conn = mac.getConnector(qualifiedUser1, new KerberosToken());

        // Shouldn't throw an exception since we granted the create table permission
        final String table = testName.getMethodName() + "_user_table";
        conn.tableOperations().create(table);

        // Make sure we can actually use the table we made
        BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
        Mutation m = new Mutation("a");
        m.put("b", "c", "d");
        bw.addMutation(m);
        bw.close();

        conn.tableOperations().compact(table, new CompactionConfig().setWait(true).setFlush(true));
        return null;
      }
    });
  }

  @Test
  public void testUserPrivilegesForTable() throws Exception {
    String user1 = testName.getMethodName();
    final File user1Keytab = new File(kdc.getKeytabDir(), user1 + ".keytab");
    if (user1Keytab.exists() && !user1Keytab.delete()) {
      log.warn("Unable to delete {}", user1Keytab);
    }

    // Create some new users -- cannot contain realm
    kdc.createPrincipal(user1Keytab, user1);

    final String qualifiedUser1 = kdc.qualifyUser(user1);

    // Log in as user1
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(qualifiedUser1, user1Keytab.getAbsolutePath());
    log.info("Logged in as {}", user1);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        // Indirectly creates this user when we use it
        Connector conn = mac.getConnector(qualifiedUser1, new KerberosToken());
        log.info("Created connector as {}", qualifiedUser1);

        // The new user should have no system permissions
        for (SystemPermission perm : SystemPermission.values()) {
          assertFalse(conn.securityOperations().hasSystemPermission(qualifiedUser1, perm));
        }
        return null;
      }

    });

    final String table = testName.getMethodName() + "_user_table";
    final String viz = "viz";

    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());

    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
        conn.tableOperations().create(table);
        // Give our unprivileged user permission on the table we made for them
        conn.securityOperations().grantTablePermission(qualifiedUser1, table, TablePermission.READ);
        conn.securityOperations().grantTablePermission(qualifiedUser1, table, TablePermission.WRITE);
        conn.securityOperations().grantTablePermission(qualifiedUser1, table, TablePermission.ALTER_TABLE);
        conn.securityOperations().grantTablePermission(qualifiedUser1, table, TablePermission.DROP_TABLE);
        conn.securityOperations().changeUserAuthorizations(qualifiedUser1, new Authorizations(viz));
        return null;
      }
    });

    // Switch back to the original user
    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(qualifiedUser1, user1Keytab.getAbsolutePath());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connector conn = mac.getConnector(qualifiedUser1, new KerberosToken());

        // Make sure we can actually use the table we made

        // Write data
        final long ts = 1000l;
        BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
        Mutation m = new Mutation("a");
        m.put("b", "c", new ColumnVisibility(viz.getBytes()), ts, "d");
        bw.addMutation(m);
        bw.close();

        // Compact
        conn.tableOperations().compact(table, new CompactionConfig().setWait(true).setFlush(true));

        // Alter
        conn.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "true");

        // Read (and proper authorizations)
        Scanner s = conn.createScanner(table, new Authorizations(viz));
        Iterator<Entry<Key,Value>> iter = s.iterator();
        assertTrue("No results from iterator", iter.hasNext());
        Entry<Key,Value> entry = iter.next();
        assertEquals(new Key("a", "b", "c", viz, ts), entry.getKey());
        assertEquals(new Value("d".getBytes()), entry.getValue());
        assertFalse("Had more results from iterator", iter.hasNext());
        return null;
      }
    });
  }

  @Test
  public void testDelegationToken() throws Exception {
    final String tableName = getUniqueNames(1)[0];

    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    final int numRows = 100, numColumns = 10;

    // As the "root" user, open up the connection and get a delegation token
    final AuthenticationToken delegationToken = root.doAs(new PrivilegedExceptionAction<AuthenticationToken>() {
      @Override
      public AuthenticationToken run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
        log.info("Created connector as {}", rootUser.getPrincipal());
        assertEquals(rootUser.getPrincipal(), conn.whoami());

        conn.tableOperations().create(tableName);
        BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
        for (int r = 0; r < numRows; r++) {
          Mutation m = new Mutation(Integer.toString(r));
          for (int c = 0; c < numColumns; c++) {
            String col = Integer.toString(c);
            m.put(col, col, col);
          }
          bw.addMutation(m);
        }
        bw.close();

        return conn.securityOperations().getDelegationToken(new DelegationTokenConfig());
      }
    });

    // The above login with keytab doesn't have a way to logout, so make a fake user that won't have krb credentials
    UserGroupInformation userWithoutPrivs = UserGroupInformation.createUserForTesting("fake_user", new String[0]);
    int recordsSeen = userWithoutPrivs.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), delegationToken);

        BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
        bs.setRanges(Collections.singleton(new Range()));
        int recordsSeen = Iterables.size(bs);
        bs.close();
        return recordsSeen;
      }
    });

    assertEquals(numRows * numColumns, recordsSeen);
  }

  @Test
  public void testDelegationTokenAsDifferentUser() throws Exception {
    // Login as the "root" user
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    final AuthenticationToken delegationToken;
    try {
      delegationToken = ugi.doAs(new PrivilegedExceptionAction<AuthenticationToken>() {
        @Override
        public AuthenticationToken run() throws Exception {
          // As the "root" user, open up the connection and get a delegation token
          Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
          log.info("Created connector as {}", rootUser.getPrincipal());
          assertEquals(rootUser.getPrincipal(), conn.whoami());
          return conn.securityOperations().getDelegationToken(new DelegationTokenConfig());
        }
      });
    } catch (UndeclaredThrowableException ex) {
      throw ex;
    }

    // make a fake user that won't have krb credentials
    UserGroupInformation userWithoutPrivs = UserGroupInformation.createUserForTesting("fake_user", new String[0]);
    try {
      // Use the delegation token to try to log in as a different user
      userWithoutPrivs.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          mac.getConnector("some_other_user", delegationToken);
          return null;
        }
      });
      fail("Using a delegation token as a different user should throw an exception");
    } catch (UndeclaredThrowableException e) {
      Throwable cause = e.getCause();
      assertNotNull(cause);
      // We should get an AccumuloSecurityException from trying to use a delegation token for the wrong user
      assertTrue("Expected cause to be AccumuloSecurityException, but was " + cause.getClass(), cause instanceof AccumuloSecurityException);
    }
  }

  @Test
  public void testGetDelegationTokenDenied() throws Exception {
    String newUser = testName.getMethodName();
    final File newUserKeytab = new File(kdc.getKeytabDir(), newUser + ".keytab");
    if (newUserKeytab.exists() && !newUserKeytab.delete()) {
      log.warn("Unable to delete {}", newUserKeytab);
    }

    // Create a new user
    kdc.createPrincipal(newUserKeytab, newUser);

    final String qualifiedNewUser = kdc.qualifyUser(newUser);

    // Login as a normal user
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(qualifiedNewUser, newUserKeytab.getAbsolutePath());
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          // As the "root" user, open up the connection and get a delegation token
          Connector conn = mac.getConnector(qualifiedNewUser, new KerberosToken());
          log.info("Created connector as {}", qualifiedNewUser);
          assertEquals(qualifiedNewUser, conn.whoami());

          conn.securityOperations().getDelegationToken(new DelegationTokenConfig());
          return null;
        }
      });
    } catch (UndeclaredThrowableException ex) {
      assertTrue(ex.getCause() instanceof AccumuloSecurityException);
    }
  }

  @Test
  public void testRestartedMasterReusesSecretKey() throws Exception {
    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    // As the "root" user, open up the connection and get a delegation token
    final AuthenticationToken delegationToken1 = root.doAs(new PrivilegedExceptionAction<AuthenticationToken>() {
      @Override
      public AuthenticationToken run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
        log.info("Created connector as {}", rootUser.getPrincipal());
        assertEquals(rootUser.getPrincipal(), conn.whoami());

        AuthenticationToken token = conn.securityOperations().getDelegationToken(new DelegationTokenConfig());

        assertTrue("Could not get tables with delegation token", mac.getConnector(rootUser.getPrincipal(), token).tableOperations().list().size() > 0);

        return token;
      }
    });

    log.info("Stopping master");
    mac.getClusterControl().stop(ServerType.MASTER);
    Thread.sleep(5000);
    log.info("Restarting master");
    mac.getClusterControl().start(ServerType.MASTER);

    // Make sure our original token is still good
    root.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), delegationToken1);

        assertTrue("Could not get tables with delegation token", conn.tableOperations().list().size() > 0);

        return null;
      }
    });

    // Get a new token, so we can compare the keyId on the second to the first
    final AuthenticationToken delegationToken2 = root.doAs(new PrivilegedExceptionAction<AuthenticationToken>() {
      @Override
      public AuthenticationToken run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
        log.info("Created connector as {}", rootUser.getPrincipal());
        assertEquals(rootUser.getPrincipal(), conn.whoami());

        AuthenticationToken token = conn.securityOperations().getDelegationToken(new DelegationTokenConfig());

        assertTrue("Could not get tables with delegation token", mac.getConnector(rootUser.getPrincipal(), token).tableOperations().list().size() > 0);

        return token;
      }
    });

    // A restarted master should reuse the same secret key after a restart if the secret key hasn't expired (1day by default)
    DelegationTokenImpl dt1 = (DelegationTokenImpl) delegationToken1;
    DelegationTokenImpl dt2 = (DelegationTokenImpl) delegationToken2;
    assertEquals(dt1.getIdentifier().getKeyId(), dt2.getIdentifier().getKeyId());
  }

  @Test(expected = AccumuloException.class)
  public void testDelegationTokenWithInvalidLifetime() throws Throwable {
    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    // As the "root" user, open up the connection and get a delegation token
    try {
      root.doAs(new PrivilegedExceptionAction<AuthenticationToken>() {
        @Override
        public AuthenticationToken run() throws Exception {
          Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
          log.info("Created connector as {}", rootUser.getPrincipal());
          assertEquals(rootUser.getPrincipal(), conn.whoami());

          // Should fail
          return conn.securityOperations().getDelegationToken(new DelegationTokenConfig().setTokenLifetime(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
        }
      });
    } catch (UndeclaredThrowableException e) {
      Throwable cause = e.getCause();
      if (null != cause) {
        throw cause;
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testDelegationTokenWithReducedLifetime() throws Throwable {
    // Login as the "root" user
    UserGroupInformation root = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    // As the "root" user, open up the connection and get a delegation token
    final AuthenticationToken dt = root.doAs(new PrivilegedExceptionAction<AuthenticationToken>() {
      @Override
      public AuthenticationToken run() throws Exception {
        Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
        log.info("Created connector as {}", rootUser.getPrincipal());
        assertEquals(rootUser.getPrincipal(), conn.whoami());

        return conn.securityOperations().getDelegationToken(new DelegationTokenConfig().setTokenLifetime(5, TimeUnit.MINUTES));
      }
    });

    AuthenticationTokenIdentifier identifier = ((DelegationTokenImpl) dt).getIdentifier();
    assertTrue("Expected identifier to expire in no more than 5 minutes: " + identifier,
        identifier.getExpirationDate() - identifier.getIssueDate() <= (5 * 60 * 1000));
  }

  @Test(expected = AccumuloSecurityException.class)
  public void testRootUserHasIrrevocablePermissions() throws Exception {
    // Login as the client (provided to `accumulo init` as the "root" user)
    UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());

    final Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());

    // The server-side implementation should prevent the revocation of the 'root' user's systems permissions
    // because once they're gone, it's possible that they could never be restored.
    conn.securityOperations().revokeSystemPermission(rootUser.getPrincipal(), SystemPermission.GRANT);
  }

  /**
   * Creates a table, adds a record to it, and then compacts the table. A simple way to make sure that the system user exists (since the master does an RPC to
   * the tserver which will create the system user if it doesn't already exist).
   */
  private void createTableWithDataAndCompact(Connector conn) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
    final String table = testName.getMethodName() + "_table";
    conn.tableOperations().create(table);
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    Mutation m = new Mutation("a");
    m.put("b", "c", "d");
    bw.addMutation(m);
    bw.close();
    conn.tableOperations().compact(table, new CompactionConfig().setFlush(true).setWait(true));
  }
}
