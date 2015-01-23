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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloIT;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * MAC test which uses {@link MiniKdc} to simulate ta secure environment. Can be used as a sanity check for Kerberos/SASL testing.
 */
public class KerberosIT extends AccumuloIT {
  private static final Logger log = LoggerFactory.getLogger(KerberosIT.class);

  private static TestingKdc kdc;
  private static String krbEnabledForITs = null;

  @BeforeClass
  public static void startKdc() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
    krbEnabledForITs = System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION);
    if (null == krbEnabledForITs || !Boolean.parseBoolean(krbEnabledForITs)) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    }
  }

  @AfterClass
  public static void stopKdc() throws Exception {
    if (null != kdc) {
      kdc.stop();
    }
    if (null != krbEnabledForITs) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, krbEnabledForITs);
    }
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60 * 5;
  }

  private MiniAccumuloClusterImpl mac;

  @Before
  public void startMac() throws Exception {
    MiniClusterHarness harness = new MiniClusterHarness();
    mac = harness.create(this, new PasswordToken("unused"), kdc);
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
    UserGroupInformation.loginUserFromKeytab(kdc.getClientPrincipal(), kdc.getClientKeytab().getAbsolutePath());

    final Connector conn = mac.getConnector(kdc.getClientPrincipal(), new KerberosToken());

    // The "root" user should have all system permissions
    for (SystemPermission perm : SystemPermission.values()) {
      assertTrue("Expected user to have permission: " + perm, conn.securityOperations().hasSystemPermission(conn.whoami(), perm));
    }

    // and the ability to modify the root and metadata tables
    for (String table : Arrays.asList(RootTable.NAME, MetadataTable.NAME)){
      assertTrue(conn.securityOperations().hasTablePermission(conn.whoami(), table, TablePermission.ALTER_TABLE));
    }
  }

  @Test
  public void testNewUser() throws Exception {
    String newUser = testName.getMethodName();
    final File newUserKeytab = new File(kdc.getKeytabDir(), newUser + ".keytab");
    if (newUserKeytab.exists()) {
      newUserKeytab.delete();
    }

    // Create a new user
    kdc.createPrincipal(newUserKeytab, newUser);

    newUser = kdc.qualifyUser(newUser);

    // Login as the "root" user
    UserGroupInformation.loginUserFromKeytab(kdc.getClientPrincipal(), kdc.getClientKeytab().getAbsolutePath());
    log.info("Logged in as {}", kdc.getClientPrincipal());

    Connector conn = mac.getConnector(kdc.getClientPrincipal(), new KerberosToken());
    log.info("Created connector as {}", kdc.getClientPrincipal());
    assertEquals(kdc.getClientPrincipal(), conn.whoami());

    // Make sure the system user doesn't exist -- this will force some RPC to happen server-side
    createTableWithDataAndCompact(conn);

    HashSet<String> users = Sets.newHashSet(kdc.getClientPrincipal());
    assertEquals(users, conn.securityOperations().listLocalUsers());

    // Switch to a new user
    UserGroupInformation.loginUserFromKeytab(newUser, newUserKeytab.getAbsolutePath());
    log.info("Logged in as {}", newUser);

    conn = mac.getConnector(newUser, new KerberosToken());
    log.info("Created connector as {}", newUser);
    assertEquals(newUser, conn.whoami());

    // The new user should have no system permissions
    for (SystemPermission perm : SystemPermission.values()) {
      assertFalse(conn.securityOperations().hasSystemPermission(newUser, perm));
    }

    users.add(newUser);

    // Same users as before, plus the new user we just created
    assertEquals(users, conn.securityOperations().listLocalUsers());
  }

  @Test
  public void testUserPrivilegesThroughGrant() throws Exception {
    String user1 = testName.getMethodName();
    final File user1Keytab = new File(kdc.getKeytabDir(), user1 + ".keytab");
    if (user1Keytab.exists()) {
      user1Keytab.delete();
    }

    // Create some new users
    kdc.createPrincipal(user1Keytab, user1);

    user1 = kdc.qualifyUser(user1);

    // Log in as user1
    UserGroupInformation.loginUserFromKeytab(user1, user1Keytab.getAbsolutePath());
    log.info("Logged in as {}", user1);

    // Indirectly creates this user when we use it
    Connector conn = mac.getConnector(user1, new KerberosToken());
    log.info("Created connector as {}", user1);

    // The new user should have no system permissions
    for (SystemPermission perm : SystemPermission.values()) {
      assertFalse(conn.securityOperations().hasSystemPermission(user1, perm));
    }

    UserGroupInformation.loginUserFromKeytab(kdc.getClientPrincipal(), kdc.getClientKeytab().getAbsolutePath());
    conn = mac.getConnector(kdc.getClientPrincipal(), new KerberosToken());

    conn.securityOperations().grantSystemPermission(user1, SystemPermission.CREATE_TABLE);

    // Switch back to the original user
    UserGroupInformation.loginUserFromKeytab(user1, user1Keytab.getAbsolutePath());
    conn = mac.getConnector(user1, new KerberosToken());

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
  }

  @Test
  public void testUserPrivilegesForTable() throws Exception {
    String user1 = testName.getMethodName();
    final File user1Keytab = new File(kdc.getKeytabDir(), user1 + ".keytab");
    if (user1Keytab.exists()) {
      user1Keytab.delete();
    }

    // Create some new users -- cannot contain realm
    kdc.createPrincipal(user1Keytab, user1);

    user1 = kdc.qualifyUser(user1);

    // Log in as user1
    UserGroupInformation.loginUserFromKeytab(user1, user1Keytab.getAbsolutePath());
    log.info("Logged in as {}", user1);

    // Indirectly creates this user when we use it
    Connector conn = mac.getConnector(user1, new KerberosToken());
    log.info("Created connector as {}", user1);

    // The new user should have no system permissions
    for (SystemPermission perm : SystemPermission.values()) {
      assertFalse(conn.securityOperations().hasSystemPermission(user1, perm));
    }

    UserGroupInformation.loginUserFromKeytab(kdc.getClientPrincipal(), kdc.getClientKeytab().getAbsolutePath());
    conn = mac.getConnector(kdc.getClientPrincipal(), new KerberosToken());

    final String table = testName.getMethodName() + "_user_table";
    conn.tableOperations().create(table);

    final String viz = "viz";

    // Give our unprivileged user permission on the table we made for them
    conn.securityOperations().grantTablePermission(user1, table, TablePermission.READ);
    conn.securityOperations().grantTablePermission(user1, table, TablePermission.WRITE);
    conn.securityOperations().grantTablePermission(user1, table, TablePermission.ALTER_TABLE);
    conn.securityOperations().grantTablePermission(user1, table, TablePermission.DROP_TABLE);
    conn.securityOperations().changeUserAuthorizations(user1, new Authorizations(viz));

    // Switch back to the original user
    UserGroupInformation.loginUserFromKeytab(user1, user1Keytab.getAbsolutePath());
    conn = mac.getConnector(user1, new KerberosToken());

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
