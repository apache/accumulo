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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
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
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
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

/**
 * MAC test which uses {@link MiniKdc} to simulate ta secure environment. Can be used as a sanity check for Kerberos/SASL testing.
 */
@Category(MiniClusterOnlyTests.class)
public class KerberosRenewalIT extends AccumuloITBase {
  private static final Logger log = LoggerFactory.getLogger(KerberosRenewalIT.class);

  private static TestingKdc kdc;
  private static String krbEnabledForITs = null;
  private static ClusterUser rootUser;

  private static final long TICKET_LIFETIME = 6 * 60 * 1000; // Anything less seems to fail when generating the ticket
  private static final long TICKET_TEST_LIFETIME = 8 * 60 * 1000; // Run a test for 8 mins
  private static final long TEST_DURATION = 9 * 60 * 1000; // The test should finish within 9 mins

  @BeforeClass
  public static void startKdc() throws Exception {
    // 30s renewal time window
    kdc = new TestingKdc(TestingKdc.computeKdcDir(), TestingKdc.computeKeytabDir(), TICKET_LIFETIME);
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
  }

  @Override
  public int defaultTimeoutSeconds() {
    return (int) TEST_DURATION / 1000;
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
        // Reduce the period just to make sure we trigger renewal fast
        site.put(Property.GENERAL_KERBEROS_RENEWAL_PERIOD.getKey(), "5s");
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

  // Intentially setting the Test annotation timeout. We do not want to scale the timeout.
  @Test(timeout = TEST_DURATION)
  public void testReadAndWriteThroughTicketLifetime() throws Exception {
    // Attempt to use Accumulo for a duration of time that exceeds the Kerberos ticket lifetime.
    // This is a functional test to verify that Accumulo services renew their ticket.
    // If the test doesn't finish on its own, this signifies that Accumulo services failed
    // and the test should fail. If Accumulo services renew their ticket, the test case
    // should exit gracefully on its own.

    // Login as the "root" user
    UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    Connector conn = mac.getConnector(rootUser.getPrincipal(), new KerberosToken());
    log.info("Created connector as {}", rootUser.getPrincipal());
    assertEquals(rootUser.getPrincipal(), conn.whoami());

    long duration = 0;
    long last = System.currentTimeMillis();
    // Make sure we have a couple renewals happen
    while (duration < TICKET_TEST_LIFETIME) {
      // Create a table, write a record, compact, read the record, drop the table.
      createReadWriteDrop(conn);
      // Wait a bit after
      Thread.sleep(5000);

      // Update the duration
      long now = System.currentTimeMillis();
      duration += now - last;
      last = now;
    }
  }

  /**
   * Creates a table, adds a record to it, and then compacts the table. A simple way to make sure that the system user exists (since the master does an RPC to
   * the tserver which will create the system user if it doesn't already exist).
   */
  private void createReadWriteDrop(Connector conn) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
    final String table = testName.getMethodName() + "_table";
    conn.tableOperations().create(table);
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    Mutation m = new Mutation("a");
    m.put("b", "c", "d");
    bw.addMutation(m);
    bw.close();
    conn.tableOperations().compact(table, new CompactionConfig().setFlush(true).setWait(true));
    try (Scanner s = conn.createScanner(table, Authorizations.EMPTY)) {
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      assertEquals("Did not find the expected key", 0, new Key("a", "b", "c").compareTo(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
      assertEquals("d", entry.getValue().toString());
      conn.tableOperations().delete(table);
    }
  }
}
