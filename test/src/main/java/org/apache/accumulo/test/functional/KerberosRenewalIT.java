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

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.util.Wait;
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
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MAC test which uses {@link MiniKdc} to simulate ta secure environment. Can be used as a sanity
 * check for Kerberos/SASL testing.
 */
@Tag(MINI_CLUSTER_ONLY)
public class KerberosRenewalIT extends AccumuloITBase {
  private static final Logger log = LoggerFactory.getLogger(KerberosRenewalIT.class);

  private static TestingKdc kdc;
  private static String krbEnabledForITs = null;
  private static ClusterUser rootUser;

  private static final long TICKET_LIFETIME = MINUTES.toMillis(6); // Anything less seems to fail
                                                                   // when generating the ticket
  private static final long TICKET_TEST_LIFETIME = MINUTES.toMillis(8); // Run a test for 8 mins

  public static final int TEST_DURATION_MINUTES = 9; // The test should finish within 9 mins

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(TEST_DURATION_MINUTES);
  }

  @BeforeAll
  public static void startKdc() throws Exception {
    // 30s renewal time window
    kdc =
        new TestingKdc(TestingKdc.computeKdcDir(), TestingKdc.computeKeytabDir(), TICKET_LIFETIME);
    kdc.start();
    krbEnabledForITs = System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION);
    if (!Boolean.parseBoolean(krbEnabledForITs)) {
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
  }

  private MiniAccumuloClusterImpl mac;

  @BeforeEach
  public void startMac() throws Exception {
    MiniClusterHarness harness = new MiniClusterHarness();
    mac = harness.create(this, new PasswordToken("unused"), kdc, (cfg, coreSite) -> {
      Map<String,String> site = cfg.getSiteConfig();
      site.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
      // Reduce the period just to make sure we trigger renewal fast
      site.put(Property.GENERAL_KERBEROS_RENEWAL_PERIOD.getKey(), "5s");
      cfg.setSiteConfig(site);
      cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
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

  // Intentionally setting the Test annotation timeout. We do not want to scale the timeout.
  @Test
  @Timeout(value = TEST_DURATION_MINUTES, unit = MINUTES)
  public void testReadAndWriteThroughTicketLifetime() throws Exception {
    // Attempt to use Accumulo for a duration of time that exceeds the Kerberos ticket lifetime.
    // This is a functional test to verify that Accumulo services renew their ticket.
    // If the test doesn't finish on its own, this signifies that Accumulo services failed
    // and the test should fail. If Accumulo services renew their ticket, the test case
    // should exit gracefully on its own.

    // Login as the "root" user
    UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(),
        rootUser.getKeytab().getAbsolutePath());
    log.info("Logged in as {}", rootUser.getPrincipal());

    try (var client = mac.createAccumuloClient(rootUser.getPrincipal(), new KerberosToken())) {
      log.info("Created client as {}", rootUser.getPrincipal());
      assertEquals(rootUser.getPrincipal(), client.whoami());

      final String tableName = getUniqueNames(1)[0] + "_table";
      long endTime = System.currentTimeMillis() + TICKET_TEST_LIFETIME;

      // Make sure we have a couple renewals happen
      while (System.currentTimeMillis() < endTime) {
        // Create a table, write a record, compact, read the record, drop the table.
        createReadWriteDrop(client, tableName);
        // Wait a bit after
        Thread.sleep(5000L);
      }
    }
  }

  /**
   * Creates a table, adds a record to it, and then compacts the table. A simple way to make sure
   * that the system user exists (since the manager does an RPC to the tserver which will create the
   * system user if it doesn't already exist).
   */
  private void createReadWriteDrop(AccumuloClient client, String tableName) throws Exception {
    client.tableOperations().create(tableName);
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("a");
      m.put("b", "c", "d");
      bw.addMutation(m);
    }
    client.tableOperations().compact(tableName,
        new CompactionConfig().setFlush(true).setWait(true));
    try (Scanner s = client.createScanner(tableName, Authorizations.EMPTY)) {
      Entry<Key,Value> entry = getOnlyElement(s);
      assertEquals(0,
          new Key("a", "b", "c").compareTo(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL),
          "Did not find the expected key");
      assertEquals("d", entry.getValue().toString());
    }
    client.tableOperations().delete(tableName);
    Wait.waitFor(() -> !client.tableOperations().exists(tableName), 20_000L, 200L);
  }
}
