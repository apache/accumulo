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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start a new table, create many splits, and offline before they can rebalance. Then try to have a
 * different table balance
 */
public class BalanceInPresenceOfOfflineTableIT extends AccumuloClusterHarness {

  private static final Logger log =
      LoggerFactory.getLogger(BalanceInPresenceOfOfflineTableIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
    cfg.setSiteConfig(siteConfig);
    // ensure we have two tservers
    if (cfg.getNumTservers() < 2) {
      cfg.setNumTservers(2);
    }
  }

  private static final int NUM_SPLITS = 200;

  private String UNUSED_TABLE, TEST_TABLE;

  private AccumuloClient accumuloClient;

  @BeforeEach
  public void setupTables() throws AccumuloException, AccumuloSecurityException,
      TableExistsException, TableNotFoundException {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
    // Need at least two tservers -- wait for them to start before failing
    for (int retries = 0; retries < 5; ++retries) {
      if (accumuloClient.instanceOperations().getTabletServers().size() >= 2) {
        break;
      }
      UtilWaitThread.sleep(TimeUnit.SECONDS.toMillis(2));
    }
    assumeTrue(accumuloClient.instanceOperations().getTabletServers().size() >= 2,
        "Not enough tservers to run test");

    // set up splits
    final SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < NUM_SPLITS; i++) {
      splits.add(new Text(String.format("%08x", i * 1000)));
    }

    String[] names = getUniqueNames(2);
    UNUSED_TABLE = names[0];
    TEST_TABLE = names[1];

    // load into a table we won't use

    accumuloClient.tableOperations().create(UNUSED_TABLE);
    accumuloClient.tableOperations().addSplits(UNUSED_TABLE, splits);
    // mark the table offline before it can rebalance.
    accumuloClient.tableOperations().offline(UNUSED_TABLE);

    // actual test table
    accumuloClient.tableOperations().create(TEST_TABLE);
    accumuloClient.tableOperations().setProperty(TEST_TABLE,
        Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
  }

  @AfterEach
  public void closeClient() {
    accumuloClient.close();
  }

  @Test
  public void test() throws Exception {
    log.info("Test that balancing is not stopped by an offline table with outstanding migrations.");

    log.debug("starting test ingestion");

    VerifyParams params = new VerifyParams(getClientProps(), TEST_TABLE, 200_000);
    TestIngest.ingest(accumuloClient, params);
    accumuloClient.tableOperations().flush(TEST_TABLE, null, null, true);
    VerifyIngest.verifyIngest(accumuloClient, params);

    log.debug("waiting for balancing, up to ~5 minutes to allow for migration cleanup.");
    final long startTime = System.currentTimeMillis();
    long currentWait = 10_000;
    boolean balancingWorked = false;

    Credentials creds = new Credentials(getAdminPrincipal(), getAdminToken());
    while (!balancingWorked && (System.currentTimeMillis() - startTime) < ((5 * 60 + 15) * 1000)) {
      Thread.sleep(currentWait);
      currentWait *= 2;

      log.debug("fetch the list of tablets assigned to each tserver.");

      ManagerMonitorInfo stats = ThriftClientTypes.MANAGER.execute((ClientContext) accumuloClient,
          client -> client.getManagerStats(TraceUtil.traceInfo(),
              creds.toThrift(accumuloClient.instanceOperations().getInstanceId())));

      if (stats.getTServerInfoSize() < 2) {
        log.debug("we need >= 2 servers. sleeping for {}ms", currentWait);
        continue;
      }
      if (stats.getUnassignedTablets() != 0) {
        log.debug("We shouldn't have unassigned tablets. sleeping for {}ms", currentWait);
        continue;
      }

      long[] tabletsPerServer = new long[stats.getTServerInfoSize()];
      Arrays.fill(tabletsPerServer, 0L);
      for (int i = 0; i < stats.getTServerInfoSize(); i++) {
        for (Map.Entry<String,TableInfo> entry : stats.getTServerInfo().get(i).getTableMap()
            .entrySet()) {
          tabletsPerServer[i] += entry.getValue().getTablets();
        }
      }

      if (tabletsPerServer[0] <= 10) {
        log.debug("We should have > 10 tablets. sleeping for {}ms", currentWait);
        continue;
      }
      long min = NumberUtils.min(tabletsPerServer), max = NumberUtils.max(tabletsPerServer);
      log.debug("Min={}, Max={}", min, max);
      if ((min / ((double) max)) < 0.5) {
        log.debug(
            "ratio of min to max tablets per server should be roughly even. sleeping for {}ms",
            currentWait);
        continue;
      }
      balancingWorked = true;
    }

    assertTrue(balancingWorked, "did not properly balance");
  }

}
