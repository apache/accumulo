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
package org.apache.accumulo.test.functional;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertTrue;

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
import org.apache.accumulo.core.clientImpl.MasterClient;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start a new table, create many splits, and offline before they can rebalance. Then try to have a
 * different table balance
 */
public class BalanceInPresenceOfOfflineTableIT extends AccumuloClusterHarness {

  private static Logger log = LoggerFactory.getLogger(BalanceInPresenceOfOfflineTableIT.class);

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

  @Override
  protected int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  private static final int NUM_SPLITS = 200;

  private String UNUSED_TABLE, TEST_TABLE;

  private AccumuloClient accumuloClient;

  @Before
  public void setupTables() throws AccumuloException, AccumuloSecurityException,
      TableExistsException, TableNotFoundException {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
    // Need at least two tservers
    Assume.assumeTrue("Not enough tservers to run test",
        accumuloClient.instanceOperations().getTabletServers().size() >= 2);

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

  @After
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
    long currentWait = 10 * 1000;
    boolean balancingWorked = false;

    Credentials creds = new Credentials(getAdminPrincipal(), getAdminToken());
    while (!balancingWorked && (System.currentTimeMillis() - startTime) < ((5 * 60 + 15) * 1000)) {
      Thread.sleep(currentWait);
      currentWait *= 2;

      log.debug("fetch the list of tablets assigned to each tserver.");

      MasterClientService.Iface client = null;
      MasterMonitorInfo stats;
      while (true) {
        try {
          client = MasterClient.getConnectionWithRetry((ClientContext) accumuloClient);
          stats = client.getMasterStats(TraceUtil.traceInfo(),
              creds.toThrift(accumuloClient.instanceOperations().getInstanceID()));
          break;
        } catch (ThriftSecurityException exception) {
          throw new AccumuloSecurityException(exception);
        } catch (ThriftNotActiveServiceException e) {
          // Let it loop, fetching a new location
          log.debug("Contacted a Master which is no longer active, retrying");
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } catch (TException exception) {
          throw new AccumuloException(exception);
        } finally {
          if (client != null) {
            MasterClient.close(client);
          }
        }
      }

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

    assertTrue("did not properly balance", balancingWorked);
  }

}
