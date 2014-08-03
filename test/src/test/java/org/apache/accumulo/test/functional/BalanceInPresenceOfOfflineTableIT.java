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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start a new table, create many splits, and offline before they can rebalance. Then try to have a different table balance
 */
public class BalanceInPresenceOfOfflineTableIT extends ConfigurableMacIT {

  private static Logger log = LoggerFactory.getLogger(BalanceInPresenceOfOfflineTableIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String, String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    cfg.setSiteConfig(siteConfig );
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
  private static final String UNUSED_TABLE = "unused";
  private static final String TEST_TABLE = "test_ingest";

  private Connector connector;

  @Before
  public void setupTables() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    // set up splits
    final SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < NUM_SPLITS; i++) {
      splits.add(new Text(String.format("%08x", i * 1000)));
    }
    // load into a table we won't use
    connector = getConnector();
    connector.tableOperations().create(UNUSED_TABLE);
    connector.tableOperations().addSplits(UNUSED_TABLE, splits);
    // mark the table offline before it can rebalance.
    connector.tableOperations().offline(UNUSED_TABLE);

    // actual test table
    connector.tableOperations().create(TEST_TABLE);
    connector.tableOperations().setProperty(TEST_TABLE, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
  }

  @Test
  public void test() throws Exception {
    log.info("Test that balancing is not stopped by an offline table with outstanding migrations.");

    log.debug("starting test ingestion");

    TestIngest.Opts opts = new TestIngest.Opts();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = opts.rows = 200000;
    TestIngest.ingest(connector, opts, BWOPTS);
    connector.tableOperations().flush(TEST_TABLE, null, null, true);
    VerifyIngest.verifyIngest(connector, vopts, SOPTS);

    log.debug("waiting for balancing, up to ~5 minutes to allow for migration cleanup.");
    final long startTime = System.currentTimeMillis();
    long currentWait = 10*1000;
    boolean balancingWorked = false;

    while (!balancingWorked && (System.currentTimeMillis() - startTime) < ((5*60 + 15)*1000)) {
      Thread.sleep(currentWait);
      currentWait *= 2;

      log.debug("fetch the list of tablets assigned to each tserver.");
      MasterMonitorInfo stats = getCluster().getMasterMonitorInfo();

      if (stats.getTServerInfoSize() < 2) {
        log.debug("we need >= 2 servers. sleeping for " + currentWait + "ms");
        continue;
      }
      if (stats.getUnassignedTablets() != 0) {
        log.debug("We shouldn't have unassigned tablets. sleeping for " + currentWait + "ms");
        continue;
      }

      int numTablets = 0;
      long[] tabletsPerServer = new long[stats.getTServerInfoSize()];
      Arrays.fill(tabletsPerServer, 0l);
      for (int i = 0; i < stats.getTServerInfoSize(); i++) {
        for (Map.Entry<String, TableInfo> entry : stats.getTServerInfo().get(i).getTableMap().entrySet()) {
          tabletsPerServer[i] += entry.getValue().getTablets();
        }
      }

      if (tabletsPerServer[0] <= 10) {
        log.debug("We should have > 10 tablets. sleeping for " + currentWait + "ms");
        continue;
      }
      if ((NumberUtils.min(tabletsPerServer) / ((double)NumberUtils.max(tabletsPerServer))) < 0.5) {
        log.debug("ratio of min to max tablets per server should be roughly even. sleeping for " + currentWait + "ms");
        continue;
      }
      balancingWorked = true;
    }

    Assert.assertTrue("did not properly balance", balancingWorked);
  }

}
