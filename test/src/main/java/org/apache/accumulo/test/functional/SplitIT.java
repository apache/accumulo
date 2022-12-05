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

import static java.util.Collections.singletonMap;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.CheckForMetadataProblems;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(SplitIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAXMEM, "5K");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "100ms");
  }

  private String tservMaxMem, tservMajcDelay;

  @BeforeEach
  public void alterConfig() throws Exception {
    assumeTrue(getClusterType() == ClusterType.MINI);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();
      Map<String,String> config = iops.getSystemConfiguration();
      tservMaxMem = config.get(Property.TSERV_MAXMEM.getKey());
      tservMajcDelay = config.get(Property.TSERV_MAJC_DELAY.getKey());

      if (!tservMajcDelay.equals("100ms")) {
        iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), "100ms");
      }

      // Property.TSERV_MAXMEM can't be altered on a running server
      boolean restarted = false;
      if (!tservMaxMem.equals("5K")) {
        iops.setProperty(Property.TSERV_MAXMEM.getKey(), "5K");
        getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
        restarted = true;
      }

      // If we restarted the tservers, we don't need to re-wait for the majc delay
      if (!restarted) {
        long millis = ConfigurationTypeHelper.getTimeInMillis(tservMajcDelay);
        log.info("Waiting for majc delay period: {}ms", millis);
        Thread.sleep(millis);
        log.info("Finished waiting for majc delay period");
      }
    }
  }

  @AfterEach
  public void resetConfig() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      if (tservMaxMem != null) {
        log.info("Resetting {}={}", Property.TSERV_MAXMEM.getKey(), tservMaxMem);
        client.instanceOperations().setProperty(Property.TSERV_MAXMEM.getKey(), tservMaxMem);
        tservMaxMem = null;
        getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }
      if (tservMajcDelay != null) {
        log.info("Resetting {}={}", Property.TSERV_MAJC_DELAY.getKey(), tservMajcDelay);
        client.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), tservMajcDelay);
        tservMajcDelay = null;
      }
    }
  }

  @Test
  public void tabletShouldSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "256K");
      props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");

      c.tableOperations().create(table, new NewTableConfiguration().setProperties(props));
      VerifyParams params = new VerifyParams(getClientProps(), table, 100_000);
      TestIngest.ingest(c, params);
      VerifyIngest.verifyIngest(c, params);
      while (c.tableOperations().listSplits(table).size() < 10) {
        sleepUninterruptibly(15, TimeUnit.SECONDS);
      }
      TableId id = TableId.of(c.tableOperations().tableIdMap().get(table));
      try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        KeyExtent extent = new KeyExtent(id, null, null);
        s.setRange(extent.toMetaRange());
        TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
        int count = 0;
        int shortened = 0;
        for (Entry<Key,Value> entry : s) {
          extent = KeyExtent.fromMetaPrevRow(entry);
          if (extent.endRow() != null && extent.endRow().toString().length() < 14) {
            shortened++;
          }
          count++;
        }

        assertTrue(shortened > 0, "Shortened should be greater than zero: " + shortened);
        assertTrue(count > 10, "Count should be cgreater than 10: " + count);
      }

      assertEquals(0, getCluster().getClusterControl().exec(CheckForMetadataProblems.class,
          new String[] {"-c", cluster.getClientPropsPath()}));
    }
  }

  @Test
  public void interleaveSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
      props.put(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");

      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      sleepUninterruptibly(5, TimeUnit.SECONDS);
      ReadWriteIT.interleaveTest(c, tableName);
      sleepUninterruptibly(5, TimeUnit.SECONDS);
      int numSplits = c.tableOperations().listSplits(tableName).size();
      while (numSplits <= 20) {
        log.info("Waiting for splits to happen");
        Thread.sleep(2000);
        numSplits = c.tableOperations().listSplits(tableName).size();
      }
      assertTrue(numSplits > 20, "Expected at least 20 splits, saw " + numSplits);
    }
  }

  @Test
  public void deleteSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName, new NewTableConfiguration()
          .setProperties(singletonMap(Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K")));
      DeleteIT.deleteTest(c, getCluster(), tableName);
      c.tableOperations().flush(tableName, null, null, true);
      for (int i = 0; i < 5; i++) {
        sleepUninterruptibly(10, TimeUnit.SECONDS);
        if (c.tableOperations().listSplits(tableName).size() > 20) {
          break;
        }
      }
      assertTrue(c.tableOperations().listSplits(tableName).size() > 20);
    }
  }

}
