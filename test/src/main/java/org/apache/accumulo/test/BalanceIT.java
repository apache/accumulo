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
package org.apache.accumulo.test;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalanceIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(BalanceIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
    siteConfig.put(Property.GENERAL_MICROMETER_ENABLED.getKey(), "true");
    siteConfig.put("general.custom.metrics.opts.logging.step", "0.5s");
    cfg.setSiteConfig(siteConfig);
    // ensure we have two tservers
    if (cfg.getNumTservers() != 2) {
      cfg.setNumTservers(2);
    }
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testBalance() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      log.info("Creating table");
      c.tableOperations().create(tableName);
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < 10; i++) {
        splits.add(new Text("" + i));
      }
      log.info("Adding splits");
      c.tableOperations().addSplits(tableName, splits);
      log.info("Waiting for balance");
      c.instanceOperations().waitForBalance();
    }
  }

  @Test
  public void testBalanceMetadata() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < 10; i++) {
        splits.add(new Text("" + i));
      }
      c.tableOperations().create(tableName, new NewTableConfiguration().withSplits(splits));

      var metaSplits = IntStream.range(1, 100).mapToObj(i -> Integer.toString(i, 36)).map(Text::new)
          .collect(Collectors.toCollection(TreeSet::new));
      c.tableOperations().addSplits(MetadataTable.NAME, metaSplits);

      var locCounts = countLocations(c, MetadataTable.NAME);

      c.instanceOperations().waitForBalance();

      locCounts = countLocations(c, MetadataTable.NAME);
      var stats = locCounts.values().stream().mapToInt(i -> i).summaryStatistics();
      assertTrue(stats.getMax() <= 51, locCounts.toString());
      assertTrue(stats.getMin() >= 50, locCounts.toString());
      assertEquals(2, stats.getCount(), locCounts.toString());

      assertEquals(2, getCluster().getConfig().getNumTservers());
      getCluster().getConfig().setNumTservers(4);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

      Wait.waitFor(() -> {
        var lc = countLocations(c, MetadataTable.NAME);
        log.info("locations:{}", lc);
        return lc.size() == 4;
      });

      c.instanceOperations().waitForBalance();

      locCounts = countLocations(c, MetadataTable.NAME);
      stats = locCounts.values().stream().mapToInt(i -> i).summaryStatistics();
      assertTrue(stats.getMax() <= 26, locCounts.toString());
      assertTrue(stats.getMin() >= 25, locCounts.toString());
      assertEquals(4, stats.getCount(), locCounts.toString());

      // The user table should eventually balance
      Wait.waitFor(() -> {
        var lc = countLocations(c, tableName);
        log.info("locations:{}", lc);
        return lc.size() == 4;
      });

      locCounts = countLocations(c, tableName);
      stats = locCounts.values().stream().mapToInt(i -> i).summaryStatistics();
      assertTrue(stats.getMax() <= 3, locCounts.toString());
      assertTrue(stats.getMin() >= 2, locCounts.toString());
      assertEquals(4, stats.getCount(), locCounts.toString());
    }
  }

  static Map<String,Integer> countLocations(AccumuloClient client, String tableName)
      throws Exception {
    var ctx = ((ClientContext) client);
    var ample = ctx.getAmple();
    try (var tabletsMeta =
        ample.readTablets().forTable(ctx.getTableId(tableName)).fetch(LOCATION, PREV_ROW).build()) {
      Map<String,Integer> locCounts = new HashMap<>();
      for (var tabletMeta : tabletsMeta) {
        var loc = tabletMeta.getLocation();
        locCounts.merge(loc == null ? " none" : loc.toString(), 1, Integer::sum);
      }
      return locCounts;
    }
  }
}
