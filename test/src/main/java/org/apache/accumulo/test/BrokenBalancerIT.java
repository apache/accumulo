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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.SimpleLoadBalancer;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokenBalancerIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(BrokenBalancerIT.class);

  public static class BrokenBalancer extends SimpleLoadBalancer {
    public BrokenBalancer() {
      super();
    }

    public BrokenBalancer(TableId tableId) {
      super(tableId);
    }

    @Override
    public void init(BalancerEnvironment balancerEnvironment) {
      throw new IllegalStateException();
    }
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL.getKey(), "3s");
    cfg.setSiteConfig(siteConfig);
    // ensure we have two tservers
    if (cfg.getClusterServerConfiguration().getTabletServerConfiguration()
        .get(Constants.DEFAULT_RESOURCE_GROUP_NAME) != 2) {
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(2);
    }
  }

  @Test
  public void testBalancerException() throws Exception {
    String tableName = getUniqueNames(1)[0];
    testBadBalancer(BrokenBalancer.class.getName(), tableName);
  }

  @Test
  public void testBalancerNotFound() throws Exception {
    String tableName = getUniqueNames(1)[0];
    testBadBalancer("org.apache.accumulo.abc.NonExistentBalancer", tableName);
  }

  private void testBadBalancer(String balancerClass, String tableName) throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < 10; i++) {
        splits.add(new Text("" + i));
      }
      var props = Map.of(Property.TABLE_LOAD_BALANCER.getKey(), balancerClass);
      NewTableConfiguration ntc =
          new NewTableConfiguration().withSplits(splits).setProperties(props);
      c.tableOperations().create(tableName, ntc);

      assertEquals(Map.of(" none", 11), BalanceIT.countLocations(c, tableName));
      UtilWaitThread.sleep(5000);
      // scan should not be able to complete because the tablet should not be assigned
      assertEquals(Map.of(" none", 11), BalanceIT.countLocations(c, tableName));

      // fix the balancer config
      log.info("fixing per tablet balancer");
      c.tableOperations().setProperty(tableName, Property.TABLE_LOAD_BALANCER.getKey(),
          SimpleLoadBalancer.class.getName());

      Wait.waitFor(() -> 2 == BalanceIT.countLocations(c, tableName).size());

      // break the balancer at the system level
      log.info("breaking manager balancer");
      c.instanceOperations().setProperty(Property.MANAGER_TABLET_BALANCER.getKey(), balancerClass);

      // add some tablet servers
      assertEquals(2, getCluster().getConfig().getClusterServerConfiguration()
          .getTabletServerConfiguration().get(Constants.DEFAULT_RESOURCE_GROUP_NAME));
      getCluster().getConfig().getClusterServerConfiguration().setNumDefaultTabletServers(5);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

      UtilWaitThread.sleep(5000);

      // should not have balanced across the two new tservers
      assertEquals(2, BalanceIT.countLocations(c, tableName).size());

      // fix the system level balancer
      log.info("fixing manager balancer");
      c.instanceOperations().setProperty(Property.MANAGER_TABLET_BALANCER.getKey(),
          TableLoadBalancer.class.getName());

      // should eventually balance across all 5 tabletsevers
      Wait.waitFor(() -> 5 == BalanceIT.countLocations(c, tableName).size());
    }
  }
}
