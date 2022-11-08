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

import java.time.Duration;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class BigRootTabletIT extends AccumuloClusterHarness {
  // ACCUMULO-542: A large root tablet will fail to load if it does't fit in the tserver scan
  // buffers

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TABLE_SCAN_MAXMEM.getKey(), "1024");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "60m");
    cfg.setSiteConfig(siteConfig);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().addSplits(MetadataTable.NAME,
          FunctionalTestUtils.splits("0 1 2 3 4 5 6 7 8 9 a".split(" ")));
      String[] names = getUniqueNames(10);
      for (String name : names) {
        c.tableOperations().create(name);
        c.tableOperations().flush(MetadataTable.NAME, null, null, true);
        c.tableOperations().flush(RootTable.NAME, null, null, true);
      }
      cluster.stop();
      cluster.start();
      assertTrue(
          c.createScanner(RootTable.NAME, Authorizations.EMPTY).stream().findAny().isPresent());
    }
  }

}
