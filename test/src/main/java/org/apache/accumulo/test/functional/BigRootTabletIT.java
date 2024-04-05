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

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
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
    // Map<String,String> siteConfig = cfg.getSiteConfig();
    // siteConfig.put(Property.TABLE_SCAN_MAXMEM.getKey(), "1024");
    // cfg.setSiteConfig(siteConfig);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      c.tableOperations().setProperty(AccumuloTable.ROOT.tableName(),
          Property.TABLE_SCAN_MAXMEM.getKey(), "1024");

      c.tableOperations().addSplits(AccumuloTable.METADATA.tableName(),
          FunctionalTestUtils.splits("0 1 2 3 4 5 6 7 8 9 a".split(" ")));

      c.instanceOperations().waitForBalance();

      try (Scanner s = c.createScanner(AccumuloTable.ROOT.tableName())) {
        s.forEach((k, v) -> System.out.println(k + " -> " + v));
      }

      // Wait for the metadata table to split and for all tablets to be hosted
      Ample ample = ((ClientContext) c).getAmple();
      TableId metadataTableId = AccumuloTable.METADATA.tableId();

      long count = ample.readTablets().forTable(metadataTableId).build().stream().count();
      while (count != 13) {
        count = ample.readTablets().forTable(metadataTableId).build().stream().count();
        Thread.sleep(100);
      }

      count = ample.readTablets().forTable(metadataTableId).fetch(ColumnType.LOCATION).build()
          .stream().filter(tm -> tm.getLocation() != null).count();
      while (count != 13) {
        count = ample.readTablets().forTable(metadataTableId).fetch(ColumnType.LOCATION).build()
            .stream().filter(tm -> tm.getLocation() != null).count();
        Thread.sleep(100);
      }

      String[] names = getUniqueNames(10);
      for (String name : names) {
        c.tableOperations().create(name);
        c.tableOperations().flush(AccumuloTable.METADATA.tableName(), null, null, true);
        c.tableOperations().flush(AccumuloTable.ROOT.tableName(), null, null, true);
      }
      cluster.stop();
      cluster.start();
      assertTrue(c.createScanner(AccumuloTable.ROOT.tableName(), Authorizations.EMPTY).stream()
          .findAny().isPresent());
    }
  }

}
