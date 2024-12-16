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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalCompactionSslIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(ExternalCompactionSslIT.class);

  public static class ExternalCompactionSslConfig implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      Map<String,String> site = cfg.getSiteConfig();
      site.put(Property.INSTANCE_RPC_SSL_CLIENT_AUTH.getKey(), "true");
      cfg.setSiteConfig(site);
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite, cfg.getClientProps());
    }
  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    System.setProperty(MiniClusterHarness.USE_SSL_FOR_IT_OPTION, Boolean.TRUE.toString());
    startMiniClusterWithConfig(new ExternalCompactionSslConfig());
  }

  @AfterAll
  public static void after() throws Exception {
    stopMiniCluster();
  }

  @Test
  public void testExternalCompaction() throws Exception {
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String table1 = names[0];
      createTable(client, table1, "cs1");

      String table2 = names[1];
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      compact(client, table1, 2, GROUP1, true);
      verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      compact(client, table2, 3, GROUP2, true);
      verify(client, table2, 3);

    }
  }

}
