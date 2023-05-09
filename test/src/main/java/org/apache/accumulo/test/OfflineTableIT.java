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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class OfflineTableIT extends SharedMiniClusterBase {

  private static class OfflineTableITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    OfflineTableITConfiguration c = new OfflineTableITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testScanOffline() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      assertThrows(TableOfflineException.class,
          () -> client.createScanner(tableName, Authorizations.EMPTY));
    }
  }

  @Test
  public void testBatchScanOffline() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      assertThrows(TableOfflineException.class,
          () -> client.createBatchScanner(tableName, Authorizations.EMPTY));
    }
  }

  @Test
  public void testSplitOffline() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));
      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("m"));
      assertThrows(TableOfflineException.class, () -> {
        client.tableOperations().addSplits(tableName, splits);
      });
    }
  }

}
