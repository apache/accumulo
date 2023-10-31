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

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BinaryStressIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_MAXMEM, "50K");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "50ms");
  }

  private String majcDelay, maxMem;

  @BeforeEach
  public void alterConfig() throws Exception {
    if (getClusterType() == ClusterType.MINI) {
      return;
    }
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();
      Map<String,String> conf = iops.getSystemConfiguration();
      majcDelay = conf.get(Property.TSERV_MAJC_DELAY.getKey());
      maxMem = conf.get(Property.TSERV_MAXMEM.getKey());

      iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
      iops.setProperty(Property.TSERV_MAXMEM.getKey(), "50K");

      getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    }
  }

  @AfterEach
  public void resetConfig() throws Exception {
    if (majcDelay != null) {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        InstanceOperations iops = client.instanceOperations();
        iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
        iops.setProperty(Property.TSERV_MAXMEM.getKey(), maxMem);
      }
      getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    }
  }

  @Test
  public void binaryStressTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
      // The call below to BinaryIT.runTest will insert enough data that the table will
      // eventually split based on the 10k split threshold set above. However, that may
      // not occur before the scanner below checks that there are 8 metadata tablets for
      // the table. This is because the TabletGroupWatcher runs every 5 seconds, so there
      // is a race condition that makes this test flaky. Run the scan in a Wait.waitFor
      // loop to give the Manager a chance to split the table.
      BinaryIT.runTest(c, tableName);
      String id = c.tableOperations().tableIdMap().get(tableName);
      Wait.waitFor(() -> getTabletCount(c, id) > 7, Wait.MAX_WAIT_MILLIS, Wait.SLEEP_MILLIS,
          "Expected at least 8 tablets");
    }
  }

  private int getTabletCount(AccumuloClient c, String tableId) throws TableNotFoundException {
    Set<Text> tablets = new HashSet<>();
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(Range.prefix(tableId));
      for (Entry<Key,Value> entry : s) {
        tablets.add(entry.getKey().getRow());
      }
    }
    return tablets.size();

  }

}
