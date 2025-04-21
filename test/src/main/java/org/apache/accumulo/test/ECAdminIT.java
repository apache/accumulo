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

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP7;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionMap;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ECAdmin;
import org.apache.accumulo.server.util.ECAdmin.RunningCompactionSummary;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class ECAdminIT extends SharedMiniClusterBase {

  // Class exists for access to protected methods
  private static class TestECAdmin extends ECAdmin {

    @Override
    protected void cancelCompaction(ServerContext context, String ecid) {
      super.cancelCompaction(context, ecid);
    }

    @Override
    protected void listCompactorsByQueue(ServerContext context) {
      super.listCompactorsByQueue(context);
    }

    @Override
    protected List<RunningCompactionSummary> runningCompactions(ServerContext context,
        boolean details) {
      return super.runningCompactions(context, details);
    }
  }

  private static final class ECAdminITConfig implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
      cfg.getClusterServerConfiguration().addCompactorResourceGroup(GROUP7, 1);
    }

  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new ECAdminITConfig());
  }

  @AfterAll
  public static void afterAll() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private final TestECAdmin eca = new TestECAdmin();

  @Test
  public void testListRunningCompactions() throws Exception {

    final String tableName = this.getUniqueNames(1)[0];

    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, tableName, "cs7");
      IteratorSetting setting = new IteratorSetting(50, "sleepy", SlowIterator.class);
      setting.addOption("sleepTime", "3000");
      setting.addOption("seekSleepTime", "3000");
      client.tableOperations().attachIterator(tableName, setting, EnumSet.of(IteratorScope.majc));
      writeData(client, tableName);
      compact(client, tableName, 2, GROUP7, false);

      Optional<HostAndPort> coordinatorHost =
          ExternalCompactionUtil.findCompactionCoordinator(getCluster().getServerContext());

      // wait for the compaction to start
      TExternalCompactionMap expected = ExternalCompactionTestUtils
          .getRunningCompactions(getCluster().getServerContext(), coordinatorHost);
      while (expected == null || expected.getCompactionsSize() == 0) {
        Thread.sleep(1000);
        expected = ExternalCompactionTestUtils
            .getRunningCompactions(getCluster().getServerContext(), coordinatorHost);
      }

      final List<RunningCompactionSummary> running =
          eca.runningCompactions(getCluster().getServerContext(), true);
      final Map<String,RunningCompactionSummary> compactionsByEcid = new HashMap<>();
      running.forEach(rcs -> compactionsByEcid.put(rcs.getEcid(), rcs));

      assertEquals(expected.getCompactionsSize(), compactionsByEcid.size());
      expected.getCompactions().values().forEach(tec -> {
        RunningCompactionSummary rcs = compactionsByEcid.get(tec.job.getExternalCompactionId());
        assertNotNull(rcs);
        assertEquals(tec.getJob().getExternalCompactionId(), rcs.getEcid());
        assertEquals(tec.groupName, rcs.getGroupName());
        assertEquals(tec.getCompactor(), rcs.getAddr());
      });

      // Confirm JSON output works
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      String json = gson.toJson(running);
      System.out.println(json);
      Type listType = new TypeToken<ArrayList<RunningCompactionSummary>>() {}.getType();
      @SuppressWarnings("unused")
      var unused = new GsonBuilder().create().fromJson(json, listType);
    }
  }

}
