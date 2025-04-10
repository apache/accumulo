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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompactionBaseIT extends AccumuloClusterHarness {

  public static final String COMPACTOR_GROUP_1 = "cg1";
  public static final String COMPACTOR_GROUP_2 = "cg2";
  protected static final int MAX_DATA = 1000;
  private static final Logger log = LoggerFactory.getLogger(CompactionBaseIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "1s");
    cfg.setProperty(Property.COMPACTOR_MIN_JOB_WAIT_TIME, "100ms");
    cfg.setProperty(Property.COMPACTOR_MAX_JOB_WAIT_TIME, "1s");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    cfg.getClusterServerConfiguration()
        .addCompactorResourceGroup(CompactionBaseIT.COMPACTOR_GROUP_1, 1);
    cfg.getClusterServerConfiguration()
        .addCompactorResourceGroup(CompactionBaseIT.COMPACTOR_GROUP_2, 1);
  }

  @Test
  public void testUserCompactionRequested() throws Exception {

    String tableName = getUniqueNames(1)[0];
    try (final AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // configure tablet compaction iterator that slows compaction down so we can test
      // that the USER_COMPACTION_REQUESTED column is set when a user compaction is requested
      // when a system compaction is running and blocking

      var ntc = new NewTableConfiguration();
      IteratorSetting iterSetting = new IteratorSetting(50, SlowIterator.class);
      SlowIterator.setSleepTime(iterSetting, 1);
      ntc.attachIterator(iterSetting, EnumSet.of(IteratorUtil.IteratorScope.majc));
      ntc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "20"));
      client.tableOperations().create(tableName, ntc);

      // Insert MAX_DATA rows
      writeRows((ClientContext) client, tableName, CompactionIT.MAX_DATA, false);

      // set the compaction ratio 1 to trigger a system compaction
      client.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1");

      var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      var extent = new KeyExtent(tableId, null, null);

      AtomicReference<ExternalCompactionId> initialCompaction = new AtomicReference<>();

      // Wait for the system compaction to start
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        var externalCompactions = tabletMeta.getExternalCompactions();
        log.debug("Current external compactions {}", externalCompactions.size());
        var current = externalCompactions.keySet().stream().findFirst();
        current.ifPresent(initialCompaction::set);
        return current.isPresent();
      }, Wait.MAX_WAIT_MILLIS, 100);

      // Trigger a user compaction which should be blocked by the system compaction
      // and should result in the userRequestedCompactions column being set so no more
      // system compactions run
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(false));
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        var userRequestedCompactions = tabletMeta.getUserCompactionsRequested().size();
        log.debug("Current user requested compaction markers {}", userRequestedCompactions);
        return userRequestedCompactions == 1;
      }, Wait.MAX_WAIT_MILLIS, 100);

      // Send more data to trigger another system compaction but the user compaction
      // should go next and the column marker should block it
      writeRows((ClientContext) client, tableName, CompactionIT.MAX_DATA, false);

      // Verify that when the next compaction starts it is a USER compaction as the
      // SYSTEM compaction should be blocked by the marker
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        log.debug("Waiting for USER compaction to start {}", extent);

        var userRequestedCompactions = tabletMeta.getUserCompactionsRequested().size();
        log.debug("Current user requested compaction markers {}", userRequestedCompactions);
        var externalCompactions = tabletMeta.getExternalCompactions();
        log.debug("External compactions size {}", externalCompactions.size());
        var current = externalCompactions.entrySet().stream().findFirst();
        current.ifPresent(c -> log.debug("Current running compaction {}", c.getKey()));

        if (current.isPresent()) {
          var currentCompaction = current.orElseThrow();
          // Next compaction started - verify it is a USER compaction and not SYSTEM
          if (!current.orElseThrow().getKey().equals(initialCompaction.get())) {
            log.debug("Next compaction {} started as type {}", currentCompaction.getKey(),
                currentCompaction.getValue().getKind());
            assertEquals(CompactionKind.USER, currentCompaction.getValue().getKind());
            return true;
          }
        }
        return false;
      }, Wait.MAX_WAIT_MILLIS, 100);

      // Wait for the user compaction to complete and clear the compactions requested column
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        var userRequestedCompactions = tabletMeta.getUserCompactionsRequested().size();
        log.debug("Current user requested compaction markers {}", userRequestedCompactions);
        return userRequestedCompactions == 0;
      }, Wait.MAX_WAIT_MILLIS, 100);

      // Wait and verify all compactions finish
      Wait.waitFor(() -> {
        var tabletMeta = ((ClientContext) client).getAmple().readTablet(extent);
        var externalCompactions = tabletMeta.getExternalCompactions().size();
        log.debug("Current external compactions {}", externalCompactions);
        return externalCompactions == 0 && tabletMeta.getCompacted().isEmpty();
      }, Wait.MAX_WAIT_MILLIS, 100);
    }

    ExternalCompactionTestUtils.assertNoCompactionMetadata(getServerContext(), tableName);
  }

  protected void writeRows(AccumuloClient client, String tableName, int rows, boolean wait)
      throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (int i = 0; i < rows; i++) {
        Mutation m = new Mutation(String.format("r:%04d", i));
        m.put("", "", "" + i);
        bw.addMutation(m);

        if (i % 75 == 0) {
          // create many files as this will cause a system compaction
          bw.flush();
          client.tableOperations().flush(tableName, null, null, wait);
        }
      }
    }
    client.tableOperations().flush(tableName, null, null, wait);
  }
}
