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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.split.SplitUtils;
import org.apache.accumulo.test.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SplitFileLimitIT extends SharedMiniClusterBase {

  private static final int MAX_FILES_TO_OPEN = 20;

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig((cfg, coreSite) -> {
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.setProperty(Property.SPLIT_MAXOPEN, Integer.toString(MAX_FILES_TO_OPEN));
      cfg.getClusterServerConfiguration().setNumDefaultCompactors(2);
      cfg.setProperty(Property.COMPACTOR_MIN_JOB_WAIT_TIME, "10ms");
      cfg.setProperty(Property.COMPACTOR_MAX_JOB_WAIT_TIME, "100ms");
    });
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Test
  public void testUnsplittableWhenTooManyFilesThenSplitsAfterCompaction() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    final int numFiles = MAX_FILES_TO_OPEN + 10;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,
          String> props = Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), "1G",
              Property.TABLE_MAJC_RATIO.getKey(), "9999", Property.TABLE_FILE_MAX.getKey(), "1000",
              Property.TABLE_FILE_PAUSE.getKey(), "1000");
      client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      for (int i = 0; i < numFiles; i++) {
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
          Mutation m = new Mutation(String.format("row_%06d", i));
          m.put("fam", "qual", "v".repeat(100) + i);
          bw.addMutation(m);
        }
        client.tableOperations().flush(tableName, null, null, true);
      }

      var ctx = getCluster().getServerContext();
      var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      var extent = new KeyExtent(tableId, null, null);

      TabletMetadata tm = ctx.getAmple().readTablet(extent);
      assertTrue(tm.getFiles().size() > MAX_FILES_TO_OPEN,
          "expected more than " + MAX_FILES_TO_OPEN + " files but saw " + tm.getFiles().size());
      assertEquals(0, client.tableOperations().listSplits(tableName).size());

      client.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "1K");

      // Too many files, so the tablet is marked unsplittable and does not split
      Wait.waitFor(() -> ctx.getAmple().readTablet(extent).getUnSplittable() != null,
          Wait.MAX_WAIT_MILLIS, 100);
      tm = ctx.getAmple().readTablet(extent);
      assertEquals(SplitUtils.toUnSplittable(ctx, tm), tm.getUnSplittable());
      assertEquals(0, client.tableOperations().listSplits(tableName).size());

      // Compacting reduces the file count below the limit, which changes the marker hash, so the
      // tablet is re-evaluated and split
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      Wait.waitFor(() -> !client.tableOperations().listSplits(tableName).isEmpty(),
          Wait.MAX_WAIT_MILLIS * 2, 250);

      // The marker is cleared from every resulting tablet
      Wait.waitFor(() -> {
        try (var tablets = ctx.getAmple().readTablets().forTable(tableId).build()) {
          return tablets.stream().allMatch(t -> t.getUnSplittable() == null);
        }
      }, Wait.MAX_WAIT_MILLIS, 250);
    }
  }

  @Test
  public void testFileCountExactlyAtLimit() throws Exception {
    final String tableName = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,
          String> props = Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), "1G",
              Property.TABLE_MAJC_RATIO.getKey(), "9999", Property.TABLE_FILE_MAX.getKey(), "1000",
              Property.TABLE_FILE_PAUSE.getKey(), "1000");
      client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      for (int i = 0; i < MAX_FILES_TO_OPEN; i++) {
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
          Mutation m = new Mutation(String.format("row_%06d", i));
          m.put("fam", "qual", "v".repeat(100) + i);
          bw.addMutation(m);
        }
        client.tableOperations().flush(tableName, null, null, true);
      }

      var ctx = getCluster().getServerContext();
      var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      var extent = new KeyExtent(tableId, null, null);

      assertEquals(MAX_FILES_TO_OPEN, ctx.getAmple().readTablet(extent).getFiles().size());
      assertNull(ctx.getAmple().readTablet(extent).getUnSplittable());

      client.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "1K");

      Wait.waitFor(() -> ctx.getAmple().readTablet(extent).getUnSplittable() != null,
          Wait.MAX_WAIT_MILLIS, 100);
      assertEquals(0, client.tableOperations().listSplits(tableName).size());
    }
  }
}
