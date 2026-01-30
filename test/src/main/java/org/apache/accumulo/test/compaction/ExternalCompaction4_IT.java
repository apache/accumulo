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

import static org.apache.accumulo.core.conf.Property.TABLE_FILE_MAX;
import static org.apache.accumulo.core.conf.Property.TABLE_MAJC_RATIO;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionCompleted;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.confirmCompactionRunning;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.waitForCompactionStartAndReturnEcids;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ErrorThrowingIterator;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class ExternalCompaction4_IT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
  }

  @Test
  public void testErrorDuringCompactionNoOutput() throws Exception {
    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      createTable(client, table1, "cs1");
      // prevent intermediate compactions from running as 50 files are generated in test
      client.tableOperations().setProperty(table1, TABLE_FILE_MAX.getKey(), "51");
      client.tableOperations().setProperty(table1, TABLE_MAJC_RATIO.getKey(), "51");
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table1));

      ReadWriteIT.ingest(client, 50, 1, 1, 0, "colf", table1, 1);
      ReadWriteIT.verify(client, 50, 1, 1, 0, table1);

      Ample ample = ((ClientContext) client).getAmple();
      try (
          TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(50, tm.getFiles().size());
      }

      IteratorSetting setting = new IteratorSetting(50, "ageoff", AgeOffFilter.class);
      setting.addOption("ttl", "0");
      setting.addOption("currentTime", Long.toString(System.currentTimeMillis() + 86400));
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));

      // Since this iterator is on the top, it will throw an error 3 times, then allow the
      // ageoff iterator to do its work.
      IteratorSetting setting2 = new IteratorSetting(51, "error", ErrorThrowingIterator.class);
      setting2.addOption(ErrorThrowingIterator.TIMES, "3");
      client.tableOperations().attachIterator(table1, setting2, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (TabletsMetadata tm = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        assertThrows(NoSuchElementException.class, () -> tm.iterator().next());
      }
      assertEquals(0, client.createScanner(table1).stream().count());
    }
  }

  @Test
  public void testErrorDuringUserCompaction() throws Exception {
    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      createTable(client, table1, "cs1");
      client.tableOperations().setProperty(table1, TABLE_FILE_MAX.getKey(), "1001");
      client.tableOperations().setProperty(table1, TABLE_MAJC_RATIO.getKey(), "1001");
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table1));

      // In addition to testing errors in compactions, this test also exercises creating lots of
      // files to compact. The following will create 1000 files to compact. When changing this test
      // try to keep both or create a new test for lots of files to compact.
      ReadWriteIT.ingest(client, 1000, 1, 1, 0, "colf", table1, 1);

      Ample ample = ((ClientContext) client).getAmple();
      try (
          TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(1000, tm.getFiles().size());
      }

      IteratorSetting setting = new IteratorSetting(50, "error", ErrorThrowingIterator.class);
      setting.addOption(ErrorThrowingIterator.TIMES, "3");
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (TabletsMetadata tms =
          ample.readTablets().forTable(tid).fetch(ColumnType.FILES, ColumnType.ECOMP).build()) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(1, tm.getFiles().size());
        // ensure the failed compactions did not leave anything in the metadata table
        assertEquals(0, tm.getExternalCompactions().size());
      }

      ReadWriteIT.verify(client, 1000, 1, 1, 0, table1);

    }

  }

  @Test
  public void testDeleteTableCancelsSleepingExternalCompaction() throws Exception {

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs4"); // TODO which service
      TableId tid = getCluster().getServerContext().getTableId(table1);
      writeData(client, table1);

      // The purpose of this test to ensure the sleeping thread is interrupted. The only way the
      // compactor can cancel this compaction is if it interrupts the sleeping thread.
      IteratorSetting iteratorSetting = new IteratorSetting(100, SlowIterator.class);
      // Sleep so long that the test would timeout if the thread is not interrupted.
      SlowIterator.setSleepTime(iteratorSetting, 600000);
      SlowIterator.setSeekSleepTime(iteratorSetting, 600000);

      client.tableOperations().compact(table1,
          new CompactionConfig().setIterators(List.of(iteratorSetting)).setWait(false));

      // Wait for the compaction to start by waiting for 1 external compaction column
      Set<ExternalCompactionId> ecids =
          waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      // Confirm that this ECID shows up in RUNNING set
      int matches = confirmCompactionRunning(getCluster().getServerContext(), ecids);
      assertTrue(matches > 0);

      client.tableOperations().delete(table1);

      confirmCompactionCompleted(getCluster().getServerContext(), ecids,
          TCompactionState.CANCELLED);

    }
  }

}
