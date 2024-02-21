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

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

public class CompactionConfigChangeIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {

    cfg.getClusterServerConfiguration().addCompactorResourceGroup("little", 1);
    cfg.getClusterServerConfiguration().addCompactorResourceGroup("big", 1);

    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.groups",
        ("[{'name':'small','maxSize':'2M'}, {'name':'medium','maxSize':'128M'},"
            + "{'name':'large'}]").replaceAll("'", "\""));

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  public static long countFiles(AccumuloClient client, String table, String fileNamePrefix)
      throws Exception {
    var ctx = ((ClientContext) client);
    var tableId = ctx.getTableId(table);

    try (var tablets = ctx.getAmple().readTablets().forTable(tableId).build()) {
      return tablets.stream().flatMap(tm -> tm.getFiles().stream())
          .filter(stf -> stf.getFileName().startsWith(fileNamePrefix)).count();
    }
  }

  @Test
  public void testRemovingCompactionExecutor() throws Exception {
    // this test reproduces #3749
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String table = getUniqueNames(1)[0];

      createTable(client, table, "cs1", 100);

      ExternalCompactionTestUtils.writeData(client, table, MAX_DATA);

      client.tableOperations().flush(table, null, null, true);

      assertEquals(100, countFiles(client, table, "F"));

      // Start 100 slow compactions, each compaction should take ~1 second. There are 2 tservers
      // each with 2 threads and then 8 threads.
      CompactionConfig compactionConfig = new CompactionConfig();
      IteratorSetting iteratorSetting = new IteratorSetting(100, SlowIterator.class);
      SlowIterator.setSleepTime(iteratorSetting, 100);
      compactionConfig.setIterators(List.of(iteratorSetting));
      compactionConfig.setWait(false);

      client.tableOperations().compact(table, compactionConfig);

      // give some time for compactions to start running
      Wait.waitFor(() -> countFiles(client, table, "F") < 95);

      // Change config deleting groups named small, medium, and large. There was bug where
      // deleting groups running compactions would leave the tablet in a bad state for future
      // compactions. Because the compactions are running slow, expect this config change to overlap
      // with running compactions.
      client.instanceOperations().setProperty(
          Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.groups",
          ("[{'name':'little','maxSize':'128M'},{'name':'big'}]").replaceAll("'", "\""));

      Wait.waitFor(() -> countFiles(client, table, "F") == 0, 60000);

      verify(client, table, 1);
    }
  }
}
