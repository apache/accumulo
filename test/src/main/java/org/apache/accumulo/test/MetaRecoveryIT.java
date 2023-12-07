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

import java.time.Duration;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

// ACCUMULO-3211
public class MetaRecoveryIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, "1048576");
  }

  @Test
  public void test() throws Exception {
    String[] tables = getUniqueNames(10);
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      int i = 0;
      for (String table : tables) {
        log.info("Creating table {}", i);
        c.tableOperations().create(table);
        try (BatchWriter bw = c.createBatchWriter(table)) {
          for (int j = 0; j < 1000; j++) {
            Mutation m = new Mutation("" + j);
            m.put("cf", "cq", "value");
            bw.addMutation(m);
          }
        }
        log.info("Data written to table {}", i);
        i++;
      }
      c.tableOperations().flush(MetadataTable.NAME, null, null, true);
      c.tableOperations().flush(RootTable.NAME, null, null, true);
      SortedSet<Text> splits = new TreeSet<>();
      for (i = 1; i < tables.length; i++) {
        splits.add(new Text("" + i));
      }
      c.tableOperations().addSplits(MetadataTable.NAME, splits);
      log.info("Added {} splits to {}", splits.size(), MetadataTable.NAME);
      c.instanceOperations().waitForBalance();
      log.info("Restarting");
      getCluster().getClusterControl().kill(ServerType.TABLET_SERVER, "localhost");
      getCluster().start();
      log.info("Verifying");
      for (String table : tables) {
        try (BatchScanner scanner = c.createBatchScanner(table)) {
          scanner.setRanges(Collections.singletonList(new Range()));
          assertEquals(1000, Iterators.size(scanner.iterator()));
        }
      }
    }
  }

}
