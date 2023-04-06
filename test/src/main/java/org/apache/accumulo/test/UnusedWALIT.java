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
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

// When reviewing the changes for ACCUMULO-3423, kturner suggested
// "tablets will now have log references that contain no data,
// so it may be marked with 3 WALs, the first with data, the 2nd without, a 3rd with data.
// It would be useful to have an IT that will test this situation.
public class UnusedWALIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    final long logSize = 1024 * 1024 * 10;
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, Long.toString(logSize));
    cfg.setNumTservers(1);
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    hadoopCoreSite.set("fs.namenode.fs-limits.min-block-size", Long.toString(logSize));
  }

  @Test
  public void test() throws Exception {
    // don't want this bad boy cleaning up walog entries
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

    // make two tables
    String[] tableNames = getUniqueNames(2);
    String bigTable = tableNames[0];
    String lilTable = tableNames[1];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.tableOperations().create(bigTable);
      c.tableOperations().create(lilTable);

      ServerContext context = getServerContext();

      // put some data in a log that should be replayed for both tables
      writeSomeData(c, bigTable, 0, 10, 0, 10);
      scanSomeData(c, bigTable, 0, 10, 0, 10);
      writeSomeData(c, lilTable, 0, 1, 0, 1);
      scanSomeData(c, lilTable, 0, 1, 0, 1);
      assertEquals(2, getWALCount(context));

      // roll the logs by pushing data into bigTable
      writeSomeData(c, bigTable, 0, 3000, 0, 1000);
      assertEquals(3, getWALCount(context));

      // put some data in the latest log
      writeSomeData(c, lilTable, 1, 10, 0, 10);
      scanSomeData(c, lilTable, 1, 10, 0, 10);

      // bounce the tserver
      getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

      // wait for the metadata table to be online
      try (Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
      }

      // check our two sets of data in different logs
      scanSomeData(c, lilTable, 0, 1, 0, 1);
      scanSomeData(c, lilTable, 1, 10, 0, 10);
    }
  }

  private void scanSomeData(AccumuloClient c, String table, int startRow, int rowCount,
      int startCol, int colCount) throws Exception {
    try (Scanner s = c.createScanner(table, Authorizations.EMPTY)) {
      s.setRange(
          new Range(Integer.toHexString(startRow), Integer.toHexString(startRow + rowCount)));
      int row = startRow;
      int col = startCol;
      for (Entry<Key,Value> entry : s) {
        assertEquals(row, Integer.parseInt(entry.getKey().getRow().toString(), 16));
        assertEquals(col++, Integer.parseInt(entry.getKey().getColumnQualifier().toString(), 16));
        if (col == startCol + colCount) {
          col = startCol;
          row++;
          if (row == startRow + rowCount) {
            break;
          }
        }
      }
      assertEquals(row, startRow + rowCount);
    }
  }

  private int getWALCount(ServerContext context) throws Exception {
    WalStateManager wals = new WalStateManager(context);
    int result = 0;
    for (Entry<TServerInstance,List<UUID>> entry : wals.getAllMarkers().entrySet()) {
      result += entry.getValue().size();
    }
    return result;
  }

  private void writeSomeData(AccumuloClient client, String table, int startRow, int rowCount,
      int startCol, int colCount) throws Exception {
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(10 * 1024 * 1024);
    BatchWriter bw = client.createBatchWriter(table, config);
    for (int r = startRow; r < startRow + rowCount; r++) {
      Mutation m = new Mutation(Integer.toHexString(r));
      for (int c = startCol; c < startCol + colCount; c++) {
        m.put("", Integer.toHexString(c), "");
      }
      bw.addMutation(m);
    }
    bw.close();
  }

}
