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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class SplitRecoveryIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  private Mutation m(String row) {
    Mutation result = new Mutation(row);
    result.put("cf", "cq", new Value("value"));
    return result;
  }

  boolean isOffline(String tablename, AccumuloClient client) throws TableNotFoundException {
    String tableId = client.tableOperations().tableIdMap().get(tablename);
    try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      return scanner.stream().findAny().isEmpty();
    }
  }

  @Test
  public void test() throws Exception {

    String tableName = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      for (int tn = 0; tn < 2; tn++) {
        // create a table and put some data in it
        client.tableOperations().create(tableName);
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
          bw.addMutation(m("a"));
          bw.addMutation(m("b"));
          bw.addMutation(m("c"));
        }
        // take the table offline
        client.tableOperations().offline(tableName);
        while (!isOffline(tableName, client)) {
          sleepUninterruptibly(200, MILLISECONDS);
        }

        // poke a partial split into the metadata table
        client.securityOperations().grantTablePermission(getAdminPrincipal(), MetadataTable.NAME,
            TablePermission.WRITE);
        TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

        KeyExtent extent = new KeyExtent(tableId, null, new Text("b"));
        Mutation m = TabletColumnFamily.createPrevRowMutation(extent);

        TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value(Double.toString(0.5)));
        TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m, TabletColumnFamily.encodePrevEndRow(null));
        try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
          bw.addMutation(m);

          if (tn == 1) {
            bw.flush();

            try (Scanner scanner = client.createScanner(MetadataTable.NAME)) {
              scanner.setRange(extent.toMetaRange());
              scanner.fetchColumnFamily(DataFileColumnFamily.NAME);

              KeyExtent extent2 = new KeyExtent(tableId, new Text("b"), null);
              m = TabletColumnFamily.createPrevRowMutation(extent2);
              ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value("t2"));
              ServerColumnFamily.TIME_COLUMN.put(m, new Value("M0"));

              for (Entry<Key,Value> entry : scanner) {
                m.put(DataFileColumnFamily.NAME, entry.getKey().getColumnQualifier(),
                    entry.getValue());
              }
              bw.addMutation(m);
            }
          }
        }

        // bring the table online
        client.tableOperations().online(tableName);

        // verify the tablets went online
        try (Scanner scanner = client.createScanner(tableName)) {
          int i = 0;
          String[] expected = {"a", "b", "c"};
          for (Entry<Key,Value> entry : scanner) {
            assertEquals(expected[i], entry.getKey().getRow().toString());
            i++;
          }
          assertEquals(3, i);

          client.tableOperations().delete(tableName);
        }
      }
    }
  }
}
