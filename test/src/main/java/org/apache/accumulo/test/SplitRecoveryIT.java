/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class SplitRecoveryIT extends AccumuloClusterHarness {

  private Mutation m(String row) {
    Mutation result = new Mutation(row);
    result.put("cf", "cq", new Value("value".getBytes()));
    return result;
  }

  boolean isOffline(String tablename, AccumuloClient accumuloClient) throws TableNotFoundException {
    String tableId = accumuloClient.tableOperations().tableIdMap().get(tablename);
    try (Scanner scanner = accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
      scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
      return Iterators.size(scanner.iterator()) == 0;
    }
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws Exception {

    String tableName = getUniqueNames(1)[0];

    try (AccumuloClient accumuloClient = createAccumuloClient()) {
      for (int tn = 0; tn < 2; tn++) {
        // create a table and put some data in it
        accumuloClient.tableOperations().create(tableName);
        BatchWriter bw = accumuloClient.createBatchWriter(tableName, new BatchWriterConfig());
        bw.addMutation(m("a"));
        bw.addMutation(m("b"));
        bw.addMutation(m("c"));
        bw.close();
        // take the table offline
        accumuloClient.tableOperations().offline(tableName);
        while (!isOffline(tableName, accumuloClient))
          sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

        // poke a partial split into the metadata table
        accumuloClient.securityOperations().grantTablePermission(getAdminPrincipal(),
            MetadataTable.NAME, TablePermission.WRITE);
        Table.ID tableId = Table.ID
            .of(accumuloClient.tableOperations().tableIdMap().get(tableName));

        KeyExtent extent = new KeyExtent(tableId, null, new Text("b"));
        Mutation m = extent.getPrevRowUpdateMutation();

        TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m,
            new Value(Double.toString(0.5).getBytes()));
        TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m,
            KeyExtent.encodePrevEndRow(null));
        bw = accumuloClient.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
        bw.addMutation(m);

        if (tn == 1) {

          bw.flush();

          try (Scanner scanner = accumuloClient.createScanner(MetadataTable.NAME,
              Authorizations.EMPTY)) {
            scanner.setRange(extent.toMetadataRange());
            scanner.fetchColumnFamily(DataFileColumnFamily.NAME);

            KeyExtent extent2 = new KeyExtent(tableId, new Text("b"), null);
            m = extent2.getPrevRowUpdateMutation();
            TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value("/t2".getBytes()));
            TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value("M0".getBytes()));

            for (Entry<Key,Value> entry : scanner) {
              m.put(DataFileColumnFamily.NAME, entry.getKey().getColumnQualifier(),
                  entry.getValue());
            }

            bw.addMutation(m);
          }
        }

        bw.close();
        // bring the table online
        accumuloClient.tableOperations().online(tableName);

        // verify the tablets went online
        try (Scanner scanner = accumuloClient.createScanner(tableName, Authorizations.EMPTY)) {
          int i = 0;
          String expected[] = {"a", "b", "c"};
          for (Entry<Key,Value> entry : scanner) {
            assertEquals(expected[i], entry.getKey().getRow().toString());
            i++;
          }
          assertEquals(3, i);

          accumuloClient.tableOperations().delete(tableName);
        }
      }
    }
  }
}
