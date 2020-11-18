/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

public class MetadataIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void testFlushAndCompact() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(2);

      // create a table to write some data to metadata table
      c.tableOperations().create(tableNames[0]);

      try (Scanner rootScanner = c.createScanner(RootTable.NAME, Authorizations.EMPTY)) {
        rootScanner.setRange(TabletsSection.getRange());
        rootScanner.fetchColumnFamily(DataFileColumnFamily.NAME);

        Set<String> files1 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner)
          files1.add(entry.getKey().getColumnQualifier().toString());

        c.tableOperations().create(tableNames[1]);
        c.tableOperations().flush(MetadataTable.NAME, null, null, true);

        Set<String> files2 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner)
          files2.add(entry.getKey().getColumnQualifier().toString());

        // flush of metadata table should change file set in root table
        assertTrue(!files2.isEmpty());
        assertNotEquals(files1, files2);

        c.tableOperations().compact(MetadataTable.NAME, null, null, false, true);

        Set<String> files3 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner)
          files3.add(entry.getKey().getColumnQualifier().toString());

        // compaction of metadata table should change file set in root table
        assertNotEquals(files2, files3);
      }
    }
  }

  @Test
  public void mergeMeta() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] names = getUniqueNames(5);
      SortedSet<Text> splits = new TreeSet<>();
      for (String id : "1 2 3 4 5".split(" ")) {
        splits.add(new Text(id));
      }
      c.tableOperations().addSplits(MetadataTable.NAME, splits);
      for (String tableName : names) {
        c.tableOperations().create(tableName);
      }
      c.tableOperations().merge(MetadataTable.NAME, null, null);
      try (Scanner s = c.createScanner(RootTable.NAME, Authorizations.EMPTY)) {
        s.setRange(DeletesSection.getRange());
        while (Iterators.size(s.iterator()) == 0) {
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
        assertEquals(0, c.tableOperations().listSplits(MetadataTable.NAME).size());
      }
    }
  }

  @Test
  public void batchScanTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      // batch scan regular metadata table
      int count = 0;
      try (BatchScanner s = c.createBatchScanner(MetadataTable.NAME)) {
        s.setRanges(Collections.singleton(new Range()));
        for (Entry<Key,Value> e : s) {
          if (e != null)
            count++;
        }
      }

      assertTrue(count > 0);

      // batch scan root metadata table
      try (BatchScanner s = c.createBatchScanner(RootTable.NAME)) {
        s.setRanges(Collections.singleton(new Range()));
        count = 0;
        for (Entry<Key,Value> e : s) {
          if (e != null)
            count++;
        }
        assertTrue(count > 0);
      }
    }
  }

  @Test
  public void testAmpleReadTablets() throws Exception {

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      accumuloClient.securityOperations().grantTablePermission(accumuloClient.whoami(),
          MetadataTable.NAME, TablePermission.WRITE);
      BatchWriter bw =
          accumuloClient.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
      ClientContext cc = (ClientContext) accumuloClient;
      // Create a fake METADATA table with these splits
      String[] splits = {"a", "e", "j", "o", "t", "z"};
      // create metadata for a table "t" with the splits above
      TableId tableId = TableId.of("t");
      Text pr = null;
      for (String s : splits) {
        Text split = new Text(s);
        Mutation prevRow = MetadataSchema.TabletsSection.TabletColumnFamily
            .createPrevRowMutation(new KeyExtent(tableId, split, pr));
        prevRow.put(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME,
            new Text("123456"), new Value("127.0.0.1:1234"));
        MetadataSchema.TabletsSection.ChoppedColumnFamily.CHOPPED_COLUMN.put(prevRow,
            new Value("junk"));
        bw.addMutation(prevRow);
        pr = split;
      }
      // Add the default tablet
      Mutation defaultTablet = MetadataSchema.TabletsSection.TabletColumnFamily
          .createPrevRowMutation(new KeyExtent(tableId, null, pr));
      defaultTablet.put(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME,
          new Text("123456"), new Value("127.0.0.1:1234"));
      bw.addMutation(defaultTablet);
      bw.close();

      Text startRow = new Text("a");
      Text endRow = new Text("z");

      // Call up Ample from the client context using table "t" and build
      TabletsMetadata tablets = cc.getAmple().readTablets().forTable(tableId)
          .overlapping(startRow, endRow).fetch(FILES, LOCATION, LAST, PREV_ROW).build();

      TabletMetadata tabletMetadata0 = Iterables.get(tablets, 0);
      TabletMetadata tabletMetadata1 = Iterables.get(tablets, 1);
      TabletMetadata tabletMetadata2 = Iterables.get(tablets, 2);
      TabletMetadata tabletMetadata3 = Iterables.get(tablets, 3);
      TabletMetadata tabletMetadata4 = Iterables.get(tablets, 4);

      String infoTabletId0 = tabletMetadata0.getTableId().toString();
      String infoExtent0 = tabletMetadata0.getExtent().toString();
      String infoPrevEndRow0 = tabletMetadata0.getPrevEndRow().toString();
      String infoEndRow0 = tabletMetadata0.getEndRow().toString();

      String infoTabletId1 = tabletMetadata1.getTableId().toString();
      String infoExtent1 = tabletMetadata1.getExtent().toString();
      String infoPrevEndRow1 = tabletMetadata1.getPrevEndRow().toString();
      String infoEndRow1 = tabletMetadata1.getEndRow().toString();

      String infoTabletId2 = tabletMetadata2.getTableId().toString();
      String infoExtent2 = tabletMetadata2.getExtent().toString();
      String infoPrevEndRow2 = tabletMetadata2.getPrevEndRow().toString();
      String infoEndRow2 = tabletMetadata2.getEndRow().toString();

      String infoTabletId3 = tabletMetadata3.getTableId().toString();
      String infoExtent3 = tabletMetadata3.getExtent().toString();
      String infoPrevEndRow3 = tabletMetadata3.getPrevEndRow().toString();
      String infoEndRow3 = tabletMetadata3.getEndRow().toString();

      String infoTabletId4 = tabletMetadata4.getTableId().toString();
      String infoExtent4 = tabletMetadata4.getExtent().toString();
      String infoPrevEndRow4 = tabletMetadata4.getPrevEndRow().toString();
      String infoEndRow4 = tabletMetadata4.getEndRow().toString();

      String testInfoTabletId = "t";

      String testInfoKeyExtent0 = "t;e;a";
      String testInfoKeyExtent1 = "t;j;e";
      String testInfoKeyExtent2 = "t;o;j";
      String testInfoKeyExtent3 = "t;t;o";
      String testInfoKeyExtent4 = "t;z;t";

      String testInfoPrevEndRow0 = "a";
      String testInfoPrevEndRow1 = "e";
      String testInfoPrevEndRow2 = "j";
      String testInfoPrevEndRow3 = "o";
      String testInfoPrevEndRow4 = "t";

      String testInfoEndRow0 = "e";
      String testInfoEndRow1 = "j";
      String testInfoEndRow2 = "o";
      String testInfoEndRow3 = "t";
      String testInfoEndRow4 = "z";

      assertEquals(infoTabletId0, testInfoTabletId);
      assertEquals(infoTabletId1, testInfoTabletId);
      assertEquals(infoTabletId2, testInfoTabletId);
      assertEquals(infoTabletId3, testInfoTabletId);
      assertEquals(infoTabletId4, testInfoTabletId);

      assertEquals(infoExtent0, testInfoKeyExtent0);
      assertEquals(infoExtent1, testInfoKeyExtent1);
      assertEquals(infoExtent2, testInfoKeyExtent2);
      assertEquals(infoExtent3, testInfoKeyExtent3);
      assertEquals(infoExtent4, testInfoKeyExtent4);

      assertEquals(infoPrevEndRow0, testInfoPrevEndRow0);
      assertEquals(infoPrevEndRow1, testInfoPrevEndRow1);
      assertEquals(infoPrevEndRow2, testInfoPrevEndRow2);
      assertEquals(infoPrevEndRow3, testInfoPrevEndRow3);
      assertEquals(infoPrevEndRow4, testInfoPrevEndRow4);

      assertEquals(infoEndRow0, testInfoEndRow0);
      assertEquals(infoEndRow1, testInfoEndRow1);
      assertEquals(infoEndRow2, testInfoEndRow2);
      assertEquals(infoEndRow3, testInfoEndRow3);
      assertEquals(infoEndRow4, testInfoEndRow4);
    }
  }

}
