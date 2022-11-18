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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
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
import org.junit.jupiter.api.Test;

public class MetadataIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
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
        for (Entry<Key,Value> entry : rootScanner) {
          files1.add(entry.getKey().getColumnQualifier().toString());
        }

        c.tableOperations().create(tableNames[1]);
        c.tableOperations().flush(MetadataTable.NAME, null, null, true);

        Set<String> files2 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner) {
          files2.add(entry.getKey().getColumnQualifier().toString());
        }

        // flush of metadata table should change file set in root table
        assertTrue(!files2.isEmpty());
        assertNotEquals(files1, files2);

        c.tableOperations().compact(MetadataTable.NAME, null, null, false, true);

        Set<String> files3 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner) {
          files3.add(entry.getKey().getColumnQualifier().toString());
        }

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
        while (s.stream().findAny().isEmpty()) {
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
      try (BatchScanner s = c.createBatchScanner(MetadataTable.NAME)) {
        s.setRanges(Collections.singleton(new Range()));
        assertTrue(s.stream().anyMatch(Objects::nonNull));
      }

      // batch scan root metadata table
      try (BatchScanner s = c.createBatchScanner(RootTable.NAME)) {
        s.setRanges(Collections.singleton(new Range()));
        assertTrue(s.stream().anyMatch(Objects::nonNull));
      }
    }
  }

  @Test
  public void testAmpleReadTablets() throws Exception {

    try (ClientContext cc = (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      cc.securityOperations().grantTablePermission(cc.whoami(), MetadataTable.NAME,
          TablePermission.WRITE);

      SortedSet<Text> partitionKeys = new TreeSet<>();
      partitionKeys.add(new Text("a"));
      partitionKeys.add(new Text("e"));
      partitionKeys.add(new Text("j"));

      cc.tableOperations().create("t");
      cc.tableOperations().addSplits("t", partitionKeys);

      Text startRow = new Text("a");
      Text endRow = new Text("z");

      // Call up Ample from the client context using table "t" and build
      TabletsMetadata tablets = cc.getAmple().readTablets().forTable(TableId.of("1"))
          .overlapping(startRow, endRow).fetch(FILES, LOCATION, LAST, PREV_ROW).build();

      TabletMetadata tabletMetadata0 = tablets.stream().findFirst().get();
      TabletMetadata tabletMetadata1 = tablets.stream().skip(1).findFirst().get();

      String infoTabletId0 = tabletMetadata0.getTableId().toString();
      String infoExtent0 = tabletMetadata0.getExtent().toString();
      String infoPrevEndRow0 = tabletMetadata0.getPrevEndRow().toString();
      String infoEndRow0 = tabletMetadata0.getEndRow().toString();

      String infoTabletId1 = tabletMetadata1.getTableId().toString();
      String infoExtent1 = tabletMetadata1.getExtent().toString();
      String infoPrevEndRow1 = tabletMetadata1.getPrevEndRow().toString();
      String infoEndRow1 = tabletMetadata1.getEndRow().toString();

      String testInfoTableId = "1";

      String testInfoKeyExtent0 = "1;e;a";
      String testInfoKeyExtent1 = "1;j;e";

      String testInfoPrevEndRow0 = "a";
      String testInfoPrevEndRow1 = "e";

      String testInfoEndRow0 = "e";
      String testInfoEndRow1 = "j";

      assertEquals(infoTabletId0, testInfoTableId);
      assertEquals(infoTabletId1, testInfoTableId);

      assertEquals(infoExtent0, testInfoKeyExtent0);
      assertEquals(infoExtent1, testInfoKeyExtent1);

      assertEquals(infoPrevEndRow0, testInfoPrevEndRow0);
      assertEquals(infoPrevEndRow1, testInfoPrevEndRow1);

      assertEquals(infoEndRow0, testInfoEndRow0);
      assertEquals(infoEndRow1, testInfoEndRow1);

    }
  }
}
