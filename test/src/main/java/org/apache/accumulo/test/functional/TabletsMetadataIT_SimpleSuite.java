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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metadata.schema.filters.TabletMetadataFilter;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TabletsMetadataIT_SimpleSuite extends SharedMiniClusterBase {

  private static class TestBuilder extends TabletsMetadata.Builder {

    private final RootTabletMetadata rtm;

    TestBuilder(AccumuloClient client, Function<DataLevel,String> tableMapper,
        RootTabletMetadata rtm) {
      super(client, tableMapper);
      this.rtm = rtm;
    }

    @Override
    protected RootTabletMetadata getRootMetadata(AccumuloClient client) {
      return rtm;
    }

  }

  /**
   * If the TabletMetadata contains the supplied key, then return the TabletMetadata
   */
  public static class HasColumnFilter extends TabletMetadataFilter {

    Predicate<TabletMetadata> pred;

    public HasColumnFilter() {}

    public HasColumnFilter(ColumnType type, boolean include) {
      pred = (tm) -> {
        Stream<Key> stream = tm.getKeyValues().stream().map(e -> e.getKey());
        if (include) {
          return stream.anyMatch(k -> ColumnType.COLUMNS_TO_QUALIFIERS.get(type).hasColumns(k));
        } else {
          return stream.noneMatch(k -> !ColumnType.COLUMNS_TO_QUALIFIERS.get(type).hasColumns(k));
        }
      };
    }

    public HasColumnFilter(Key key, boolean include) {
      pred = (tm) -> {
        Stream<Key> stream = tm.getKeyValues().stream().map(e -> e.getKey());
        if (include) {
          return stream.anyMatch(k -> k.equals(key, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL));
        } else {
          return stream
              .noneMatch(k -> k.equals(key, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL));
        }
      };
    }

    @Override
    public Set<ColumnType> getColumns() {
      return EnumSet.allOf(ColumnType.class);
    }

    @Override
    protected Predicate<TabletMetadata> acceptTablet() {
      return pred;
    }
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void testTabletsMetadataRoot() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.instanceOperations().waitForBalance();
      final RootTabletMetadata rtm = RootTabletMetadata.read((ClientContext) c);
      TestBuilder builder = new TestBuilder(c, DataLevel::metaTable, rtm);
      TabletsMetadata tms = builder.forLevel(DataLevel.ROOT).saveKeyValues().build();
      Iterator<TabletMetadata> iter = tms.iterator();
      assertTrue(iter.hasNext());
      final TabletMetadata tm = iter.next();
      assertFalse(iter.hasNext());

      Map<Key,Value> fromTM = new TreeMap<>();
      tm.getKeyValues().forEach(e -> fromTM.put(e.getKey(), e.getValue()));

      Map<Key,Value> fromZK = new TreeMap<>();
      rtm.getKeyValues().forEach(e -> fromZK.put(e.getKey(), e.getValue()));

      assertEquals(fromTM, fromZK);
    }
  }

  @Test
  public void testTabletsMetadataRootFilter() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.instanceOperations().waitForBalance();
      final RootTabletMetadata rtm = RootTabletMetadata.read((ClientContext) c);
      for (Entry<Key,Value> e : rtm.toKeyValues().entrySet()) {
        TestBuilder builder = new TestBuilder(c, DataLevel::metaTable, rtm);
        TabletsMetadata tms =
            builder.forLevel(DataLevel.ROOT).filter(new HasColumnFilter(e.getKey(), true)).build();
        assertTrue(tms.iterator().hasNext());
        TestBuilder builder2 = new TestBuilder(c, DataLevel::metaTable, rtm);
        TabletsMetadata tms2 = builder2.forLevel(DataLevel.ROOT)
            .filter(new HasColumnFilter(e.getKey(), false)).build();
        assertFalse(tms2.iterator().hasNext());
      }
    }
  }

  @Test
  public void testTabletsMetadataMetadata() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.instanceOperations().waitForBalance();
      TabletsMetadata tms =
          TabletsMetadata.builder(c).forLevel(DataLevel.METADATA).saveKeyValues().build();
      Iterator<TabletMetadata> iter = tms.iterator();
      assertTrue(iter.hasNext());
      TabletMetadata tm = iter.next();
      assertEquals(SystemTables.METADATA.tableId(), tm.getExtent().tableId());
      assertTrue(iter.hasNext());
      tm = iter.next();
      assertEquals(SystemTables.METADATA.tableId(), tm.getExtent().tableId());
      assertFalse(iter.hasNext());
    }
  }

  @Test
  public void testTabletsMetadataMetadataFilter() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.instanceOperations().waitForBalance();
      TabletsMetadata tms =
          TabletsMetadata.builder(c).forLevel(DataLevel.METADATA).fetch(ColumnType.values())
              .filter(new HasColumnFilter(ColumnType.PREV_ROW, true)).build();
      Iterator<TabletMetadata> iter = tms.iterator();
      assertTrue(iter.hasNext());
      TabletMetadata tm = iter.next();
      assertEquals(SystemTables.METADATA.tableId(), tm.getExtent().tableId());
      assertTrue(iter.hasNext());
      tm = iter.next();
      assertEquals(SystemTables.METADATA.tableId(), tm.getExtent().tableId());
      assertFalse(iter.hasNext());
      TabletsMetadata tms2 =
          TabletsMetadata.builder(c).forLevel(DataLevel.METADATA).fetch(ColumnType.values())
              .filter(new HasColumnFilter(ColumnType.PREV_ROW, false)).build();
      Iterator<TabletMetadata> iter2 = tms2.iterator();
      assertFalse(iter2.hasNext());
    }
  }

  @Test
  public void testTabletsMetadataUserFilter() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String table = getUniqueNames(1)[0];
      c.tableOperations().create(table);
      ReadWriteIT.ingest(c, 100, 10, 10, 0, table);
      c.tableOperations().flush(table);
      TableId tid = TableId.of(c.tableOperations().tableIdMap().get(table));
      c.instanceOperations().waitForBalance();

      TabletsMetadata tms =
          TabletsMetadata.builder(c).forLevel(DataLevel.USER).fetch(ColumnType.values())
              .filter(new HasColumnFilter(ColumnType.PREV_ROW, true)).build();
      Iterator<TabletMetadata> iter = tms.iterator();
      assertTrue(iter.hasNext());
      TabletMetadata tm = iter.next();
      assertEquals(tid, tm.getExtent().tableId());
      assertTrue(iter.hasNext());
      tm = iter.next();
      assertEquals(tid, tm.getExtent().tableId());
      assertFalse(iter.hasNext());
      TabletsMetadata tms2 =
          TabletsMetadata.builder(c).forLevel(DataLevel.USER).fetch(ColumnType.values())
              .filter(new HasColumnFilter(ColumnType.PREV_ROW, false)).build();
      Iterator<TabletMetadata> iter2 = tms2.iterator();
      assertFalse(iter2.hasNext());
    }
  }

}
