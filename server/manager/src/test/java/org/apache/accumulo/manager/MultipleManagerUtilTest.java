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
package org.apache.accumulo.manager;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata.TableOptions;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata.TableRangeOptions;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;

public class MultipleManagerUtilTest {

  @Test
  public void testNoManager() {
    assertThrows(IllegalStateException.class, () -> {
      ServerContext ctx = createMock(ServerContext.class);
      Set<TableId> tables = Set.of(TableId.of("1"), TableId.of("2"), TableId.of("3"));
      MultipleManagerUtil.getTablesForManagers(ctx, tables, 0);
    });
  }

  @Test
  public void testSingleManager() {
    ServerContext ctx = createMock(ServerContext.class);
    Set<TableId> tables = Set.of(TableId.of("1"), TableId.of("2"), TableId.of("3"));
    List<Set<TableId>> result = MultipleManagerUtil.getTablesForManagers(ctx, tables, 1);
    assertEquals(1, result.size());
    assertEquals(tables, result.get(0));
  }

  @Test
  public void testTwoManagers() {
    TableId t1 = TableId.of("1");
    int t1TabletCount = 4;

    TableId t2 = TableId.of("2");
    int t2TabletCount = 34;

    TableId t3 = TableId.of("3");
    int t3TabletCount = 14;

    ServerContext ctx = createMock(ServerContext.class);
    Ample ample = createMock(Ample.class);
    TableOptions to = createMock(TableOptions.class);
    TableRangeOptions tro1 = createMock(TableRangeOptions.class);
    TableRangeOptions tro2 = createMock(TableRangeOptions.class);
    TableRangeOptions tro3 = createMock(TableRangeOptions.class);
    TabletsMetadata tm1 = createMock(TabletsMetadata.class);
    TabletsMetadata tm2 = createMock(TabletsMetadata.class);
    TabletsMetadata tm3 = createMock(TabletsMetadata.class);

    expect(ctx.getAmple()).andReturn(ample).times(3);
    expect(ample.readTablets()).andReturn(to).anyTimes();
    expect(to.forTable(t1)).andReturn(tro1);
    expect(to.forTable(t2)).andReturn(tro2);
    expect(to.forTable(t3)).andReturn(tro3);
    expect(tro1.build()).andReturn(tm1);
    expect(tro2.build()).andReturn(tm2);
    expect(tro3.build()).andReturn(tm3);
    expect(tm1.stream()).andReturn(createTabletMetadataList(t1TabletCount).stream());
    expect(tm2.stream()).andReturn(createTabletMetadataList(t2TabletCount).stream());
    expect(tm3.stream()).andReturn(createTabletMetadataList(t3TabletCount).stream());

    replay(ctx, ample, to, tro1, tro2, tro3, tm1, tm2, tm3);

    Set<TableId> tables = Set.of(t1, t2, t3);
    List<Set<TableId>> result = MultipleManagerUtil.getTablesForManagers(ctx, tables, 2);
    verify(ctx, ample, to, tro1, tro2, tro3, tm1, tm2, tm3);

    assertEquals(2, result.size());
    Set<TableId> firstManager = result.get(0);
    assertEquals(1, firstManager.size());
    assertTrue(firstManager.contains(t2));

    Set<TableId> secondManager = result.get(1);
    assertEquals(2, secondManager.size());
    assertTrue(secondManager.contains(t1));
    assertTrue(secondManager.contains(t3));

  }

  @Test
  public void testThreeManagers() {
    TableId t1 = TableId.of("1");
    int t1TabletCount = 4;

    TableId t2 = TableId.of("2");
    int t2TabletCount = 34;

    TableId t3 = TableId.of("3");
    int t3TabletCount = 14;

    ServerContext ctx = createMock(ServerContext.class);
    Ample ample = createMock(Ample.class);
    TableOptions to = createMock(TableOptions.class);
    TableRangeOptions tro1 = createMock(TableRangeOptions.class);
    TableRangeOptions tro2 = createMock(TableRangeOptions.class);
    TableRangeOptions tro3 = createMock(TableRangeOptions.class);
    TabletsMetadata tm1 = createMock(TabletsMetadata.class);
    TabletsMetadata tm2 = createMock(TabletsMetadata.class);
    TabletsMetadata tm3 = createMock(TabletsMetadata.class);

    expect(ctx.getAmple()).andReturn(ample).times(3);
    expect(ample.readTablets()).andReturn(to).anyTimes();
    expect(to.forTable(t1)).andReturn(tro1);
    expect(to.forTable(t2)).andReturn(tro2);
    expect(to.forTable(t3)).andReturn(tro3);
    expect(tro1.build()).andReturn(tm1);
    expect(tro2.build()).andReturn(tm2);
    expect(tro3.build()).andReturn(tm3);
    expect(tm1.stream()).andReturn(createTabletMetadataList(t1TabletCount).stream());
    expect(tm2.stream()).andReturn(createTabletMetadataList(t2TabletCount).stream());
    expect(tm3.stream()).andReturn(createTabletMetadataList(t3TabletCount).stream());

    replay(ctx, ample, to, tro1, tro2, tro3, tm1, tm2, tm3);

    Set<TableId> tables = Set.of(t1, t2, t3);
    List<Set<TableId>> result = MultipleManagerUtil.getTablesForManagers(ctx, tables, 3);
    verify(ctx, ample, to, tro1, tro2, tro3, tm1, tm2, tm3);

    assertEquals(3, result.size());
    Set<TableId> firstManager = result.get(0);
    assertEquals(1, firstManager.size());
    assertTrue(firstManager.contains(t2));

    Set<TableId> secondManager = result.get(1);
    assertEquals(1, secondManager.size());
    assertTrue(secondManager.contains(t3));

    Set<TableId> thirdManager = result.get(2);
    assertEquals(1, thirdManager.size());
    assertTrue(thirdManager.contains(t1));

  }

  @Test
  public void testFourManagers() {
    TableId t1 = TableId.of("1");
    int t1TabletCount = 4;

    TableId t2 = TableId.of("2");
    int t2TabletCount = 34;

    TableId t3 = TableId.of("3");
    int t3TabletCount = 14;

    ServerContext ctx = createMock(ServerContext.class);
    Ample ample = createMock(Ample.class);
    TableOptions to = createMock(TableOptions.class);
    TableRangeOptions tro1 = createMock(TableRangeOptions.class);
    TableRangeOptions tro2 = createMock(TableRangeOptions.class);
    TableRangeOptions tro3 = createMock(TableRangeOptions.class);
    TabletsMetadata tm1 = createMock(TabletsMetadata.class);
    TabletsMetadata tm2 = createMock(TabletsMetadata.class);
    TabletsMetadata tm3 = createMock(TabletsMetadata.class);

    expect(ctx.getAmple()).andReturn(ample).times(3);
    expect(ample.readTablets()).andReturn(to).anyTimes();
    expect(to.forTable(t1)).andReturn(tro1);
    expect(to.forTable(t2)).andReturn(tro2);
    expect(to.forTable(t3)).andReturn(tro3);
    expect(tro1.build()).andReturn(tm1);
    expect(tro2.build()).andReturn(tm2);
    expect(tro3.build()).andReturn(tm3);
    expect(tm1.stream()).andReturn(createTabletMetadataList(t1TabletCount).stream());
    expect(tm2.stream()).andReturn(createTabletMetadataList(t2TabletCount).stream());
    expect(tm3.stream()).andReturn(createTabletMetadataList(t3TabletCount).stream());

    replay(ctx, ample, to, tro1, tro2, tro3, tm1, tm2, tm3);

    Set<TableId> tables = Set.of(t1, t2, t3);
    List<Set<TableId>> result = MultipleManagerUtil.getTablesForManagers(ctx, tables, 4);
    verify(ctx, ample, to, tro1, tro2, tro3, tm1, tm2, tm3);

    assertEquals(4, result.size());
    Set<TableId> firstManager = result.get(0);
    assertEquals(1, firstManager.size());
    assertTrue(firstManager.contains(t2));

    Set<TableId> secondManager = result.get(1);
    assertEquals(1, secondManager.size());
    assertTrue(secondManager.contains(t3));

    Set<TableId> thirdManager = result.get(2);
    assertEquals(1, thirdManager.size());
    assertTrue(thirdManager.contains(t1));

    Set<TableId> fourthManager = result.get(3);
    assertEquals(0, fourthManager.size());

  }

  private static final List<TabletMetadata> createTabletMetadataList(int numEntries) {
    List<TabletMetadata> results = new ArrayList<>();
    IntStream.range(0, numEntries).forEach(i -> results.add(new TabletMetadata()));
    return results;

  }
}
