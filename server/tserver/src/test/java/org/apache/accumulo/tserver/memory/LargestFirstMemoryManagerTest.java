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
package org.apache.accumulo.tserver.memory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
public class LargestFirstMemoryManagerTest {

  private static final long ZERO = LargestFirstMemoryManager.ZERO_TIME;
  private static final long LATER = ZERO + MINUTES.toMillis(20);
  private static final long ONE_GIG = 1024 * 1024 * 1024;
  private static final long ONE_MEG = 1024 * 1024;
  private static final long HALF_GIG = ONE_GIG / 2;
  private static final long QGIG = ONE_GIG / 4;

  private ServerContext context;

  @BeforeEach
  public void mockServerInfo() {
    context = createMock(ServerContext.class);
    AccumuloConfiguration conf = createMock(AccumuloConfiguration.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(conf.getAsBytes(Property.TSERV_MAXMEM)).andReturn(ONE_GIG).anyTimes();
    expect(conf.getCount(Property.TSERV_MINC_MAXCONCURRENT)).andReturn(4).anyTimes();
    replay(context, conf);
  }

  @Test
  public void test() {
    LargestFirstMemoryManagerUnderTest mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(context);
    List<KeyExtent> tabletsToMinorCompact;
    // nothing to do
    tabletsToMinorCompact =
        mgr.tabletsToMinorCompact(tablets(t(k("x"), ZERO, 1000, 0), t(k("y"), ZERO, 2000, 0)));
    assertEquals(0, tabletsToMinorCompact.size());
    // one tablet is really big
    tabletsToMinorCompact =
        mgr.tabletsToMinorCompact(tablets(t(k("x"), ZERO, ONE_GIG, 0), t(k("y"), ZERO, 2000, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(k("x"), tabletsToMinorCompact.get(0));
    // one tablet is idle
    mgr.currentTime = LATER;
    tabletsToMinorCompact =
        mgr.tabletsToMinorCompact(tablets(t(k("x"), ZERO, 1001, 0), t(k("y"), LATER, 2000, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(k("x"), tabletsToMinorCompact.get(0));
    // one tablet is idle, but one is really big
    tabletsToMinorCompact =
        mgr.tabletsToMinorCompact(tablets(t(k("x"), ZERO, 1001, 0), t(k("y"), LATER, ONE_GIG, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(k("y"), tabletsToMinorCompact.get(0));
    // lots of work to do
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(context);
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, HALF_GIG, 0),
        t(k("b"), ZERO, HALF_GIG + ONE_MEG, 0), t(k("c"), ZERO, HALF_GIG + (2 * ONE_MEG), 0),
        t(k("d"), ZERO, HALF_GIG + (3 * ONE_MEG), 0), t(k("e"), ZERO, HALF_GIG + (4 * ONE_MEG), 0),
        t(k("f"), ZERO, HALF_GIG + (5 * ONE_MEG), 0), t(k("g"), ZERO, HALF_GIG + (6 * ONE_MEG), 0),
        t(k("h"), ZERO, HALF_GIG + (7 * ONE_MEG), 0),
        t(k("i"), ZERO, HALF_GIG + (8 * ONE_MEG), 0)));
    assertEquals(2, tabletsToMinorCompact.size());
    assertEquals(k("i"), tabletsToMinorCompact.get(0));
    assertEquals(k("h"), tabletsToMinorCompact.get(1));
    // one finished, one in progress, one filled up
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(context);
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, HALF_GIG, 0),
        t(k("b"), ZERO, HALF_GIG + ONE_MEG, 0), t(k("c"), ZERO, HALF_GIG + (2 * ONE_MEG), 0),
        t(k("d"), ZERO, HALF_GIG + (3 * ONE_MEG), 0), t(k("e"), ZERO, HALF_GIG + (4 * ONE_MEG), 0),
        t(k("f"), ZERO, HALF_GIG + (5 * ONE_MEG), 0), t(k("g"), ZERO, ONE_GIG, 0),
        t(k("h"), ZERO, 0, HALF_GIG + (7 * ONE_MEG)), t(k("i"), ZERO, 0, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(k("g"), tabletsToMinorCompact.get(0));
    // memory is very full, lots of candidates
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, HALF_GIG, 0),
        t(k("b"), ZERO, ONE_GIG + ONE_MEG, 0), t(k("c"), ZERO, ONE_GIG + (2 * ONE_MEG), 0),
        t(k("d"), ZERO, ONE_GIG + (3 * ONE_MEG), 0), t(k("e"), ZERO, ONE_GIG + (4 * ONE_MEG), 0),
        t(k("f"), ZERO, ONE_GIG + (5 * ONE_MEG), 0), t(k("g"), ZERO, ONE_GIG + (6 * ONE_MEG), 0),
        t(k("h"), ZERO, 0, 0), t(k("i"), ZERO, 0, 0)));
    assertEquals(2, tabletsToMinorCompact.size());
    assertEquals(k("g"), tabletsToMinorCompact.get(0));
    assertEquals(k("f"), tabletsToMinorCompact.get(1));
    // only have two compactors, still busy
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, HALF_GIG, 0),
        t(k("b"), ZERO, ONE_GIG + ONE_MEG, 0), t(k("c"), ZERO, ONE_GIG + (2 * ONE_MEG), 0),
        t(k("d"), ZERO, ONE_GIG + (3 * ONE_MEG), 0), t(k("e"), ZERO, ONE_GIG + (4 * ONE_MEG), 0),
        t(k("f"), ZERO, ONE_GIG, ONE_GIG + (5 * ONE_MEG)),
        t(k("g"), ZERO, ONE_GIG, ONE_GIG + (6 * ONE_MEG)), t(k("h"), ZERO, 0, 0),
        t(k("i"), ZERO, 0, 0)));
    assertEquals(0, tabletsToMinorCompact.size());
    // finished one
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, HALF_GIG, 0),
        t(k("b"), ZERO, ONE_GIG + ONE_MEG, 0), t(k("c"), ZERO, ONE_GIG + (2 * ONE_MEG), 0),
        t(k("d"), ZERO, ONE_GIG + (3 * ONE_MEG), 0), t(k("e"), ZERO, ONE_GIG + (4 * ONE_MEG), 0),
        t(k("f"), ZERO, ONE_GIG, ONE_GIG + (5 * ONE_MEG)), t(k("g"), ZERO, ONE_GIG, 0),
        t(k("h"), ZERO, 0, 0), t(k("i"), ZERO, 0, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(k("e"), tabletsToMinorCompact.get(0));

    // many are running: do nothing
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(context);
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, HALF_GIG, 0),
        t(k("b"), ZERO, HALF_GIG + ONE_MEG, 0), t(k("c"), ZERO, HALF_GIG + (2 * ONE_MEG), 0),
        t(k("d"), ZERO, 0, HALF_GIG), t(k("e"), ZERO, 0, HALF_GIG), t(k("f"), ZERO, 0, HALF_GIG),
        t(k("g"), ZERO, 0, HALF_GIG), t(k("i"), ZERO, 0, HALF_GIG), t(k("j"), ZERO, 0, HALF_GIG),
        t(k("k"), ZERO, 0, HALF_GIG), t(k("l"), ZERO, 0, HALF_GIG), t(k("m"), ZERO, 0, HALF_GIG)));
    assertEquals(0, tabletsToMinorCompact.size());

    // observe adjustment:
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(context);
    // compact the largest
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, QGIG, 0),
        t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, QGIG + 2, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(k("c"), tabletsToMinorCompact.get(0));
    // show that it is compacting... do nothing
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, QGIG, 0),
        t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, 0, QGIG + 2)));
    assertEquals(0, tabletsToMinorCompact.size());
    // not going to bother compacting any more
    mgr.currentTime += MINUTES.toMillis(1);
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(k("a"), ZERO, QGIG, 0),
        t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, 0, QGIG + 2)));
    assertEquals(0, tabletsToMinorCompact.size());
    // now do nothing
    mgr.currentTime += MINUTES.toMillis(1);
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(
        tablets(t(k("a"), ZERO, QGIG, 0), t(k("b"), ZERO, 0, 0), t(k("c"), ZERO, 0, 0)));
    assertEquals(0, tabletsToMinorCompact.size());
    // on no! more data, this time we compact because we've adjusted
    mgr.currentTime += MINUTES.toMillis(1);
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(
        tablets(t(k("a"), ZERO, QGIG, 0), t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, 0, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(k("b"), tabletsToMinorCompact.get(0));
  }

  @Test
  public void testDeletedTable() {
    final String deletedTableId = "1";
    final String beingDeleted = "2";
    Predicate<TableId> existenceCheck =
        tableId -> !deletedTableId.contentEquals(tableId.canonical());
    Predicate<TableId> deletingCheck = tableId -> beingDeleted.contentEquals(tableId.canonical());
    LargestFirstMemoryManagerWithExistenceCheck mgr =
        new LargestFirstMemoryManagerWithExistenceCheck(existenceCheck, deletingCheck);

    mgr.init(context);
    List<KeyExtent> tabletsToMinorCompact;
    // one tablet is really big and the other is for a nonexistent table
    KeyExtent extent = new KeyExtent(TableId.of("3"), new Text("j"), null);
    KeyExtent extent2 = new KeyExtent(TableId.of("2"), new Text("j"), null);
    tabletsToMinorCompact = mgr.tabletsToMinorCompact(tablets(t(extent, ZERO, ONE_GIG, 0),
        t(extent2, ZERO, ONE_GIG, 0), t(k("j"), ZERO, ONE_GIG, 0)));
    assertEquals(1, tabletsToMinorCompact.size());
    assertEquals(extent, tabletsToMinorCompact.get(0));
  }

  private static class LargestFirstMemoryManagerUnderTest extends LargestFirstMemoryManager {

    public long currentTime = ZERO;

    @Override
    protected long currentTimeMillis() {
      return currentTime;
    }

    @Override
    protected long getMinCIdleThreshold(KeyExtent extent) {
      return MINUTES.toMillis(15);
    }

    @Override
    protected boolean tableExists(TableId tableId) {
      return true;
    }

    @Override
    protected boolean tableBeingDeleted(TableId tableId) {
      return false;
    }
  }

  private static class LargestFirstMemoryManagerWithExistenceCheck
      extends LargestFirstMemoryManagerUnderTest {

    Predicate<TableId> existenceCheck;
    Predicate<TableId> deletingCheck;

    public LargestFirstMemoryManagerWithExistenceCheck(Predicate<TableId> existenceCheck,
        Predicate<TableId> deletingCheck) {
      this.existenceCheck = existenceCheck;
      this.deletingCheck = deletingCheck;
    }

    @Override
    protected boolean tableExists(TableId tableId) {
      return existenceCheck.test(tableId);
    }

    @Override
    protected boolean tableBeingDeleted(TableId tableId) {
      return deletingCheck.test(tableId);
    }
  }

  private static KeyExtent k(String endRow) {
    return new KeyExtent(TableId.of("1"), new Text(endRow), null);
  }

  private TabletMemoryReport t(KeyExtent ke, long lastCommit, long memSize, long compactingSize) {
    return new TabletMemoryReport(null, lastCommit, memSize, compactingSize) {
      @Override
      public KeyExtent getExtent() {
        return ke;
      }
    };
  }

  private static List<TabletMemoryReport> tablets(TabletMemoryReport... states) {
    return Arrays.asList(states);
  }

}
