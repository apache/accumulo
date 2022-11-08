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
package org.apache.accumulo.tserver;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;

public class BusiestTrackerTest {

  private static BusiestTracker newTestTracker(int numToLog) {
    return BusiestTracker.newBusiestIngestTracker(numToLog);
  }

  private static Collection<Tablet> createTablets(Object... testData) {
    Preconditions.checkArgument(testData.length % 2 == 0);

    List<Tablet> data = new ArrayList<>();

    for (int i = 0; i < testData.length; i += 2) {
      String tableId = (String) testData[i];
      long count = (Long) testData[i + 1];

      Tablet tablet = createMock(Tablet.class);

      expect(tablet.getExtent()).andReturn(new KeyExtent(TableId.of(tableId), null, null))
          .anyTimes();
      expect(tablet.totalIngest()).andReturn(count).anyTimes();
      replay(tablet);

      data.add(tablet);
    }

    return data;
  }

  private static ComparablePair<Long,KeyExtent> newPair(String tableId, long count) {
    return new ComparablePair<>(count, new KeyExtent(TableId.of(tableId), null, null));
  }

  private static Set<ComparablePair<Long,KeyExtent>> createExpected(Object... testData) {
    Preconditions.checkArgument(testData.length % 2 == 0);

    Set<ComparablePair<Long,KeyExtent>> expected = new HashSet<>();

    for (int i = 0; i < testData.length; i += 2) {
      String tableId = (String) testData[i];
      long count = (Long) testData[i + 1];

      expected.add(newPair(tableId, count));
    }

    return expected;
  }

  @Test
  public void testNoChange() {
    BusiestTracker tracker = newTestTracker(3);

    Collection<Tablet> data1 = createTablets("e1", 5L, "e2", 0L);
    Collection<ComparablePair<Long,KeyExtent>> busy1 = tracker.computeBusiest(data1);
    assertEquals(createExpected("e1", 5L), new HashSet<>(busy1));

    Collection<ComparablePair<Long,KeyExtent>> busy2 = tracker.computeBusiest(data1);
    assertTrue(busy2.isEmpty());

    Collection<Tablet> data2 = createTablets("e1", 5L, "e2", 2L, "e3", 3L, "e4", 6L);
    Collection<ComparablePair<Long,KeyExtent>> busy3 = tracker.computeBusiest(data2);
    assertEquals(createExpected("e2", 2L, "e3", 3L, "e4", 6L), new HashSet<>(busy3));

    Collection<ComparablePair<Long,KeyExtent>> busy4 = tracker.computeBusiest(data2);
    assertTrue(busy4.isEmpty());
  }

  @Test
  public void testTruncate() {
    BusiestTracker tracker = newTestTracker(3);

    Collection<Tablet> data1 = createTablets("e1", 5L, "e2", 6L, "e3", 7L, "e4", 8L, "e5", 8L);
    Collection<ComparablePair<Long,KeyExtent>> busy1 = tracker.computeBusiest(data1);
    assertEquals(createExpected("e3", 7L, "e4", 8L, "e5", 8L), new HashSet<>(busy1));

    Collection<Tablet> data2 =
        createTablets("e1", 100L, "e2", 100L, "e3", 100L, "e4", 100L, "e5", 100L);
    Collection<ComparablePair<Long,KeyExtent>> busy2 = tracker.computeBusiest(data2);
    assertEquals(createExpected("e1", 100L - 5L, "e2", 100L - 6L, "e3", 100L - 7L),
        new HashSet<>(busy2));

    Collection<Tablet> data3 =
        createTablets("e1", 112L, "e2", 110L, "e3", 111L, "e4", 112L, "e5", 110L, "e6", 50L);
    Collection<ComparablePair<Long,KeyExtent>> busy3 = tracker.computeBusiest(data3);
    assertEquals(createExpected("e1", 112L - 100L, "e4", 112L - 100L, "e6", 50L),
        new HashSet<>(busy3));
  }

  @Test
  public void testReload() {
    BusiestTracker tracker = newTestTracker(3);

    Collection<Tablet> data1 = createTablets("e1", 5L, "e2", 3L);
    Collection<ComparablePair<Long,KeyExtent>> busy1 = tracker.computeBusiest(data1);
    assertEquals(createExpected("e1", 5L, "e2", 3L), new HashSet<>(busy1));

    // when count is less than previously seen, previous count should be ignored
    Collection<Tablet> data2 = createTablets("e1", 7L, "e2", 1L);
    Collection<ComparablePair<Long,KeyExtent>> busy2 = tracker.computeBusiest(data2);
    assertEquals(createExpected("e1", 2L, "e2", 1L), new HashSet<>(busy2));

    Collection<Tablet> data3 = createTablets("e1", 8L, "e2", 4L);
    Collection<ComparablePair<Long,KeyExtent>> busy3 = tracker.computeBusiest(data3);
    assertEquals(createExpected("e1", 1L, "e2", 3L), new HashSet<>(busy3));
  }

  @Test
  public void testReload2() {
    BusiestTracker tracker = newTestTracker(3);

    // This test differs from testReload because the tablet that has its count decrease does not
    // show up in busy1
    Collection<Tablet> data1 = createTablets("e1", 115L, "e2", 73L, "e3", 206L, "e4", 85L);
    Collection<ComparablePair<Long,KeyExtent>> busy1 = tracker.computeBusiest(data1);
    assertEquals(createExpected("e3", 206L, "e1", 115L, "e4", 85L), new HashSet<>(busy1));

    // the count for e2 decreases, simulating a tablet leaving a tserver and coming back
    Collection<Tablet> data2 = createTablets("e1", 118L, "e2", 20L, "e3", 216L, "e4", 89L);
    Collection<ComparablePair<Long,KeyExtent>> busy2 = tracker.computeBusiest(data2);
    assertEquals(createExpected("e2", 20L, "e3", 10L, "e4", 4L), new HashSet<>(busy2));

    Collection<Tablet> data3 = createTablets("e1", 118L, "e2", 21L, "e3", 218L, "e4", 89L);
    Collection<ComparablePair<Long,KeyExtent>> busy3 = tracker.computeBusiest(data3);
    assertEquals(createExpected("e2", 1L, "e3", 2L), new HashSet<>(busy3));
  }

  @Test
  public void testOrder() {
    BusiestTracker tracker = newTestTracker(3);

    Collection<Tablet> data1 = createTablets("e1", 5L, "e2", 6L, "e3", 13L, "e4", 8L, "e5", 9L);
    List<ComparablePair<Long,KeyExtent>> busy1 = tracker.computeBusiest(data1);
    assertEquals(3, busy1.size());
    assertEquals(newPair("e3", 13L), busy1.get(0));
    assertEquals(newPair("e5", 9L), busy1.get(1));
    assertEquals(newPair("e4", 8L), busy1.get(2));

    Collection<Tablet> data2 = createTablets("e1", 15L, "e2", 17L, "e3", 13L, "e4", 20L, "e5", 9L);
    List<ComparablePair<Long,KeyExtent>> busy2 = tracker.computeBusiest(data2);
    assertEquals(3, busy2.size());
    assertEquals(newPair("e4", 12L), busy2.get(0));
    assertEquals(newPair("e2", 11L), busy2.get(1));
    assertEquals(newPair("e1", 10L), busy2.get(2));
  }
}
