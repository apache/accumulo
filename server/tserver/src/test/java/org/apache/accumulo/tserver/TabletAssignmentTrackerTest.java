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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletAssignmentTrackerTest {

  private static final TableId TID = TableId.of("1");
  private static final KeyExtent ALL = new KeyExtent(TID, null, null);
  private static final KeyExtent LOW = new KeyExtent(TID, new Text("m"), null);
  private static final KeyExtent HIGH = new KeyExtent(TID, null, new Text("m"));

  private static Tablet tablet(KeyExtent extent) {
    Tablet tablet = createMock(Tablet.class);
    expect(tablet.getExtent()).andReturn(extent).anyTimes();
    replay(tablet);
    return tablet;
  }

  @Test
  public void testLifecycle() {
    var tracker = new TabletAssignmentTracker();

    assertNull(tracker.addUnopenedIfNoOverlap(ALL));
    assertEquals(1, tracker.getUnopenedCount());

    // overlapping assignments are rejected
    var overlapping = tracker.addUnopenedIfNoOverlap(LOW);
    assertNotNull(overlapping);
    assertEquals(Set.of(ALL), overlapping.unopened);
    assertEquals(1, tracker.getUnopenedCount());

    assertTrue(tracker.unopenedToOpening(ALL));
    assertFalse(tracker.unopenedToOpening(ALL));
    assertEquals(0, tracker.getUnopenedCount());
    assertEquals(1, tracker.getOpeningCount());
    assertEquals(List.of(ALL), tracker.getUnopenedAndOpening());
    assertEquals(Set.of(ALL), tracker.getOpeningAndOnline().opening);

    Tablet tablet = tablet(ALL);
    tracker.openingToOnline(ALL, tablet);
    assertEquals(0, tracker.getOpeningCount());
    assertSame(tablet, tracker.getOnline().get(ALL));
    assertEquals(Map.of(ALL, tablet), tracker.addUnopenedIfNoOverlap(HIGH).online);

    tracker.split(ALL, tablet(LOW), tablet(HIGH));
    assertEquals(Set.of(LOW, HIGH), tracker.getOnline().keySet());

    tracker.removeOnline(LOW);
    tracker.removeOnline(HIGH);
    assertTrue(tracker.getOnline().isEmpty());
  }

  @Test
  public void testRetry() {
    var tracker = new TabletAssignmentTracker();
    assertNull(tracker.addUnopenedIfNoOverlap(ALL));
    assertTrue(tracker.unopenedToOpening(ALL));

    tracker.openingToUnopened(ALL, LOW);
    assertEquals(List.of(LOW), tracker.getUnopenedAndOpening());

    assertTrue(tracker.removeUnopened(LOW));
    assertFalse(tracker.removeUnopened(LOW));
    // assignment was canceled by an unload before it ran
    assertFalse(tracker.unopenedToOpening(LOW));

    // a rolled back split can put an extent in unopened that overlaps another unopened extent
    assertNull(tracker.addUnopenedIfNoOverlap(HIGH));
    assertNull(tracker.addUnopenedIfNoOverlap(LOW));
    assertTrue(tracker.unopenedToOpening(LOW));
    tracker.openingToUnopened(LOW, ALL);
    assertThrows(IllegalStateException.class, () -> tracker.unopenedToOpening(ALL));
  }

  @Test
  public void testAwaitNotOpening() throws Exception {
    var tracker = new TabletAssignmentTracker();
    assertNull(tracker.addUnopenedIfNoOverlap(ALL));
    assertTrue(tracker.unopenedToOpening(ALL));

    Thread waiter = new Thread(() -> tracker.awaitNotOpening(ALL));
    waiter.start();
    waiter.join(100);
    assertTrue(waiter.isAlive());

    tracker.removeOpening(ALL);
    waiter.join(10_000);
    assertFalse(waiter.isAlive());
  }
}
