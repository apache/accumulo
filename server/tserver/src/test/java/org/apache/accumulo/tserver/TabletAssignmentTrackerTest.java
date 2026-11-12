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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

  private static void assertState(TabletAssignmentTracker tracker, Set<KeyExtent> unopened,
      Set<KeyExtent> opening, Set<KeyExtent> online) {
    Set<KeyExtent> unopenedAndOpening = new HashSet<>(unopened);
    unopenedAndOpening.addAll(opening);
    assertEquals(unopenedAndOpening, Set.copyOf(tracker.getUnopenedAndOpening()));
    assertEquals(unopened.size(), tracker.getUnopenedCount());
    assertEquals(opening.size(), tracker.getOpeningCount());
    assertEquals(opening, tracker.getOpening());
    assertEquals(online, tracker.getOnline().keySet());
    assertEquals(online, tracker.getOnline().keySet());
  }

  @Test
  public void testLifecycle() {
    var tracker = new TabletAssignmentTracker();

    assertTrue(tracker.addUnopenedIfNoOverlap(ALL).isEmpty());
    assertState(tracker, Set.of(ALL), Set.of(), Set.of());

    // overlapping assignments are rejected
    var overlapping = tracker.addUnopenedIfNoOverlap(LOW);
    assertEquals(Set.of(ALL), overlapping.unopened);
    assertEquals(Set.of(), overlapping.opening);
    assertEquals(Map.of(), overlapping.online);
    assertState(tracker, Set.of(ALL), Set.of(), Set.of());

    assertTrue(tracker.unopenedToOpening(ALL));
    assertFalse(tracker.unopenedToOpening(ALL));
    assertState(tracker, Set.of(), Set.of(ALL), Set.of());

    Tablet tablet = tablet(ALL);
    tracker.openingToOnline(ALL, tablet);
    assertState(tracker, Set.of(), Set.of(), Set.of(ALL));
    assertSame(tablet, tracker.getOnline().get(ALL));

    overlapping = tracker.addUnopenedIfNoOverlap(HIGH);
    assertEquals(Set.of(), overlapping.unopened);
    assertEquals(Set.of(), overlapping.opening);
    assertEquals(Map.of(ALL, tablet), overlapping.online);

    tracker.split(ALL, tablet(LOW), tablet(HIGH));
    assertState(tracker, Set.of(), Set.of(), Set.of(LOW, HIGH));

    tracker.removeOnline(LOW);
    tracker.removeOnline(HIGH);
    assertState(tracker, Set.of(), Set.of(), Set.of());
  }

  @Test
  public void testRetry() {
    var tracker = new TabletAssignmentTracker();
    assertTrue(tracker.addUnopenedIfNoOverlap(ALL).isEmpty());
    assertTrue(tracker.unopenedToOpening(ALL));

    tracker.openingToUnopened(ALL, LOW);
    assertState(tracker, Set.of(LOW), Set.of(), Set.of());

    assertTrue(tracker.removeUnopened(LOW));
    assertFalse(tracker.removeUnopened(LOW));
    assertState(tracker, Set.of(), Set.of(), Set.of());
    // assignment was canceled by an unload before it ran
    assertFalse(tracker.unopenedToOpening(LOW));

    // a rolled back split can put an extent in unopened that overlaps another unopened extent
    assertTrue(tracker.addUnopenedIfNoOverlap(HIGH).isEmpty());
    assertTrue(tracker.addUnopenedIfNoOverlap(LOW).isEmpty());
    assertTrue(tracker.unopenedToOpening(LOW));
    tracker.openingToUnopened(LOW, ALL);
    assertState(tracker, Set.of(HIGH, ALL), Set.of(), Set.of());
    assertThrows(IllegalStateException.class, () -> tracker.unopenedToOpening(ALL));
  }

  @Test
  @Timeout(30)
  public void testAwaitNotOpening() throws Exception {
    var tracker = new TabletAssignmentTracker();
    assertTrue(tracker.addUnopenedIfNoOverlap(ALL).isEmpty());
    assertTrue(tracker.unopenedToOpening(ALL));

    Thread waiter = new Thread(() -> tracker.awaitNotOpening(ALL));
    waiter.start();
    while (waiter.getState() != Thread.State.WAITING) {
      Thread.sleep(10);
    }

    tracker.removeOpening(ALL);
    waiter.join();
    assertState(tracker, Set.of(), Set.of(), Set.of());
  }

  @Test
  // on a livelock removeOpening blocks on the monitor, which only a separate thread can time out
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  public void testAwaitNotOpeningInterrupted() throws Exception {
    var tracker = new TabletAssignmentTracker();
    assertTrue(tracker.addUnopenedIfNoOverlap(ALL).isEmpty());
    assertTrue(tracker.unopenedToOpening(ALL));

    var stillInterrupted = new AtomicBoolean();
    Thread waiter = new Thread(() -> {
      // interrupt before waiting so the first wait() throws while holding the monitor
      Thread.currentThread().interrupt();
      tracker.awaitNotOpening(ALL);
      stillInterrupted.set(Thread.currentThread().isInterrupted());
    });
    waiter.setDaemon(true);
    waiter.start();
    while (waiter.getState() != Thread.State.WAITING) {
      Thread.sleep(10);
    }

    tracker.removeOpening(ALL);
    waiter.join();
    assertTrue(stillInterrupted.get());
    assertState(tracker, Set.of(), Set.of(), Set.of());
  }

  @Test
  public void testReturnsAreImmutable() {
    var tracker = new TabletAssignmentTracker();
    assertTrue(tracker.addUnopenedIfNoOverlap(LOW).isEmpty());
    assertTrue(tracker.unopenedToOpening(LOW));
    tracker.openingToOnline(LOW, tablet(LOW));
    assertTrue(tracker.addUnopenedIfNoOverlap(HIGH).isEmpty());
    assertTrue(tracker.unopenedToOpening(HIGH));

    var overlapping = tracker.addUnopenedIfNoOverlap(ALL);
    assertThrows(UnsupportedOperationException.class, overlapping.unopened::clear);
    assertThrows(UnsupportedOperationException.class, overlapping.opening::clear);
    assertThrows(UnsupportedOperationException.class, overlapping.online::clear);

    assertThrows(UnsupportedOperationException.class, tracker.getOpening()::clear);
    assertThrows(UnsupportedOperationException.class, tracker.getOnline()::clear);
    assertThrows(UnsupportedOperationException.class, () -> tracker.getOnline().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> tracker.getUnopenedAndOpening().clear());
  }
}
