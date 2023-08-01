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

import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.NEEDS_COMPACTING;
import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.NEEDS_LOCATION_UPDATE;
import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.NEEDS_SPLITTING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.manager.EventCoordinator.EventScope;
import org.apache.accumulo.server.manager.state.ClosableIterable;
import org.apache.accumulo.server.manager.state.ClosableIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.MoreCollectors;

public class TabletGroupWatcherTest {

  private KeyExtent newExtent(String tableId, String endRow, String prevEndRow) {
    return new KeyExtent(TableId.of(tableId), endRow == null ? null : new Text(endRow),
        prevEndRow == null ? null : new Text(prevEndRow));
  }

  private TabletManagement newTabletManagement(String tableId,
      TabletManagement.ManagementAction action) {
    KeyExtent extent1 = newExtent(tableId, null, null);
    TabletMetadata meta1 = TabletMetadata.builder(extent1).build();
    return new TabletManagement(Set.of(action), meta1);
  }

  static class ClosableTestIterator implements ClosableIterator<TabletManagement> {

    private final Iterator<TabletManagement> iter;

    private volatile boolean closed;

    ClosableTestIterator(Iterator<TabletManagement> iter) {
      this.iter = iter;
    }

    public boolean wasClosed() {
      return closed;
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public TabletManagement next() {
      return iter.next();
    }
  }

  TabletManagement mgmt1 = newTabletManagement("1", NEEDS_COMPACTING);
  TabletManagement mgmt2 = newTabletManagement("2", NEEDS_SPLITTING);
  TabletManagement mgmt3 = newTabletManagement("3", NEEDS_LOCATION_UPDATE);

  AtomicBoolean keepRunning;
  Map<Range,TabletManagement> paritialData;
  TabletGroupWatcher.IteratorManager iteratorManager;

  AtomicLong currentTime;

  TabletGroupWatcher.EventHandler eventHandler;

  volatile ClosableTestIterator lastFullIterator;

  @BeforeEach
  public void setup() {

    keepRunning = new AtomicBoolean(true);

    paritialData = new ConcurrentHashMap<>();

    // This is the store used to lookup tablet management actions based on events.
    Function<List<Range>,ClosableIterator<TabletManagement>> paritialStore = ranges -> {
      ArrayList<TabletManagement> found = new ArrayList<>();
      for (var range : ranges) {
        if (paritialData.containsKey(range)) {
          found.add(paritialData.get(range));
        } else {
          System.out.println("Did not find " + range);
        }
      }

      return new ClosableTestIterator(found.iterator());
    };

    eventHandler = new TabletGroupWatcher.EventHandler(keepRunning::get, paritialStore);

    // This simulates the iterator for a full scan of the metadata table
    ClosableIterable<TabletManagement> fullStore = () -> {
      lastFullIterator = new ClosableTestIterator(List.of(mgmt1, mgmt2, mgmt3).iterator());
      return lastFullIterator;
    };

    currentTime = new AtomicLong(10);

    iteratorManager =
        new TabletGroupWatcher.IteratorManager(eventHandler, fullStore, Ample.DataLevel.USER) {
          @Override
          long getCurrentTime() {
            return currentTime.get();
          }
        };

  }

  @AfterEach
  public void cleanup() {
    if (keepRunning != null) {
      // shutdown the background thread in event handler
      keepRunning.set(false);
    }
  }

  private void checkData(List<TabletManagement> expected, Iterator<TabletManagement> iter) {
    List<TabletManagement> seen = new ArrayList<>();
    iter.forEachRemaining(seen::add);

    Function<TabletManagement,KeyExtent> toExtent = tm -> tm.getTabletMetadata().getExtent();

    assertEquals(expected.stream().map(toExtent).collect(toList()),
        seen.stream().map(toExtent).collect(toList()));

    Function<TabletManagement,TabletManagement.ManagementAction> toAction =
        tm -> tm.getActions().stream().collect(MoreCollectors.onlyElement());

    assertEquals(expected.stream().map(toAction).collect(toList()),
        seen.stream().map(toAction).collect(toList()));

  }

  @Test
  public void testFullScan() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    assertFalse(lastFullIterator.wasClosed());
    iteratorManager.endScan();
    assertTrue(lastFullIterator.wasClosed());

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    currentTime.addAndGet(3);
    // time advanced, but should not be enough to cause a full scan
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    currentTime.addAndGet(3);
    // time should have advanced enough to cause another full scan
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    assertFalse(lastFullIterator.wasClosed());
    iteratorManager.endScan();
    assertTrue(lastFullIterator.wasClosed());

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    currentTime.addAndGet(3);

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // an event should cause another full scan
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    currentTime.addAndGet(3);
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // time advancing should cause another full scan
    currentTime.addAndGet(3);
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testEventAllDuringFullScan() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // an event should cause another full scan
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    iter = iteratorManager.beginScan(5);
    assertEquals(mgmt1.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    // an event happening in the middle of scan should cause the next round to do a full scan
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    assertEquals(mgmt2.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    assertEquals(mgmt3.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // should do a full scan because of the event that happened in the middle of the last scan
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // This should not be enough time to cause another full scan
    currentTime.addAndGet(2);

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testEventTableDuringFullScan() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // an table event in the middle of a full scan should be seen
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    iter = iteratorManager.beginScan(5);
    assertEquals(mgmt1.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    // create a table event and pass it to the event handler
    var mgmt4 = newTabletManagement("4", NEEDS_SPLITTING);
    processEvents(mgmt4);
    // event handler has a background thread that processes range events, so wait for it to be done
    eventHandler.waitForEvents(3000);
    // the table event should be injected and seen by the scan
    checkData(List.of(mgmt4, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // ensure the procssing of a single event does not interfere with a full scan in anyway when its
    // needed
    currentTime.addAndGet(6);
    // based on the time since last full scan, should do another full scan
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testTableEventNoFullScan() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    var mgmt4 = newTabletManagement("4", NEEDS_SPLITTING);
    processEvents(mgmt4);
    // event handler has a background thread that processes range events, so wait for it to be done
    eventHandler.waitForEvents(3000);

    // should not do a full scan, should only scan the data related to the table event
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt4), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // ensure the procssing of a single event does not interfere with a full scan in anyway when its
    // needed
    currentTime.addAndGet(6);
    // based on the time since last full scan, should do another full scan
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testMultipleTableEventsDuringFullScan() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // an table event in the middle of a full scan should be seen
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    iter = iteratorManager.beginScan(5);
    assertEquals(mgmt1.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    // create a table event and pass it to the event handler
    var mgmt4 = newTabletManagement("4", NEEDS_SPLITTING);
    var mgmt5 = newTabletManagement("5", NEEDS_LOCATION_UPDATE);
    var mgmt6 = newTabletManagement("6", NEEDS_COMPACTING);
    var mgmt7 = newTabletManagement("7", NEEDS_SPLITTING);
    var mgmt8 = newTabletManagement("8", NEEDS_COMPACTING);
    var mgmt9 = newTabletManagement("9", NEEDS_LOCATION_UPDATE);
    processEvents(mgmt4, mgmt5, mgmt6, mgmt7, mgmt8, mgmt9);
    // event handler has a background thread that processes range events, so wait for it to be done
    eventHandler.waitForEvents(3000);
    // Should see the table events interleaved with full scan entries. Once the full scan is done,
    // this scan will finish even though there are more events to process. This allows another full
    // scan to start if needed.
    checkData(List.of(mgmt4, mgmt5, mgmt2, mgmt6, mgmt7, mgmt3), iter);
    assertFalse(lastFullIterator.wasClosed());
    iteratorManager.endScan();
    assertTrue(lastFullIterator.wasClosed());

    // should not wait here at all, if so the test should timeout
    eventHandler.waitForEvents(360000);
    // there should be two events left over from the full scan that are ready for immediate
    // processing
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt8, mgmt9), iter);
    iteratorManager.endScan();

    // ensure the procssing of the events does not interfere with a full scan in anyway when its
    // needed
    currentTime.addAndGet(6);
    // based on the time since last full scan, should do another full scan
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testMultipleTableEventsAndAllEventDuringFullScan() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    // an table event in the middle of a full scan should be seen
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    iter = iteratorManager.beginScan(5);
    assertEquals(mgmt1.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    // create a table event and pass it to the event handler
    var mgmt4 = newTabletManagement("4", NEEDS_SPLITTING);
    var mgmt5 = newTabletManagement("5", NEEDS_LOCATION_UPDATE);
    var mgmt6 = newTabletManagement("6", NEEDS_COMPACTING);
    var mgmt7 = newTabletManagement("7", NEEDS_SPLITTING);
    var mgmt8 = newTabletManagement("8", NEEDS_COMPACTING);
    var mgmt9 = newTabletManagement("9", NEEDS_LOCATION_UPDATE);
    // this should cause another full scan after this full scan finishes
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    processEvents(mgmt4, mgmt5, mgmt6, mgmt7, mgmt8, mgmt9);
    // event handler has a background thread that processes range events, so wait for it to be done
    eventHandler.waitForEvents(3000);
    // Should see the table events interleaved with full scan entries. Once the full scan is done,
    // this scan will finish even though there are more events to process. This allows another full
    // scan to start if needed.
    checkData(List.of(mgmt4, mgmt5, mgmt2, mgmt6, mgmt7, mgmt3), iter);
    iteratorManager.endScan();

    // Should not wait here at all, if so the test should timeout
    eventHandler.waitForEvents(360000);
    // there should be two events left over from the last full scan that are ready for immediate
    // processing and another full scan should be needed
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt8, mgmt9, mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // ensure the procssing of the events does not interfere with a full scan in anyway when its
    // needed
    currentTime.addAndGet(6);
    // based on the time since last full scan, should do another full scan
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testEventScansStopsWhenFullScanSignaled() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    var mgmt4 = newTabletManagement("4", NEEDS_SPLITTING);
    var mgmt5 = newTabletManagement("5", NEEDS_LOCATION_UPDATE);
    var mgmt6 = newTabletManagement("6", NEEDS_COMPACTING);
    processEvents(mgmt4, mgmt5, mgmt6);
    eventHandler.waitForEvents(3000);

    // some events happened so should see them, should not see anything related a to a full scan
    iter = iteratorManager.beginScan(5);
    assertEquals(mgmt4.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    assertEquals(mgmt5.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    // This signal indicates a full scan is needed, which should cause the event only scan top stop
    // so the full scan can start.
    eventHandler.process(new EventCoordinator.Event(EventScope.ALL));
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    iter = iteratorManager.beginScan(5);
    // should see that last event that was not consumed in the previous scan and the full scan data
    checkData(List.of(mgmt6, mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testEventScansStopsWhenFullScanTimeElapses() throws Exception {
    // based on the time since last full scan, should do another full scan
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    var mgmt4 = newTabletManagement("4", NEEDS_SPLITTING);
    var mgmt5 = newTabletManagement("5", NEEDS_LOCATION_UPDATE);
    var mgmt6 = newTabletManagement("6", NEEDS_COMPACTING);
    processEvents(mgmt4, mgmt5, mgmt6);
    eventHandler.waitForEvents(3000);

    currentTime.addAndGet(3);

    // some events happened so should see them, should not see anything related a to a full scan
    iter = iteratorManager.beginScan(5);
    assertEquals(mgmt4.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    // This change in time should not cause the iterator to stop
    currentTime.addAndGet(1);
    assertEquals(mgmt5.getTabletMetadata().getExtent(),
        iter.next().getTabletMetadata().getExtent());
    // The change in time should cause the iterator to detect a full scan is now needed and stop.
    currentTime.addAndGet(2);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();

    iter = iteratorManager.beginScan(5);
    // should see that last event that was not consumed in the previous scan and the full scan data
    checkData(List.of(mgmt6, mgmt1, mgmt2, mgmt3), iter);
    iteratorManager.endScan();

    currentTime.addAndGet(4);

    // should not do another full scan because there were no events and not enough time has passed
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  @Test
  public void testScanCleanup() throws IOException {
    var iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    assertFalse(lastFullIterator.wasClosed());
    // when endScan() is not called, cleanup() should close the iterator
    iteratorManager.cleanup();
    assertTrue(lastFullIterator.wasClosed());

    // since scan did not finish, it should not have set the finish time so another full scan should
    // start
    iter = iteratorManager.beginScan(5);
    checkData(List.of(mgmt1, mgmt2, mgmt3), iter);
    assertFalse(lastFullIterator.wasClosed());
    iteratorManager.endScan();
    assertTrue(lastFullIterator.wasClosed());
    iteratorManager.cleanup();

    // full scan should not start now
    iter = iteratorManager.beginScan(5);
    assertFalse(iter.hasNext());
    iteratorManager.endScan();
  }

  private void processEvents(TabletManagement... mgmts) {
    for (var mgmt : mgmts) {
      var extent = mgmt.getTabletMetadata().getExtent();
      paritialData.put(extent.toMetaRange(), mgmt);
      eventHandler.process(new EventCoordinator.Event(EventScope.TABLE_RANGE, extent));
    }

    while (!eventHandler.isAllDataProcessed()) {
      UtilWaitThread.sleep(1);
    }
  }

}
