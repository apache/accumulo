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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.manager.EventCoordinator.Event;
import org.apache.accumulo.manager.EventCoordinator.EventScope;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class EventQueueTest {
  @Test
  public void testAll() throws Exception {
    EventQueue eventQueue = new EventQueue();

    var tableId1 = TableId.of("1");
    var tableId2 = TableId.of("2");
    var tableId3 = TableId.of("3");

    var extent1a = new KeyExtent(tableId1, new Text("m"), null);
    var extent1b = new KeyExtent(tableId1, null, new Text("m"));
    var extent2 = new KeyExtent(tableId2, null, null);

    List<Event> eventsToAdd1 = new ArrayList<>();
    eventsToAdd1.add(new Event());
    eventsToAdd1.add(new Event(DataLevel.METADATA));
    eventsToAdd1.add(new Event(tableId1));
    eventsToAdd1.add(new Event(tableId2));
    eventsToAdd1.add(new Event(extent1a));
    eventsToAdd1.add(new Event(extent1b));
    eventsToAdd1.add(new Event(extent2));

    List<Event> eventsToAdd2 =
        new ArrayList<>(List.of(new Event(DataLevel.METADATA), new Event(tableId2),
            new Event(extent1a), new Event(extent1b), new Event(extent2), new Event(tableId3)));
    List<Event> eventsToAdd3 = new ArrayList<>(List.of(new Event(DataLevel.USER),
        new Event(tableId2), new Event(extent1a), new Event(extent1b), new Event(extent2)));
    for (int i = 0; i < 10; i++) {
      // should see the same result for events added in any order
      Collections.shuffle(eventsToAdd1);
      eventsToAdd1.forEach(eventQueue::add);
      var events = eventQueue.take();
      assertEquals(1, events.size());
      assertEquals(EventScope.ALL, events.get(0).getScope());

      Collections.shuffle(eventsToAdd2);
      eventsToAdd2.forEach(eventQueue::add);
      events = eventQueue.take();
      assertEquals(5, events.size());
      assertTrue(events.stream().anyMatch(
          e -> e.getScope() == EventScope.DATA_LEVEL && e.getLevel() == DataLevel.METADATA));
      assertTrue(events.stream().anyMatch(e -> e.getScope() == EventScope.TABLE
          && e.getLevel() == DataLevel.USER && e.getTableId().equals(tableId2)));
      assertTrue(events.stream().anyMatch(e -> e.getScope() == EventScope.TABLE
          && e.getLevel() == DataLevel.USER && e.getTableId().equals(tableId3)));
      assertTrue(events.stream()
          .anyMatch(e -> e.getScope() == EventScope.TABLE_RANGE && e.getLevel() == DataLevel.USER
              && e.getTableId().equals(tableId1) && e.getExtent().equals(extent1a)));
      assertTrue(events.stream()
          .anyMatch(e -> e.getScope() == EventScope.TABLE_RANGE && e.getLevel() == DataLevel.USER
              && e.getTableId().equals(tableId1) && e.getExtent().equals(extent1b)));

      Collections.shuffle(eventsToAdd3);
      eventsToAdd3.forEach(eventQueue::add);
      events = eventQueue.take();
      assertEquals(1, events.size());
      assertTrue(events.stream()
          .anyMatch(e -> e.getScope() == EventScope.DATA_LEVEL && e.getLevel() == DataLevel.USER));

      assertEquals(0, eventQueue.poll(1, TimeUnit.MILLISECONDS).size());
    }

  }
}
