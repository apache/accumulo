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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.util.CountDownTimer;
import org.apache.accumulo.manager.EventCoordinator.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Event queue that collapses events when possible.
 */
public class EventQueue {

  private static final Logger log = LoggerFactory.getLogger(EventQueue.class);
  private boolean allLevels = false;

  private static class Table {
    final TableId tableId;
    boolean allExtents = false;
    Map<KeyExtent,Event> extents = new HashMap<>();

    private Table(TableId tableId) {
      this.tableId = tableId;
    }

    public void add(Event event) {
      if (allExtents) {
        return;
      }

      if (event.getScope() == EventCoordinator.EventScope.TABLE) {
        allExtents = true;
        extents.clear();
      } else {
        Preconditions.checkArgument(event.getScope() == EventCoordinator.EventScope.TABLE_RANGE);
        extents.put(event.getExtent(), event);
        if (extents.size() > 10_000) {
          allExtents = true;
          extents.clear();
        }
      }
    }

    public void fill(List<Event> events) {
      if (allExtents) {
        events.add(new Event(tableId));
      } else {
        events.addAll(extents.values());
      }
    }
  }

  private static class Level {
    final Ample.DataLevel dataLevel;
    boolean allTables = false;
    Map<TableId,Table> tables = new HashMap<>();

    private Level(Ample.DataLevel dataLevel) {
      this.dataLevel = dataLevel;
    }

    void add(Event event) {
      if (allTables) {
        return;
      }

      if (event.getScope() == EventCoordinator.EventScope.DATA_LEVEL) {
        allTables = true;
        tables.clear();
      } else {
        var table = tables.computeIfAbsent(event.getTableId(), Table::new);
        table.add(event);
      }
    }

    public void fill(List<Event> events) {
      if (allTables) {
        events.add(new Event(dataLevel));
      } else {
        tables.values().forEach(table -> table.fill(events));
      }
    }
  }

  private HashMap<Ample.DataLevel,Level> levels = new HashMap<>();

  public synchronized void add(Event event) {
    if (allLevels) {
      return;
    }

    if (event.getScope() == EventCoordinator.EventScope.ALL) {
      allLevels = true;
      levels.clear();
    } else {
      var level = levels.computeIfAbsent(event.getLevel(), Level::new);
      level.add(event);
    }
    notify();
  }

  private static final List<Event> ALL_LEVELS = List.of(new Event());

  public synchronized List<Event> poll(long duration, TimeUnit timeUnit)
      throws InterruptedException {
    CountDownTimer timer = CountDownTimer.startNew(duration, timeUnit);
    while (!allLevels && levels.isEmpty() && !timer.isExpired()) {
      wait(Math.max(1, timer.timeLeft(TimeUnit.MILLISECONDS)));
    }

    List<Event> events;
    if (allLevels) {
      events = ALL_LEVELS;
    } else {
      events = new ArrayList<>();
      levels.values().forEach(l -> l.fill(events));
    }

    // reset back to empty
    allLevels = false;
    levels.clear();

    return events;
  }

  public List<Event> take() throws InterruptedException {
    return poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
  }
}
