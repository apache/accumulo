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

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class EventCoordinator {

  private static final Logger log = LoggerFactory.getLogger(EventCoordinator.class);

  private long eventCounter = 0;

  synchronized long waitForEvents(long millis, long lastEvent) {
    // Did something happen since the last time we waited?
    if (lastEvent == eventCounter) {
      // no
      if (millis <= 0) {
        return eventCounter;
      }
      try {
        wait(millis);
      } catch (InterruptedException e) {
        log.debug("ignoring InterruptedException", e);
      }
    }
    return eventCounter;
  }

  private final Map<Ample.DataLevel,Listener> listeners = new EnumMap<>(Ample.DataLevel.class);

  public enum EventScope {
    ALL, DATA_LEVEL, TABLE, TABLE_RANGE
  }

  public static class Event {

    private final EventScope scope;
    private final Ample.DataLevel level;
    private final KeyExtent extent;

    Event(EventScope scope, KeyExtent extent) {
      this.scope = scope;
      this.level = Ample.DataLevel.of(extent.tableId());
      this.extent = extent;
    }

    Event(EventScope scope, TableId tableId) {
      this.scope = scope;
      this.level = Ample.DataLevel.of(tableId);
      this.extent = new KeyExtent(tableId, null, null);
    }

    Event(EventScope scope, Ample.DataLevel level) {
      this.scope = scope;
      this.level = level;
      this.extent = null;
    }

    Event() {
      this.scope = EventScope.ALL;
      this.level = null;
      this.extent = null;
    }

    public EventScope getScope() {
      return scope;
    }

    public Ample.DataLevel getLevel() {
      Preconditions.checkState(scope != EventScope.ALL);
      return level;
    }

    public TableId getTableId() {
      Preconditions.checkState(scope == EventScope.TABLE || scope == EventScope.TABLE_RANGE);
      return extent.tableId();
    }

    public KeyExtent getExtent() {
      Preconditions.checkState(scope == EventScope.TABLE || scope == EventScope.TABLE_RANGE);
      return extent;
    }
  }

  public void event(String msg, Object... args) {
    log.info(String.format(msg, args));
    publish(new Event());
  }

  public void event(Ample.DataLevel level, String msg, Object... args) {
    log.info(String.format(msg, args));
    publish(new Event(EventScope.DATA_LEVEL, level));
  }

  public void event(TableId tableId, String msg, Object... args) {
    log.info(String.format(msg, args));
    publish(new Event(EventScope.TABLE, tableId));
  }

  public void event(KeyExtent extent, String msg, Object... args) {
    log.debug(String.format(msg, args));
    publish(new Event(EventScope.TABLE_RANGE, extent));
  }

  public void event(Collection<KeyExtent> extents, String msg, Object... args) {
    if (!extents.isEmpty()) {
      log.debug(String.format(msg, args));
      extents.forEach(extent -> publish(new Event(EventScope.TABLE_RANGE, extent)));
    }
  }

  private synchronized void publish(Event event) {
    if (event.getScope() == EventScope.ALL) {
      listeners.values().forEach(listener -> listener.process(event));
    } else {
      listeners.getOrDefault(event.getLevel(), e -> {}).process(event);
    }

    eventCounter++;
    notifyAll();
  }

  public interface Listener {
    void process(Event event);
  }

  public synchronized void addListener(Ample.DataLevel level, Listener listener) {
    // Currently only expecting one listener for each level, so keeping the code simple and
    // detecting deviations. Can adept if needed.
    Preconditions.checkState(listeners.put(level, listener) == null);
  }

  public Tracker getTracker() {
    return new Tracker();
  }

  /**
   * Tracks the event counter and helps detect changes in it.
   */
  public class Tracker {
    long lastEvent;

    Tracker() {
      lastEvent = eventCounter;
    }

    public void waitForEvents(long millis) {
      lastEvent = EventCoordinator.this.waitForEvents(millis, lastEvent);
    }
  }

}
