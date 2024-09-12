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
package org.apache.accumulo.monitor.util.logging;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.accumulo.monitor.rest.logs.LogResource;
import org.apache.accumulo.monitor.rest.logs.SanitizedLogEvent;
import org.apache.accumulo.monitor.rest.logs.SingleLogEvent;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * A recent logs cache for the monitor that holds log messages received from
 * {@link AccumuloMonitorAppender} instances at the monitor's REST endpoint
 * {@link LogResource#append(SingleLogEvent)} until retrieved at the REST endpoint
 * {@link LogResource#getRecentLogs()} or cleared via the REST endpoint
 * {@link LogResource#clearLogs()}.
 */
public class RecentLogs {

  private static final int MAX_LOGS = 50;

  private final Cache<String,DedupedEvent> eventsCache =
      Caffeine.newBuilder().maximumSize(MAX_LOGS).build();
  private final ConcurrentMap<String,DedupedEvent> events = eventsCache.asMap();

  /**
   * Internal class for keeping the current count and most recent event that matches a given cache
   * key (derived from the event's application, logger, level, and message fields).
   */
  private static class DedupedEvent {
    private final SingleLogEvent event;
    private final AtomicInteger count;

    private DedupedEvent(SingleLogEvent event) {
      this.event = event;
      this.count = new AtomicInteger();
    }
  }

  public void addEvent(SingleLogEvent event) {
    String key = event.application + ":" + event.logger + ":" + event.level + ":" + event.message;
    events.computeIfAbsent(key, k -> new DedupedEvent(event)).count.incrementAndGet();
  }

  public void clearEvents() {
    events.clear();
  }

  public int numEvents() {
    return events.size();
  }

  public boolean eventsIncludeErrors() {
    return events.values().stream().anyMatch(
        x -> x.event.level.equalsIgnoreCase("ERROR") || x.event.level.equalsIgnoreCase("FATAL"));
  }

  public List<SanitizedLogEvent> getSanitizedEvents() {
    return events.values().stream().map(ev -> new SanitizedLogEvent(ev.event, ev.count.get()))
        .collect(Collectors.toList());
  }

}
