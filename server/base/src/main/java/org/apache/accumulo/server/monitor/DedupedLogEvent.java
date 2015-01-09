/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.monitor;

import org.apache.log4j.spi.LoggingEvent;

public class DedupedLogEvent {

  private LoggingEvent event;
  private int count = 0;
  private int hash = -1;

  public DedupedLogEvent(LoggingEvent event) {
    this(event, 1);
  }

  public DedupedLogEvent(LoggingEvent event, int count) {
    this.event = event;
    this.count = count;
  }

  public LoggingEvent getEvent() {
    return event;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public int hashCode() {
    if (hash == -1) {
      String eventId = event.getMDC("application").toString() + ":" + event.getLevel().toString() + ":" + event.getMessage().toString();
      hash = eventId.hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DedupedLogEvent)
      return this.event.equals(((DedupedLogEvent) obj).event);
    return false;
  }

  @Override
  public String toString() {
    return event.getMDC("application").toString() + ":" + event.getLevel().toString() + ":" + event.getMessage().toString();
  }
}
