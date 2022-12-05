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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventCoordinator {

  private static final Logger log = LoggerFactory.getLogger(EventCoordinator.class);
  long eventCounter = 0;

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

  public synchronized void event(String msg, Object... args) {
    log.info(String.format(msg, args));
    eventCounter++;
    notifyAll();
  }

  public Listener getListener() {
    return new Listener();
  }

  public class Listener {
    long lastEvent;

    Listener() {
      lastEvent = eventCounter;
    }

    public void waitForEvents(long millis) {
      lastEvent = EventCoordinator.this.waitForEvents(millis, lastEvent);
    }
  }

}
