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
package org.apache.accumulo.master;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventCoordinator {

  private static final Logger log =
      LoggerFactory.getLogger(EventCoordinator.class);

  private final Lock eventCoordinatorLock = new ReentrantLock();
  private final Condition eventReceived = eventCoordinatorLock.newCondition();

  private final LongAdder eventCounter = new LongAdder();

  long waitForEvents(final long millis, final long lastEvent) {
    long nanos = TimeUnit.MILLISECONDS.toNanos(millis);
    eventCoordinatorLock.lock();
    try {
      while (lastEvent == eventCounter.sum()) {
        if (nanos <= 0L) {
          return eventCounter.sum();
        }
        nanos = eventReceived.awaitNanos(nanos);
      }
    } catch (InterruptedException e) {
      log.debug("Uninterruptable", e);
    } finally {
      eventCoordinatorLock.unlock();
    }
    return eventCounter.sum();
  }

  public void event(final String msg, final Object... args) {
    log.info(String.format(msg, args));

    eventCoordinatorLock.lock();
    try {
      eventCounter.increment();
      eventReceived.signalAll();
    } finally {
      eventCoordinatorLock.unlock();
    }
  }

  public Listener getListener() {
    return new Listener();
  }

  public class Listener {
    long lastEvent;

    Listener() {
      lastEvent = eventCounter.sum();
    }

    public void waitForEvents(long millis) {
      lastEvent = EventCoordinator.this.waitForEvents(millis, lastEvent);
    }
  }
}
