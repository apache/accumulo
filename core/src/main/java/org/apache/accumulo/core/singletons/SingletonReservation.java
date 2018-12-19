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

package org.apache.accumulo.core.singletons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see SingletonManager#getClientReservation()
 */
public class SingletonReservation implements AutoCloseable {

  // volatile so finalize does not need to synchronize to reliably read
  protected volatile boolean closed = false;

  private static Logger log = LoggerFactory.getLogger(SingletonReservation.class);

  private final Exception e;

  public SingletonReservation() {
    e = new Exception();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    SingletonManager.releaseRerservation();
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      if (!closed) {
        log.warn("An Accumulo Client was garbage collected without being closed.", e);
      }
    } finally {
      super.finalize();
    }
  }

  private static class NoopSingletonReservation extends SingletonReservation {
    NoopSingletonReservation() {
      closed = true;
    }
  }

  private static final SingletonReservation NOOP = new NoopSingletonReservation();

  /**
   * @return A reservation where the close method is a no-op.
   */
  public static SingletonReservation noop() {
    return NOOP;
  }
}
