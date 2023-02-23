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
package org.apache.accumulo.core.util;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilWaitThread {
  private static final Logger log = LoggerFactory.getLogger(UtilWaitThread.class);

  /**
   * Sleep for specified duration in milliseconds. If the sleep is interrupted, a message is logged
   * and the interrupt flag is reset, but otherwise the interrupt is ignored.
   * <p>
   *
   * @deprecated Use {@link UtilWaitThread#sleep(long, TimeUnit)} instead.
   */
  @Deprecated(since = "3.0.0")
  public static void sleep(long millis) {
    sleep(millis, MICROSECONDS);
  }

  /**
   * Sleep for a specific duration. If an interrupt occurs during the sleep, the interrupt flag is
   * reset and the functions return false so caller can take appropriate action by testing the
   * return value or calling isInterrupted() on the current thread.
   *
   * @param duration the sleep time
   * @param unit the sleep units
   * @return success of the sleep - true if the sleep completed, false if the sleep was interrupted.
   */
  public static boolean sleep(final long duration, final TimeUnit unit) {
    try {
      unit.sleep(duration);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("{}", e.getMessage(), e);
      return false;
    }
  }
}
