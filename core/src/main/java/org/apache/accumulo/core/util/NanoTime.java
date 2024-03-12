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

import java.time.Duration;

/**
 * This class implements a strong type for System.nanoTime() that offers the limited operations that
 * can be performed on a nanoTime.
 */
public final class NanoTime {
  private final long nanoTime;

  private NanoTime(long nanoTime) {
    this.nanoTime = nanoTime;
  }

  /**
   * @return this.nanoTime - other.nanoTime as a Duration
   */
  public Duration subtract(NanoTime other) {
    return Duration.ofNanos(nanoTime - other.nanoTime);
  }

  /**
   * Determines the amount of time that has elapsed since this object was created relative to the
   * current nanoTime.
   *
   * @return System.nanoTime() - this.nanoTime
   */
  public Duration elapsed() {
    return Duration.ofNanos(System.nanoTime() - nanoTime);
  }

  /**
   * @return a NanoTime created using System.nanoTime()
   */
  public static NanoTime now() {
    return new NanoTime(System.nanoTime());
  }

  /**
   * @return a NanoTime created using System.nanoTime() + duration.toNanos()
   */
  public static NanoTime nowPlus(Duration duration) {
    return new NanoTime(System.nanoTime() + duration.toNanos());
  }
}
