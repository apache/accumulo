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
package org.apache.accumulo.core.util.time;

import java.time.Duration;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class implements a strong type for System.nanoTime() that offers the limited operations that
 * can be performed on a nanoTime. See the System.nanoTime() javadoc for details - specifically
 * these values are meaningful only when the difference between two such values, obtained within the
 * same instance of a Java virtual machine, are computed.
 */
public final class NanoTime implements Comparable<NanoTime> {
  // In the System.nanoTime javadoc it describes the returned value as the "nanoseconds since some
  // fixed but arbitrary origin time (perhaps in the future, so values may be negative)". This
  // variable name is derived from that where AO is arbitrary origin.
  private final long nanosSinceAO;

  // This method should only be called by test inorder to test edge conditions, that is why it is
  // package private. Calling this outside of test makes it hard to reason about the correctness of
  // using this class.
  @VisibleForTesting
  NanoTime(long ntsao) {
    this.nanosSinceAO = ntsao;
  }

  /**
   * @return this.nanoTime - other.nanoTime as a Duration
   */
  public Duration subtract(NanoTime other) {
    return Duration.ofNanos(nanosSinceAO - other.nanosSinceAO);
  }

  /**
   * Determines the amount of time that has elapsed since this object was created relative to the
   * current nanoTime.
   *
   * @return System.nanoTime() - this.nanoTime
   */
  public Duration elapsed() {
    return Duration.ofNanos(System.nanoTime() - nanosSinceAO);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof NanoTime) {
      return nanosSinceAO == ((NanoTime) other).nanosSinceAO;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(nanosSinceAO);
  }

  @Override
  public int compareTo(NanoTime other) {
    // All operations w/ nanoTimes must use differences, can not directly compare. This is because a
    // nano time of Long.MAX_VALUE -10 is considered less than Long.MAX_VALUE +10
    long diff = nanosSinceAO - other.nanosSinceAO;

    if (diff < 0) {
      return -1;
    } else if (diff > 0) {
      return 1;
    } else {
      return 0;
    }
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
