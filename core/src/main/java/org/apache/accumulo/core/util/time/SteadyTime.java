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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * SteadyTime represents an approximation of the total duration of time this cluster has had a
 * Manager. Because this represents an elapsed time it is guaranteed to not be negative. SteadyTime
 * is not expected to represent real world date times, its main use is for computing deltas similar
 * System.nanoTime but across JVM processes.
 */
public class SteadyTime implements Comparable<SteadyTime> {

  private final Duration time;

  private SteadyTime(Duration time) {
    Preconditions.checkArgument(!time.isNegative(), "SteadyTime '%s' should not be negative.",
        time.toNanos());
    this.time = time;
  }

  public long getMillis() {
    return time.toMillis();
  }

  public long getNanos() {
    return time.toNanos();
  }

  public Duration getDuration() {
    return time;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SteadyTime that = (SteadyTime) o;
    return Objects.equals(time, that.time);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(time);
  }

  @Override
  public int compareTo(SteadyTime other) {
    return time.compareTo(other.time);
  }

  public static SteadyTime from(long time, TimeUnit unit) {
    return new SteadyTime(Duration.of(time, unit.toChronoUnit()));
  }

  public static SteadyTime from(Duration time) {
    return new SteadyTime(time);
  }
}
