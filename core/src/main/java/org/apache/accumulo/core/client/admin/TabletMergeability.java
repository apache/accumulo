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
package org.apache.accumulo.core.client.admin;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;

/**
 * @since 4.0.0
 */
public class TabletMergeability implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final TabletMergeability NEVER = new TabletMergeability();
  private static final TabletMergeability ALWAYS = new TabletMergeability(Duration.ZERO);

  private final Duration delay;

  private TabletMergeability(Duration delay) {
    this.delay = delay;
    Preconditions.checkArgument(!delay.isNegative(), "Delay '%s' must not be negative.",
        delay.toNanos());
  }

  // Edge case for NEVER
  private TabletMergeability() {
    this.delay = null;
  }

  /**
   * Determines if the configured delay signals a tablet is never eligible to be automatically
   * merged.
   *
   * @return true if never mergeable, else false
   */
  public boolean isNever() {
    return this.delay == null;
  }

  /**
   * Determines if the configured delay signals a tablet is always eligible to be automatically
   * merged now. (Has a delay of 0)
   *
   * @return true if always mergeable now, else false
   */
  public boolean isAlways() {
    return delay != null && this.delay.isZero();
  }

  /**
   * Returns an Optional duration of the delay which is one of:
   *
   * <ul>
   * <li>empty (never)</li>
   * <li>0 (now)</li>
   * <li>positive delay</li>
   * </ul>
   *
   *
   * @return the configured mergeability delay or empty if mergeability is NEVER
   */
  public Optional<Duration> getDelay() {
    return Optional.ofNullable(delay);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletMergeability that = (TabletMergeability) o;
    return Objects.equals(delay, that.delay);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(delay);
  }

  @Override
  public String toString() {
    if (delay == null) {
      return "TabletMergeability=NEVER";
    }
    return "TabletMergeability=AFTER:" + delay.toMillis() + "ms";
  }

  /**
   * Signifies that a tablet is never eligible to be automatically merged.
   *
   * @return a {@link TabletMergeability} with an empty delay signaling never merge
   */
  public static TabletMergeability never() {
    return NEVER;
  }

  /**
   * Signifies that a tablet is eligible now to be automatically merged
   *
   * @return a {@link TabletMergeability} with a delay of 0 signaling never merge
   */
  public static TabletMergeability always() {
    return ALWAYS;
  }

  /**
   * Creates a {@link TabletMergeability} that signals a tablet has a delay to a point in the future
   * before it is automatically eligible to be merged. The duration must be greater than or equal to
   * 0. A delay of 0 means a Tablet is immediately eligible to be merged.
   *
   * @param delay the duration of the delay
   *
   * @return a {@link TabletMergeability} from the given delay.
   */
  public static TabletMergeability after(Duration delay) {
    // Delay will be validated that it is >=0 inside the constructor
    return new TabletMergeability(delay);
  }

}
