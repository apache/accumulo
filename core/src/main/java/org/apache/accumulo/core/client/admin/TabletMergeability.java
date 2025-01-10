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

import com.google.common.base.Preconditions;

/**
 * @since 4.0.0
 */
public class TabletMergeability implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final TabletMergeability NEVER = new TabletMergeability(Duration.ofNanos(-1));
  private static final TabletMergeability NOW = new TabletMergeability(Duration.ZERO);

  private final Duration delay;

  private TabletMergeability(Duration delay) {
    this.delay = Objects.requireNonNull(delay);
  }

  /**
   * Determines if the configured delay signals a tablet is never eligible to be automatically
   * merged. (Has a delay of -1)
   *
   * @return true if never mergeable, else false
   */
  public boolean isNever() {
    return this.delay.isNegative();
  }

  /**
   * Determines if the configured delay signals a tablet is always eligible to be automatically
   * merged now. (Has a delay of 0)
   *
   * @return true if always mergeable now, else false
   */
  public boolean isNow() {
    return this.delay.isZero();
  }

  /**
   * Determines if the configured delay signals a tablet has a configured delay before being
   * eligible to be automatically merged. (Has a positive delay)
   *
   * @return true if there is a configured delay, else false
   */
  public boolean isDelayed() {
    return delay.toNanos() > 0;
  }

  /**
   * Returns the duration of the delay which is one of:
   *
   * <ul>
   * <li>-1 (never)</li>
   * <li>0 (now)</li>
   * <li>positive delay</li>
   * </ul>
   *
   * @return the configured mergeability delay
   */
  public Duration getDelay() {
    return delay;
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
    if (isNow()) {
      return "TabletMergeability=NOW";
    } else if (isNever()) {
      return "TabletMergeability=NEVER";
    }
    return "TabletMergeability=AFTER:" + delay.toMillis() + "ms";
  }

  /**
   * Signifies that a tablet is never eligible to be automatically merged.
   *
   * @return a {@link TabletMergeability} with a delay of -1 signaling never merge
   */
  public static TabletMergeability never() {
    return NEVER;
  }

  /**
   * Signifies that a tablet is eligible now to be automatically merged
   *
   * @return a {@link TabletMergeability} with a delay of 0 signaling never merge
   */
  public static TabletMergeability now() {
    return NOW;
  }

  /**
   * Creates a {@link TabletMergeability} from the given delay. The duration must be one of
   * <ul>
   * <li>-1 (never)</li>
   * <li>0 (now)</li>
   * <li>positive delay</li>
   * </ul>
   *
   * @param delay the duration of the delay
   *
   * @return a {@link TabletMergeability} from the given delay.
   */
  public static TabletMergeability from(Duration delay) {
    Preconditions.checkArgument(delay.toNanos() >= -1,
        "Duration of delay must be -1, 0, or a positive delay.");
    return new TabletMergeability(delay);
  }

  /**
   * Creates a {@link TabletMergeability} that signals a tablet has a delay to a point in the future
   * before it is automatically eligible to be merged. The duration must be positive value.
   *
   * @param delay the duration of the delay
   *
   * @return a {@link TabletMergeability} from the given delay.
   */
  public static TabletMergeability after(Duration delay) {
    Preconditions.checkArgument(delay.toNanos() > 0, "Duration of delay must be greater than 0.");
    return new TabletMergeability(delay);
  }

}
