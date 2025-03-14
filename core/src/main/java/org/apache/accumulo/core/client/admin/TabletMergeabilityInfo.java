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

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;

import com.google.common.base.Preconditions;

/**
 * @since 4.0.0
 */
public class TabletMergeabilityInfo {

  private final TabletMergeability tabletMergeability;
  private final Optional<Duration> insertionTime;
  private final Supplier<Duration> currentTime;

  public TabletMergeabilityInfo(TabletMergeability tabletMergeability,
      Optional<Duration> insertionTime, Supplier<Duration> currentTime) {
    this.tabletMergeability = Objects.requireNonNull(tabletMergeability);
    this.insertionTime = Objects.requireNonNull(insertionTime);
    this.currentTime = Objects.requireNonNull(currentTime);
    // This makes sure that insertionTime is set if TabletMergeability has a delay, and is empty
    // if TabletMergeability is NEVER
    Preconditions.checkArgument(tabletMergeability.isNever() == insertionTime.isEmpty(),
        "insertionTime must not be empty if and only if TabletMergeability delay is >= 0");
    insertionTime.ifPresent(
        it -> Preconditions.checkArgument(!it.isNegative(), "insertionTime must not be negative"));
  }

  /**
   * @return the TabletMergeability
   */
  public TabletMergeability getTabletMergeability() {
    return tabletMergeability;
  }

  /**
   * If the TabletMergeability is configured with a delay this returns an estimate of the elapsed
   * time since the delay was initially set. Returns optional.empty() if the tablet was configured
   * with a TabletMergeability of never.
   */
  public Optional<Duration> getElapsed() {
    // @formatter:off
    /*
     * It's possible the "current time" is read from the manager and then a tablets insertion time
     * is read later and this could cause a negative read, in this case set the elapsed time to
     * zero.  For example:
     *
     * 1. Thread_1 starts reading tablet info objects
     * 2. Thread_1 reads TabletMergeabilityInfo for tablet_X. This causes the memoized
     *    current steady time supplier to be set to 5.
     * 3. Thread_2 updates TabletMergeability for tablet_Y. It sets the insertion steady
     *    time for the update to 7.
     * 4. Thread_1 reads TabletMergeabilityInfo for tablet_Y. To compute elapsed it
     *    does 5 - 7 = -2. In this case, just return 0.
     */
    // @formatter:on
    return insertionTime.map(it -> currentTime.get().minus(it))
        .map(elapsed -> elapsed.isNegative() ? Duration.ZERO : elapsed);
  }

  /**
   * If the TabletMergeability is configured with a delay this returns an estimate of the remaining
   * time since the delay was initially set. Returns optional.empty() if the tablet was configured
   * with a TabletMergeability of never.
   */
  public Optional<Duration> getRemaining() {
    // Delay should always be set if insertionTime is not empty
    // getElapsed() and getDelay() are guaranteed to both be >= 0
    return getElapsed().map(elapsed -> tabletMergeability.getDelay().orElseThrow().minus(elapsed))
        .map(remaining -> remaining.isNegative() ? Duration.ZERO : remaining);
  }

  /**
   * Returns an Optional duration of the configured {@link TabletMergeability} delay which is one
   * of:
   *
   * <ul>
   * <li>empty (never)</li>
   * <li>0 (now)</li>
   * <li>positive delay</li>
   * </ul>
   *
   * @return the configured mergeability delay or empty if mergeability is NEVER
   */
  public Optional<Duration> getDelay() {
    return tabletMergeability.getDelay();
  }

  /**
   * Check if this tablet is eligible to be automatically merged. <br>
   * If TabletMergeability is set to "always", this will return true. <br>
   * If TabletMergeability is set to "never", this will return false. <br>
   * If TabletMergeability is configured with a delay, this will check if enough time has elapsed
   * and return true if it has.
   *
   * @return true if the tablet is eligible for automatic merging, else false
   */
  public boolean isMergeable() {
    if (tabletMergeability.isNever()) {
      return false;
    }
    // The constructor checks that if tabletMergeability is set to a value other
    // than never that the insertionTime is also present
    return TabletMergeabilityUtil.isMergeable(insertionTime.orElseThrow(),
        tabletMergeability.getDelay().orElseThrow(), currentTime.get());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TabletMergeabilityInfo that = (TabletMergeabilityInfo) o;
    return tabletMergeability.equals(that.tabletMergeability)
        && insertionTime.equals(that.insertionTime);
  }

  @Override
  public int hashCode() {
    int result = tabletMergeability.hashCode();
    result = 31 * result + insertionTime.hashCode();
    return result;
  }
}
