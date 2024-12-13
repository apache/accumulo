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

public class TabletMergeability implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final TabletMergeability NEVER = new TabletMergeability(Duration.ofNanos(-1));
  public static final TabletMergeability NOW = new TabletMergeability(Duration.ofNanos(0));

  private final Duration delay;

  private TabletMergeability(Duration delay) {
    this.delay = Objects.requireNonNull(delay);
  }

  public boolean isNever() {
    return this.delay.isNegative();
  }

  public boolean isNow() {
    return this.delay.isZero();
  }

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
    return "TabletMergeability=AFTER:" + delay.toNanos();
  }

  public static TabletMergeability from(Duration delay) {
    Preconditions.checkArgument(delay.toNanos() >= -1,
        "Duration of delay must be -1, 0, or a positive offset.");
    return new TabletMergeability(delay);
  }

  public static TabletMergeability after(Duration delay) {
    Preconditions.checkArgument(delay.toNanos() > 0, "Duration of delay must be greater than 0.");
    return new TabletMergeability(delay);
  }

}
