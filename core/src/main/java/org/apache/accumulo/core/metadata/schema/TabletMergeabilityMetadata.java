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
package org.apache.accumulo.core.metadata.schema;

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.time.SteadyTime;

import com.google.common.base.Preconditions;

public class TabletMergeabilityMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final TabletMergeabilityMetadata NEVER =
      new TabletMergeabilityMetadata(TabletMergeability.NEVER);
  public static final TabletMergeabilityMetadata NOW =
      new TabletMergeabilityMetadata(TabletMergeability.NOW);

  private final TabletMergeability tabletMergeability;
  private final SteadyTime steadyTime;

  private TabletMergeabilityMetadata(TabletMergeability tabletMergeability, SteadyTime steadyTime) {
    this.tabletMergeability = Objects.requireNonNull(tabletMergeability);
    this.steadyTime = steadyTime;
    Preconditions.checkArgument(tabletMergeability.isFuture() == (steadyTime != null),
        "SteadyTime must be set if and only if TabletMergeability delay is greater than 0.");
  }

  private TabletMergeabilityMetadata(TabletMergeability tabletMergeability) {
    this(tabletMergeability, null);
  }

  public TabletMergeability getTabletMergeability() {
    return tabletMergeability;
  }

  public Optional<SteadyTime> getSteadyTime() {
    return Optional.ofNullable(steadyTime);
  }

  public boolean isMergeable(SteadyTime currentTime) {
    if (tabletMergeability.isNever()) {
      return false;
    }
    return currentTime.getDuration().compareTo(totalDelay()) >= 0;
  }

  private Duration totalDelay() {
    return steadyTime != null ? steadyTime.getDuration().plus(tabletMergeability.getDelay())
        : tabletMergeability.getDelay();
  }

  private static class GSonData {
    long delay;
    Long steadyTime;
  }

  String toJson() {
    GSonData jData = new GSonData();
    jData.delay = tabletMergeability.getDelay().toNanos();
    jData.steadyTime = steadyTime != null ? steadyTime.getNanos() : null;
    return GSON.get().toJson(jData);
  }

  static TabletMergeabilityMetadata fromJson(String json) {
    GSonData jData = GSON.get().fromJson(json, GSonData.class);
    TabletMergeability tabletMergeability = TabletMergeability.from(Duration.ofNanos(jData.delay));
    SteadyTime steadyTime =
        jData.steadyTime != null ? SteadyTime.from(jData.steadyTime, TimeUnit.NANOSECONDS) : null;
    return new TabletMergeabilityMetadata(tabletMergeability, steadyTime);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletMergeabilityMetadata that = (TabletMergeabilityMetadata) o;
    return Objects.equals(tabletMergeability, that.tabletMergeability)
        && Objects.equals(steadyTime, that.steadyTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tabletMergeability, steadyTime);
  }

  @Override
  public String toString() {
    return "TabletMergeabilityMetadata{" + tabletMergeability + ", " + steadyTime + '}';
  }

  public static TabletMergeabilityMetadata after(Duration delay, SteadyTime currentTime) {
    return from(TabletMergeability.after(delay), currentTime);
  }

  public static TabletMergeabilityMetadata from(TabletMergeability tm, SteadyTime currentTime) {
    return new TabletMergeabilityMetadata(tm, currentTime);
  }

  public static Value toValue(TabletMergeabilityMetadata tmm) {
    return new Value(tmm.toJson());
  }

  public static TabletMergeabilityMetadata fromValue(Value value) {
    return TabletMergeabilityMetadata.fromJson(value.toString());
  }
}
