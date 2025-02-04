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
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.time.SteadyTime;

import com.google.common.base.Preconditions;

public class TabletMergeabilityMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final TabletMergeabilityMetadata NEVER =
      new TabletMergeabilityMetadata(TabletMergeability.never());;

  private final TabletMergeability tabletMergeability;
  private final SteadyTime steadyTime;

  private TabletMergeabilityMetadata(TabletMergeability tabletMergeability, SteadyTime steadyTime) {
    this.tabletMergeability = Objects.requireNonNull(tabletMergeability);
    this.steadyTime = steadyTime;
    // This makes sure that SteadyTime is set if TabletMergeability has a delay, and is null
    // if TabletMergeability is NEVER as we don't need to store it in that case
    Preconditions.checkArgument(tabletMergeability.isNever() == (steadyTime == null),
        "SteadyTime must be set if and only if TabletMergeability delay is >= 0");
  }

  private TabletMergeabilityMetadata(TabletMergeability tabletMergeability) {
    this(tabletMergeability, null);
  }

  public TabletMergeability getTabletMergeability() {
    return tabletMergeability;
  }

  public boolean isNever() {
    return tabletMergeability.isNever();
  }

  public boolean isAlways() {
    return tabletMergeability.isAlways();
  }

  public Optional<Duration> getDelay() {
    return tabletMergeability.getDelay();
  }

  public Optional<SteadyTime> getSteadyTime() {
    return Optional.ofNullable(steadyTime);
  }

  public boolean isMergeable(SteadyTime currentTime) {
    if (tabletMergeability.isNever()) {
      return false;
    }
    // Steady time should never be null unless TabletMergeability is NEVER
    Preconditions.checkState(steadyTime != null, "SteadyTime should be set");
    return TabletMergeabilityUtil.isMergeable(steadyTime.getDuration(),
        tabletMergeability.getDelay().orElseThrow(), currentTime.getDuration());
  }

  private static class GSonData {
    boolean never;
    Long delay;
    Long steadyTime;
  }

  String toJson() {
    GSonData jData = new GSonData();
    jData.never = tabletMergeability.isNever();
    jData.delay = tabletMergeability.getDelay().map(Duration::toNanos).orElse(null);
    jData.steadyTime = steadyTime != null ? steadyTime.getNanos() : null;
    return GSON.get().toJson(jData);
  }

  static TabletMergeabilityMetadata fromJson(String json) {
    GSonData jData = GSON.get().fromJson(json, GSonData.class);
    if (jData.never) {
      Preconditions.checkArgument(jData.delay == null && jData.steadyTime == null,
          "delay and steadyTime should be null if mergeability 'never' is true");
    } else {
      Preconditions.checkArgument(jData.delay != null && jData.steadyTime != null,
          "delay and steadyTime should both be set if mergeability 'never' is false");
    }
    TabletMergeability tabletMergeability = jData.never ? TabletMergeability.never()
        : TabletMergeability.after(Duration.ofNanos(jData.delay));
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

  public static TabletMergeabilityMetadata never() {
    return NEVER;
  }

  public static TabletMergeabilityMetadata always(SteadyTime currentTime) {
    return new TabletMergeabilityMetadata(TabletMergeability.always(), currentTime);
  }

  public static TabletMergeabilityMetadata after(Duration delay, SteadyTime currentTime) {
    return new TabletMergeabilityMetadata(TabletMergeability.after(delay), currentTime);
  }

  public static TabletMergeabilityMetadata toMetadata(TabletMergeability mergeability,
      SteadyTime currentTime) {
    if (mergeability.isNever()) {
      return TabletMergeabilityMetadata.never();
    }
    return after(mergeability.getDelay().orElseThrow(), currentTime);
  }

  public static Value toValue(TabletMergeabilityMetadata tmm) {
    return new Value(tmm.toJson());
  }

  public static TabletMergeabilityMetadata fromValue(Value value) {
    return TabletMergeabilityMetadata.fromJson(value.toString());
  }
}
