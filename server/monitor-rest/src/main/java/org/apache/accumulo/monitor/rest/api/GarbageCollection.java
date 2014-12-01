/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.monitor.rest.api;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class GarbageCollection {
  public static final GarbageCollection EMPTY = new GarbageCollection();

  protected GarbageCollectorCycle last = new GarbageCollectorCycle(), current = new GarbageCollectorCycle();

  public GarbageCollection() {}

  public GarbageCollection(GcCycleStats last, GcCycleStats current) {
    this.last = new GarbageCollectorCycle(last);
    this.current = new GarbageCollectorCycle(current);
  }

  public GarbageCollection(GarbageCollectorCycle last, GarbageCollectorCycle current) {
    this.last = last;
    this.current = current;
  }

  @JsonProperty("lastCycle")
  public GarbageCollectorCycle getLastCycle() {
    return last;
  }

  @JsonProperty("lastCycle")
  public void setLastCycle(GarbageCollectorCycle last) {
    this.last = last;
  }

  @JsonProperty("currentCycle")
  public GarbageCollectorCycle getCurrentCycle() {
    return current;
  }

  @JsonProperty("currentCycle")
  public void setCurrentCycle(GarbageCollectorCycle current) {
    this.current = current;
  }
}
