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
package org.apache.accumulo.monitor.rest.gc;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;

/**
 *
 * GarbageCollection is responsible for creating the gc JSON object
 *
 * @since 2.0.0
 *
 */
public class GarbageCollection {

  private static final GarbageCollection EMPTY = new GarbageCollection();

  // Variable names become JSON key
  public GarbageCollectorCycle lastCycle = new GarbageCollectorCycle();
  public GarbageCollectorCycle currentCycle = new GarbageCollectorCycle();

  public GarbageCollection() {}

  /**
   * Creates a new Garbage Collector JSON object
   *
   * @param last
   *          last GC cycle
   * @param current
   *          current GC cycle
   */
  public GarbageCollection(GcCycleStats last, GcCycleStats current) {
    this.lastCycle = new GarbageCollectorCycle(last);
    this.currentCycle = new GarbageCollectorCycle(current);
  }

  /**
   * Creates a new Garbage Collector JSON object
   *
   * @param last
   *          last GC cycle
   * @param current
   *          current GC cycle
   */
  public GarbageCollection(GarbageCollectorCycle last, GarbageCollectorCycle current) {
    this.lastCycle = last;
    this.currentCycle = current;
  }

  public static GarbageCollection getEmpty() {
    return EMPTY;
  }
}
