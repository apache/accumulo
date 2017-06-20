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
 * Metrics about a single cycle of the garbage collector
 *
 * @since 2.0.0
 *
 */
public class GarbageCollectorCycle {

  private static final GarbageCollectorCycle EMPTY = new GarbageCollectorCycle();

  // Variable names become JSON key
  public long started = 0l;
  public long finished = 0l;
  public long candidates = 0l;
  public long inUse = 0l;
  public long deleted = 0l;
  public long errors = 0l;

  public GarbageCollectorCycle() {}

  /**
   * Creates a new garbage collector cycle
   *
   * @param thriftStats
   *          used to find cycle information
   */
  public GarbageCollectorCycle(GcCycleStats thriftStats) {
    this.started = thriftStats.started;
    this.finished = thriftStats.finished;
    this.candidates = thriftStats.candidates;
    this.inUse = thriftStats.inUse;
    this.deleted = thriftStats.deleted;
    this.errors = thriftStats.errors;
  }

  public static GarbageCollectorCycle getEmpty() {
    return EMPTY;
  }
}
