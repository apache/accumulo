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
package org.apache.accumulo.monitor.rest.gc;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics about one type of garbage collection
 *
 * @since 2.1.0
 */
public class GarbageCollectorStats {
  private static final Logger log = LoggerFactory.getLogger(GarbageCollectorStats.class);

  // Variable names become JSON key
  public final String type;
  public final long finished;
  public final long candidates;
  public final long inUse;
  public final long deleted;
  public final long errors;
  public final long duration;

  /**
   * Creates a new garbage collector cycle
   *
   * @param thriftStats used to find cycle information
   */
  public GarbageCollectorStats(String type, GcCycleStats thriftStats) {
    log.info("Creating {} stats using thriftStats = {}", type, thriftStats);
    this.type = type;
    this.finished = thriftStats.finished;
    this.candidates = thriftStats.candidates;
    this.inUse = thriftStats.inUse;
    this.deleted = thriftStats.deleted;
    this.errors = thriftStats.errors;
    this.duration = this.finished - thriftStats.started;
  }
}
