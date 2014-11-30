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
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Metrics about a single cycle of the garbage collector
 */
public class GarbageCollectorCycle {
  public static final GarbageCollectorCycle EMPTY = new GarbageCollectorCycle();

  protected long started = 0, finished = 0;
  protected long candidates = 0, inUse = 0, deleted = 0;
  protected long errors = 0;

  public GarbageCollectorCycle() { }

  public GarbageCollectorCycle(GcCycleStats thriftStats) {
    this.started = thriftStats.started;
    this.finished = thriftStats.finished;
    this.candidates = thriftStats.candidates;
    this.inUse = thriftStats.inUse;
    this.deleted = thriftStats.deleted;
    this.errors = thriftStats.errors;
  }

  @JsonProperty("started")
  public long getStarted() {
    return started;
  }

  @JsonProperty("started")
  public void setStarted(long started) {
    this.started = started;
  }

  @JsonProperty("finished")
  public long getFinished() {
    return finished;
  }

  @JsonProperty("finished")
  public void setFinished(long finished) {
    this.finished = finished;
  }

  @JsonProperty("candidates")
  public long getCandidates() {
    return candidates;
  }

  @JsonProperty("candidates")
  public void setCandidates(long candidates) {
    this.candidates = candidates;
  }

  @JsonProperty("inUse")
  public long getInUse() {
    return inUse;
  }

  @JsonProperty("inUse")
  public void setInUse(long inUse) {
    this.inUse = inUse;
  }

  @JsonProperty("deleted")
  public long getDeleted() {
    return deleted;
  }

  @JsonProperty("deleted")
  public void setDeleted(long deleted) {
    this.deleted = deleted;
  }

  @JsonProperty("errors")
  public long getErrors() {
    return errors;
  }

  @JsonProperty("errors")
  public void setErrors(long errors) {
    this.errors = errors;
  }
}
