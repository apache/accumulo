/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.coordinator;

public class ExternalCompactionMetrics {

  private long started = 0;
  private long running = 0;
  private long completed = 0;
  private long failed = 0;

  public long getStarted() {
    return started;
  }

  public void setStarted(long started) {
    this.started = started;
  }

  public void incrementStarted() {
    this.started++;
  }

  public long getRunning() {
    return running;
  }

  public void setRunning(long running) {
    this.running = running;
  }

  public long getCompleted() {
    return completed;
  }

  public void setCompleted(long completed) {
    this.completed = completed;
  }

  public void incrementCompleted() {
    this.completed++;
  }

  public long getFailed() {
    return failed;
  }

  public void setFailed(long failed) {
    this.failed = failed;
  }

  public void incrementFailed() {
    this.failed++;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("started: ").append(started);
    buf.append(", running: ").append(running);
    buf.append(", completed: ").append(completed);
    buf.append(", failed: ").append(failed);
    return buf.toString();
  }

}
