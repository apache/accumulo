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

import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 */
public class RecoveryStatusInformation {

  private String name;
  private int runtime;
  private double progress;

  public RecoveryStatusInformation() {}

  public RecoveryStatusInformation(String name, int runtime, double progress) {
    this.name = name;
    this.runtime = runtime;
    this.progress = progress;
  }

  public RecoveryStatusInformation(RecoveryStatus recovery) {
    this.name = recovery.name;
    this.runtime = recovery.runtime;
    this.progress = recovery.progress;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("runtime")
  public int getRuntime() {
    return runtime;
  }

  @JsonProperty("runtime")
  public void setRuntime(int runtime) {
    this.runtime = runtime;
  }

  @JsonProperty("progress")
  public double getProgress() {
    return progress;
  }

  @JsonProperty("progress")
  public void setProgress(double progress) {
    this.progress = progress;
  }
}
