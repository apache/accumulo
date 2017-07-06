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
package org.apache.accumulo.monitor.rest.trace;

import org.apache.accumulo.core.master.thrift.RecoveryStatus;

/**
 *
 * Generates a recovery status
 *
 * @since 2.0.0
 *
 */
public class RecoveryStatusInformation {

  // Variable names become JSON keys
  public String name;
  public Integer runtime;
  public Double progress;

  public RecoveryStatusInformation() {}

  /**
   * Stores recovery information
   *
   * @param name
   *          Name of the table
   * @param runtime
   *          Runtime of the recovery
   * @param progress
   *          Progress of the recovery
   */
  public RecoveryStatusInformation(String name, Integer runtime, Double progress) {
    this.name = name;
    this.runtime = runtime;
    this.progress = progress;
  }

  /**
   * Stores recovery information
   *
   * @param recovery
   *          Recovery status to obtain name, runtime, and progress
   */
  public RecoveryStatusInformation(RecoveryStatus recovery) {
    this.name = recovery.name;
    this.runtime = recovery.runtime;
    this.progress = recovery.progress;
  }
}
