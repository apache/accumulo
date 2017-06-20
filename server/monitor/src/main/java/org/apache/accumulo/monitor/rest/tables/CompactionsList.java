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
package org.apache.accumulo.monitor.rest.tables;

/**
 *
 * Generates a list of compactions as a JSON object
 *
 * @since 2.0.0
 *
 */
public class CompactionsList {

  // Variable names become JSON keys
  public Integer running = null;
  public Integer queued = null;

  public CompactionsList() {}

  /**
   * Generate a compation list
   *
   * @param running
   *          Number of running compactions
   * @param queued
   *          Number of queued compactions
   */
  public CompactionsList(Integer running, Integer queued) {
    this.running = running;
    this.queued = queued;
  }
}
