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

/**
 *
 * Generates a list of traces for an ID
 *
 * @since 2.0.0
 *
 */
public class TraceInformation {

  // Variable names become JSON keys
  public int level;
  public long time;
  public long start;
  public long spanID;
  public String location, name;
  public AddlInformation addlData;

  public TraceInformation() {}

  /**
   * Generates a trace
   *
   * @param level
   *          Level of the trace
   * @param time
   *          Amount of time the trace ran
   * @param start
   *          Start time of the trace
   * @param spanID
   *          ID of the span
   * @param location
   *          Location of the trace
   * @param name
   *          Name of the trace
   * @param addlData
   *          Additional data for the trace
   */
  public TraceInformation(int level, long time, long start, long spanID, String location, String name, AddlInformation addlData) {
    this.level = level;
    this.time = time;
    this.start = start;
    this.spanID = spanID;
    this.location = location;
    this.name = name;
    this.addlData = addlData;
  }
}
