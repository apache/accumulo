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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Generates a list of traces grouped by type
 *
 * @since 2.0.0
 *
 */
public class TraceType {

  // Variable names become JSON objects
  public String traceType;
  public List<TracesForTypeInformation> traces = new ArrayList<>();

  public TraceType() {}

  /**
   * Creates a new list grouped by type
   *
   * @param type
   *          Type of the trace group
   */
  public TraceType(String type) {
    this.traceType = type;
    this.traces = new ArrayList<>();
  }

  /**
   * Adds a new trace to the list
   *
   * @param traces
   *          Trace to add
   */
  public void addTrace(TracesForTypeInformation traces) {
    this.traces.add(traces);
  }
}
