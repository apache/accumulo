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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.gc.thrift.GCStatus;

/**
 * Responsible for grouping files and wals into a JSON object
 *
 * @since 2.0.0
 */
public class GarbageCollectorStatus {

  // variable names become JSON key
  public List<GarbageCollectorStats> stats = new ArrayList<>();

  /**
   * Groups gc status into files and wals
   *
   * @param status garbage collector status
   */
  public GarbageCollectorStatus(GCStatus status) {
    if (status != null) {
      stats.add(new GarbageCollectorStats("Current GC", status.current));
      stats.add(new GarbageCollectorStats("Last GC", status.last));
      stats.add(new GarbageCollectorStats("Current WAL", status.currentLog));
      stats.add(new GarbageCollectorStats("Last WAL", status.lastLog));
    }
  }
}
