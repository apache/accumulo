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

import org.apache.accumulo.core.gc.thrift.GCStatus;

/**
 *
 * Responsible for grouping files and wals into a JSON object
 *
 * @since 2.0.0
 *
 */
public class GarbageCollectorStatus {

  private static final GarbageCollectorStatus EMPTY = new GarbageCollectorStatus();

  // variable names become JSON key
  public GarbageCollection files = new GarbageCollection();
  public GarbageCollection wals = new GarbageCollection();

  public GarbageCollectorStatus() {}

  /**
   * Groups gc status into files and wals
   *
   * @param status
   *          garbace collector status
   */
  public GarbageCollectorStatus(GCStatus status) {
    if (null != status) {
      files = new GarbageCollection(status.last, status.current);
      wals = new GarbageCollection(status.lastLog, status.currentLog);
    }
  }

  public static GarbageCollectorStatus getEmpty() {
    return EMPTY;
  }
}
