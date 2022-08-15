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
package org.apache.accumulo.minicluster;

/**
 * @since 1.6.0
 */
public enum ServerType {
  /**
   * @deprecated Use {@link #MANAGER} instead
   */
  @Deprecated(since = "2.1.0", forRemoval = true)
  MASTER("Master"),
  MANAGER("Manager"),
  ZOOKEEPER("ZooKeeper"),
  TABLET_SERVER("TServer"),
  GARBAGE_COLLECTOR("GC"),
  COMPACTION_COORDINATOR("CompactionCoordinator"),
  COMPACTOR("Compactor"),
  /**
   * @deprecated Accumulo-managed Tracer service was removed
   */
  @Deprecated(since = "2.1.0", forRemoval = true)
  TRACER("Tracer"),
  MONITOR("Monitor"),
  SCAN_SERVER("SServer");

  private final String prettyPrint;

  public String prettyPrint() {
    return prettyPrint;
  }

  ServerType(String prettyPrint) {
    this.prettyPrint = prettyPrint;
  }
}
