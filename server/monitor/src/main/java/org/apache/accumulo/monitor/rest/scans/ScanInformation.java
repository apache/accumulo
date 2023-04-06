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
package org.apache.accumulo.monitor.rest.scans;

import org.apache.accumulo.monitor.Monitor;

/**
 * Generates a scan JSON object
 *
 * @since 2.0.0
 */
public class ScanInformation {

  // Variable names become JSON keys
  public String server;

  public long fetched;
  public long scanCount;
  public Long oldestScan;

  public ScanInformation() {}

  /**
   * Stores new scan information
   */
  public ScanInformation(String tserverName, Monitor.ScanStats stats) {
    this.server = tserverName;
    this.fetched = stats.fetched;
    this.scanCount = stats.scanCount;
    this.oldestScan = stats.oldestScan;
  }
}
