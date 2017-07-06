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
package org.apache.accumulo.monitor.rest.scans;

import org.apache.accumulo.core.master.thrift.TabletServerStatus;

/**
 *
 * Generates a scan JSON object
 *
 * @since 2.0.0
 *
 */
public class ScanInformation {

  // Variable names become JSON keys
  public String server;

  public long scanCount;
  public Long oldestScan;

  public ScanInformation() {}

  /**
   * Stores new scan information
   *
   * @param tserverInfo
   *          status of the tserver
   * @param scanCount
   *          number of scans
   * @param oldestScan
   *          time of oldest scan
   */
  public ScanInformation(TabletServerStatus tserverInfo, long scanCount, Long oldestScan) {
    this.server = tserverInfo.getName();
    this.scanCount = scanCount;
    this.oldestScan = oldestScan;
  }
}
