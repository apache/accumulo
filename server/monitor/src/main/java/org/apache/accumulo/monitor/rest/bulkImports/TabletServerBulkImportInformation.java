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
package org.apache.accumulo.monitor.rest.bulkImports;

import org.apache.accumulo.core.master.thrift.TabletServerStatus;

/**
 * Stores tserver bulk import information
 *
 * @since 2.0.0
 */
public class TabletServerBulkImportInformation {

  // Variable names become JSON key
  public String server;
  public int importSize;
  public long oldestAge;

  public TabletServerBulkImportInformation() {}

  /**
   * Creates a new tserver bulk import object
   *
   * @param server server name
   * @param importSize import size
   * @param oldestAge tserver bulk import age
   */
  public TabletServerBulkImportInformation(TabletServerStatus server, int importSize,
      long oldestAge) {
    this.server = server.getName();
    this.importSize = importSize;
    this.oldestAge = oldestAge;
  }
}
