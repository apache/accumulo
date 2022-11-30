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

import java.util.ArrayList;
import java.util.List;

/**
 * BulkImport stores the bulk import and tserver bulk imports
 *
 * @since 2.0.0
 */
public class BulkImport {

  // Variable names become JSON key
  public List<BulkImportInformation> bulkImport = new ArrayList<>();
  public List<TabletServerBulkImportInformation> tabletServerBulkImport = new ArrayList<>();

  /**
   * Adds a new bulk import to the array
   *
   * @param bulkImport new bulk import to add
   */
  public void addBulkImport(BulkImportInformation bulkImport) {
    this.bulkImport.add(bulkImport);
  }

  /**
   * Adds a new tserver bulk import to the array
   *
   * @param tabletServerBulkImport new tserver bulk import to add
   */
  public void addTabletServerBulkImport(TabletServerBulkImportInformation tabletServerBulkImport) {
    this.tabletServerBulkImport.add(tabletServerBulkImport);
  }
}
