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
package org.apache.accumulo.core.metadata;

import static org.apache.accumulo.core.Constants.HDFS_TABLES_DIR;

import org.apache.accumulo.core.data.TableId;

/**
 * The Tablet directory that should exist on disk. The {@link #toString()} method only returns the
 * tablet directory itself, the same as {@link #getTabletDir()}, which is just the name of the
 * directory, like "t-0003". For the full directory path, use {@link #getNormalizedPath}.
 */
public class TabletDirectory {
  // parts of an absolute URI, like "hdfs://1.2.3.4/accumulo/tables/2a/t-0003"
  private final String volume; // hdfs://1.2.3.4/accumulo
  private final TableId tableId; // 2a
  private final String tabletDir; // t-0003
  private final String normalizedPath;

  public TabletDirectory(String volume, TableId tableId, String tabletDir) {
    this.volume = volume;
    this.tableId = tableId;
    this.tabletDir = tabletDir;
    this.normalizedPath = volume + HDFS_TABLES_DIR + "/" + tableId.canonical() + "/" + tabletDir;

  }

  public String getVolume() {
    return volume;
  }

  public TableId getTableId() {
    return tableId;
  }

  public String getTabletDir() {
    return tabletDir;
  }

  public String getNormalizedPath() {
    return normalizedPath;
  }

  @Override
  public String toString() {
    return tabletDir;
  }
}
