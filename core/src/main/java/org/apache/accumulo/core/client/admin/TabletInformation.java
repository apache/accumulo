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
package org.apache.accumulo.core.client.admin;

import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.NumUtil;

public class TabletInformation {

  private final String tableName;
  private final String endRow;
  private final String prevEndRow;
  private final int numFiles;
  private final int numWalLogs;
  private final long numEntries;
  private final long size;
  private final String status;
  private final String location;
  private final String dir;
  private final TableId tableId;
  private final TabletHostingGoal goal;

  public TabletInformation(String tableName, TableId tableId, String endRow, String prevEndRow,
      int numFiles, int numWalLogs, long numEntries, long size, String status, String location,
      String dir, TabletHostingGoal goal) {
    this.tableName = tableName;
    this.tableId = tableId;
    this.endRow = endRow;
    this.prevEndRow = prevEndRow;
    this.numFiles = numFiles;
    this.numWalLogs = numWalLogs;
    this.numEntries = numEntries;
    this.size = size;
    this.status = status;
    this.location = location;
    this.dir = dir;
    this.goal = goal;
  }

  public String getEndRow() {
    return Objects.requireNonNullElse(this.endRow, "+INF");
  }

  public String getStartRow() {
    return Objects.requireNonNullElse(this.prevEndRow, "-INF");
  }

  public String getTableId() {
    return tableId.canonical();
  }

  public String getTablet() {
    return getStartRow() + " " + getEndRow();
  }

  public String getTableName() {
    return this.tableName;
  }

  public int getNumFiles() {
    return this.numFiles;
  }

  public int getNumWalLogs() {
    return this.numWalLogs;
  }

  public long getNumEntries() {
    return this.numEntries;
  }

  public String getNumEntries(final boolean humanReadable) {
    if (humanReadable) {
      return NumUtil.bigNumberForQuantity(numEntries);
    }
    return String.format("%,d", numEntries);
  }

  public long getSize() {
    return this.size;
  }

  public String getSize(final boolean humanReadable) {
    if (humanReadable) {
      return NumUtil.bigNumberForSize(size);
    }
    return String.format("%,d", size);
  }

  public String getStatus() {
    return this.status;
  }

  public String getLocation() {
    return this.location;
  }

  public String getTabletDir() {
    return this.dir;
  }

  public TabletHostingGoal getHostingGoal() {
    return this.goal;
  }

  @Override
  public String toString() {
    return "TabletInformation{tableName='" + tableName + '\'' + ", numFiles=" + numFiles
        + ", numWalLogs=" + numWalLogs + ", numEntries=" + numEntries + ", size=" + size
        + ", status='" + status + '\'' + ", location='" + location + '\'' + ", dir='" + dir + '\''
        + ", tableId=" + tableId + ", tablet=" + getTablet() + ", goal=" + goal + '}';
  }

  public static final String header =
      String.format("%-4s %-15s %-5s %-5s %-9s %-9s %-10s %-30s %-5s %-20s %-20s %-10s", "NUM",
          "TABLET_DIR", "FILES", "WALS", "ENTRIES", "SIZE", "STATUS", "LOCATION", "ID",
          "START (Exclusive)", "END", "GOAL");

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletInformation that = (TabletInformation) o;
    return numFiles == that.numFiles && numWalLogs == that.numWalLogs
        && numEntries == that.numEntries && size == that.size
        && Objects.equals(tableName, that.tableName) && Objects.equals(endRow, that.endRow)
        && Objects.equals(prevEndRow, that.prevEndRow) && Objects.equals(status, that.status)
        && Objects.equals(location, that.location) && Objects.equals(dir, that.dir)
        && Objects.equals(tableId, that.tableId) && goal == that.goal;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, endRow, prevEndRow, numFiles, numWalLogs, numEntries, size,
        status, location, dir, tableId, goal);
  }
}
