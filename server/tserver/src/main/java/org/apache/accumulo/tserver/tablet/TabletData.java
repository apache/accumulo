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
package org.apache.accumulo.tserver.tablet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.tabletserver.log.LogEntry;

/*
 * Basic information needed to create a tablet.
 */
public class TabletData {
  private MetadataTime time = null;
  private SortedMap<StoredTabletFile,DataFileValue> dataFiles = new TreeMap<>();
  private List<LogEntry> logEntries = new ArrayList<>();
  private HashSet<StoredTabletFile> scanFiles = new HashSet<>();
  private long flushID = -1;
  private Location lastLocation = null;
  private String directoryName = null;
  private final TabletHostingGoal goal;

  // Read tablet data from metadata tables
  public TabletData(TabletMetadata meta) {

    this.time = meta.getTime();
    this.flushID = meta.getFlushId().orElse(-1);
    this.directoryName = meta.getDirName();
    this.logEntries.addAll(meta.getLogs());
    scanFiles.addAll(meta.getScans());

    if (meta.getLast() != null) {
      this.lastLocation = meta.getLast();
    }

    dataFiles.putAll(meta.getFilesMap());

    this.goal = meta.getHostingGoal();
  }

  public MetadataTime getTime() {
    return time;
  }

  public SortedMap<StoredTabletFile,DataFileValue> getDataFiles() {
    return dataFiles;
  }

  public List<LogEntry> getLogEntries() {
    return logEntries;
  }

  public HashSet<StoredTabletFile> getScanFiles() {
    return scanFiles;
  }

  public long getFlushID() {
    return flushID;
  }

  public Location getLastLocation() {
    return lastLocation;
  }

  public String getDirectoryName() {
    return directoryName;
  }

  public TabletHostingGoal getHostingGoal() {
    return goal;
  }
}
