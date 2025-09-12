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
package org.apache.accumulo.core.clientImpl;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.TabletMergeabilityInfo;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;

import com.google.common.base.Suppliers;

public class TabletInformationImpl implements TabletInformation {

  private final TabletMetadata tabletMetadata;

  private final Supplier<String> tabletState;
  private final Supplier<Duration> currentTime;
  private final Supplier<FileInfo> fileInfo;

  private class FileInfo {
    private final long estimatedSize;
    private final long estimatedEntries;

    FileInfo() {
      long estimatedEntries = 0L;
      long estimatedSize = 0L;
      for (DataFileValue dfv : tabletMetadata.getFilesMap().values()) {
        estimatedEntries += dfv.getNumEntries();
        estimatedSize += dfv.getSize();
      }
      this.estimatedEntries = estimatedEntries;
      this.estimatedSize = estimatedSize;
    }
  }

  public TabletInformationImpl(TabletMetadata tabletMetadata, Supplier<String> tabletState,
      Supplier<Duration> currentTime) {
    this.tabletMetadata = tabletMetadata;
    this.fileInfo = Suppliers.memoize(FileInfo::new);
    this.tabletState = tabletState;
    this.currentTime = Objects.requireNonNull(currentTime);
  }

  @Override
  public TabletId getTabletId() {
    return new TabletIdImpl(tabletMetadata.getExtent());
  }

  @Override
  public int getNumFiles() {
    return tabletMetadata.getFilesMap().size();
  }

  @Override
  public int getNumWalLogs() {
    return tabletMetadata.getLogs().size();
  }

  @Override
  public long getEstimatedEntries() {
    return this.fileInfo.get().estimatedEntries;
  }

  @Override
  public long getEstimatedSize() {
    return fileInfo.get().estimatedSize;
  }

  @Override
  public String getTabletState() {
    return tabletState.get();
  }

  @Override
  public Optional<String> getLocation() {
    Location location = tabletMetadata.getLocation();
    return location == null ? Optional.empty() : Optional
        .of(location.getType() + ":" + location.getServerInstance().getServer().toHostPortString());
  }

  @Override
  public String getTabletDir() {
    return tabletMetadata.getDirName();
  }

  @Override
  public TabletAvailability getTabletAvailability() {
    return tabletMetadata.getTabletAvailability();
  }

  @Override
  public TabletMergeabilityInfo getTabletMergeabilityInfo() {
    return TabletMergeabilityUtil.toInfo(tabletMetadata.getTabletMergeability(), currentTime);
  }

  @Override
  public String toString() {
    return "TabletInformationImpl{tabletMetadata=" + tabletMetadata + '}';
  }
}
