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

import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.hadoop.io.Text;

public class TabletInformationImpl implements TabletInformation {

  private final TabletMetadata tabletMetadata;
  private long estimatedSize;
  private long estimatedEntries;
  private final String tabletState;

  public TabletInformationImpl(TabletMetadata tabletMetadata, String tabletState) {
    this.tabletMetadata = tabletMetadata;
    estimatedEntries = 0L;
    estimatedSize = 0L;
    for (DataFileValue dfv : tabletMetadata.getFilesMap().values()) {
      estimatedEntries += dfv.getNumEntries();
      estimatedSize += dfv.getSize();
    }
    this.tabletState = tabletState;
  }

  public Text getEndRow() {
    return tabletMetadata.getEndRow();
  }

  public Text getStartRow() {
    return tabletMetadata.getPrevEndRow();
  }

  public TableId getTableId() {
    return tabletMetadata.getTableId();
  }

  public TabletId getTabletId() {
    return new TabletIdImpl(tabletMetadata.getExtent());
  }

  public int getNumFiles() {
    return tabletMetadata.getFilesMap().size();
  }

  public int getNumWalLogs() {
    return tabletMetadata.getLogs().size();
  }

  public long getEstimatedEntries() {
    return this.estimatedEntries;
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  public String getTabletState() {
    return tabletState;
  }

  public Optional<Location> getLocation() {
    return Optional.ofNullable(tabletMetadata.getLocation());
  }

  public String getTabletDir() {
    return tabletMetadata.getDirName();
  }

  public TabletHostingGoal getHostingGoal() {
    return tabletMetadata.getHostingGoal();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletInformationImpl that = (TabletInformationImpl) o;
    return estimatedSize == that.estimatedSize && estimatedEntries == that.estimatedEntries
        && Objects.equals(tabletMetadata, that.tabletMetadata)
        && Objects.equals(tabletState, that.tabletState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tabletMetadata, estimatedSize, estimatedEntries, tabletState);
  }

  @Override
  public String toString() {
    return "TabletInformationImpl{tabletMetadata=" + tabletMetadata + ", estimatedSize="
        + estimatedSize + ", estimatedEntries=" + estimatedEntries + ", tabletState='" + tabletState
        + '\'' + '}';
  }
}
