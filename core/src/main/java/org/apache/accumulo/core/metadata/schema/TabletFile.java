/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.metadata.schema;

import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Object representing a tablet file entry in the metadata table. Keeps a string of the exact entry
 * of what is in the metadata table for the column qualifier of the
 * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily}
 * Validates the full URI form: "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"
 */
public class TabletFile implements Comparable<TabletFile> {
  private String volume; // hdfs://1.2.3.4/accumulo
  // /tables/
  private TableId tableId; // 2a
  private String tabletDir; // t-0003
  private String fileName; // C0004.rf

  // exact string that is stored the metadata table
  private String metadataEntry;
  private Path metadataPath;

  public TabletFile(String metadataEntry) {
    this.metadataPath = new Path(metadataEntry);
    this.metadataEntry = Objects.requireNonNull(metadataEntry);
    this.fileName = metadataPath.getName();
    // TODO validate filename

    Path tabletDirPath = metadataPath.getParent();
    this.tabletDir = tabletDirPath.getName();
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(tabletDir);

    Path tableIdPath = tabletDirPath.getParent();
    this.tableId = TableId.of(tableIdPath.getName());
    // TODO validate tableId

    Path volumePath = TabletFileUtil.getVolumeFromFullPath(metadataPath, "tables");
    TabletFileUtil.validateVolume(volumePath);
    this.volume = volumePath.toString();
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

  public String getFileName() {
    return fileName;
  }

  public String getMetadataEntry() {
    return metadataEntry;
  }

  public Text meta() {
    return new Text(metadataEntry);
  }

  public Path path() {
    return metadataPath;
  }

  @Override
  public int compareTo(TabletFile o) {
    if (metadataEntry.equals(o.metadataEntry)) {
      return 0;
    } else {
      return metadataEntry.compareTo(o.getMetadataEntry());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TabletFile)
      return metadataEntry.equals(((TabletFile) obj).getMetadataEntry());
    return false;
  }

  @Override
  public int hashCode() {
    return metadataEntry.hashCode();
  }

  @Override
  public String toString() {
    return metadataEntry;
  }
}
