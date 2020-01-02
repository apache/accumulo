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

import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Object representing a tablet file entry in the metadata table. Keeps a string of the exact entry
 * of what is in the metadata table for the column qualifier of the
 * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily}
 *
 * As of 2.1, Tablet file paths should now be only absolute URIs with the removal of relative paths
 * in Upgrader9to10.upgradeRelativePaths()
 */
public class TabletFile implements Comparable<TabletFile> {
  // parts of an absolute URI, like "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"
  private final String volume; // hdfs://1.2.3.4/accumulo
  private final TableId tableId; // 2a
  private final String tabletDir; // t-0003
  private final String fileName; // C0004.rf
  private final String metadataEntry;
  private final String suffix; // 2a/t-0003/C0004.rf

  public TabletFile(String metadataEntry) {
    this.metadataEntry = Objects.requireNonNull(metadataEntry);
    String errorMsg = " is missing from tablet file metadata entry: " + metadataEntry;

    Path metaPath = new Path(metadataEntry);

    // use Path object to step backwards from the filename through all the parts
    this.fileName = metaPath.getName();
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(fileName);

    Path tabletDirPath = Objects.requireNonNull(metaPath.getParent(), "Tablet dir" + errorMsg);
    this.tabletDir = tabletDirPath.getName();
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(tabletDir);

    Path tableIdPath = Objects.requireNonNull(tabletDirPath.getParent(), "Table ID" + errorMsg);
    this.tableId = TableId.of(tableIdPath.getName());
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(tableId.canonical());

    Path volumePath = Objects.requireNonNull(
        TabletFileUtil.getVolumeFromFullPath(metaPath, "tables"), "Volume " + errorMsg);
    this.volume = volumePath.toString();

    this.suffix = tableId.canonical() + "/" + tabletDir + "/" + fileName;
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

  /**
   * Exact string that is stored the metadata table
   */
  public String getMetadataEntry() {
    return metadataEntry;
  }

  /**
   * Suffix made from: "tableId.canonical() + "/" + tabletDir + "/" + fileName" This is used for
   * comparison and sorting.
   */
  public String getSuffix() {
    return suffix;
  }

  public Text meta() {
    return new Text(metadataEntry);
  }

  public Path path() {
    return new Path(metadataEntry);
  }

  @Override
  public int compareTo(TabletFile o) {
    if (suffix.equals(o.suffix)) {
      return 0;
    } else {
      return suffix.compareTo(o.getSuffix());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TabletFile)
      return suffix.equals(((TabletFile) obj).getSuffix());
    return false;
  }

  @Override
  public int hashCode() {
    return suffix.hashCode();
  }
}
