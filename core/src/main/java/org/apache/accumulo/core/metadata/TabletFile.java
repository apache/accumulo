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
package org.apache.accumulo.core.metadata;

import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Object representing a tablet file entry in the metadata table. Keeps a string of the exact entry
 * of what is in the metadata table for the column qualifier of the
 * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily}
 * <p>
 * As of 2.1, Tablet file paths should now be only absolute URIs with the removal of relative paths
 * in Upgrader9to10.upgradeRelativePaths()
 */
public class TabletFile implements Comparable<TabletFile> {
  // parts of an absolute URI, like "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"
  private final String volume; // hdfs://1.2.3.4/accumulo
  private final TableId tableId; // 2a
  private final String tabletDir; // t-0003
  private final String fileName; // C0004.rf
  private final Path metaPath;
  private final String normalizedPath;

  private Optional<String> metadataEntry;

  /**
   * Construct new tablet file using a Path. Used in the case where we had to use Path object to
   * qualify an absolute path or create a new file.
   */
  public TabletFile(Path metaPath) {
    this(metaPath, Optional.empty());
  }

  /**
   * Construct a tablet file using the string read from the metadata. Preserve the exact string so
   * the entry can be deleted.
   */
  public TabletFile(String metadataEntry) {
    this(new Path(metadataEntry), Optional.of(metadataEntry));
  }

  private TabletFile(Path metaPath, Optional<String> originalMetaEntry) {
    this.metadataEntry = originalMetaEntry;
    this.metaPath = Objects.requireNonNull(metaPath);
    String errorMsg = "Missing or invalid part of tablet file metadata entry: " + metaPath;

    // use Path object to step backwards from the filename through all the parts
    this.fileName = metaPath.getName();
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(fileName);

    Path tabletDirPath = Objects.requireNonNull(metaPath.getParent(), errorMsg);
    this.tabletDir = tabletDirPath.getName();
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(tabletDir);

    Path tableIdPath = Objects.requireNonNull(tabletDirPath.getParent(), errorMsg);
    this.tableId = TableId.of(tableIdPath.getName());
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(tableId.canonical());

    Path tablePath = Objects.requireNonNull(tableIdPath.getParent(), errorMsg);
    String tpString = "/" + tablePath.getName();
    Preconditions.checkArgument(tpString.equals(Constants.HDFS_TABLES_DIR), errorMsg);

    Path volumePath = Objects.requireNonNull(tablePath.getParent(), errorMsg);
    Preconditions.checkArgument(volumePath.toUri().getScheme() != null, errorMsg);
    this.volume = volumePath.toString();

    this.normalizedPath = volume + Constants.HDFS_TABLES_DIR + "/" + tableId.canonical() + "/"
        + tabletDir + "/" + fileName;
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
   * Return a string for opening and reading the tablet file. Doesn't have to be exact string in metadata.
   */
  public String getMetaRead() {
    return normalizedPath;
  }

  /**
   * Return a string for inserting a new tablet file.
   */
  public String getMetaInsert() {
    return normalizedPath;
  }

  /**
   * Return a new Text object of {@link #getMetaInsert()}
   */
  public Text getMetaInsertText() {
    return new Text(getMetaInsert());
  }

  /**
   * New file was written to metadata so update the entry.
   */
  public void inserted() {
    this.metadataEntry = Optional.of(normalizedPath);
  }

  /**
   * Return the exact string that is stored in the metadata table. This is important for updating
   * and deleting metadata entries. If the exact string is not used, erroneous entries can pollute
   * the metadata table.
   */
  public String getMetaUpdateDelete() {
    if (metadataEntry.isEmpty())
      throw new IllegalStateException(
          "TabletFile " + metaPath + " does not have a metadata entry.");
    return metadataEntry.get();
  }

  /**
   * Return a new Text object of {@link #getMetaUpdateDelete()}
   */
  public Text getMetaUpdateDeleteText() {
    return new Text(getMetaUpdateDelete());
  }

  public Path getPath() {
    return metaPath;
  }

  @Override
  public int compareTo(TabletFile o) {
    if (equals(o)) {
      return 0;
    } else {
      return normalizedPath.compareTo(o.normalizedPath);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TabletFile) {
      TabletFile that = (TabletFile) obj;
      return normalizedPath.equals(that.normalizedPath);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return normalizedPath.hashCode();
  }

  @Override
  public String toString() {
    return normalizedPath;
  }
}
