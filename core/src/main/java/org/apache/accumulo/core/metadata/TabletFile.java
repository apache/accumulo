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

import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Object representing a tablet file that may exist in the metadata table. This class is used for
 * reading and opening tablet files. It is also used when inserting new tablet files. When a new
 * file is inserted, the {@link #insert()} method is called and returns a {@link StoredTabletFile}
 * For situations where a tablet file needs to be updated or deleted in the metadata, a
 * {@link StoredTabletFile} is required.
 * <p>
 * As of 2.1, Tablet file paths should now be only absolute URIs with the removal of relative paths
 * in Upgrader9to10.upgradeRelativePaths()
 */
public class TabletFile implements Comparable<TabletFile> {
  // parts of an absolute URI, like "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"
  private final TabletDirectory tabletDir; // hdfs://1.2.3.4/accumulo/tables/2a/t-0003
  private final String fileName; // C0004.rf
  protected final Path metaPath;
  private final String normalizedPath;

  private static final Logger log = LoggerFactory.getLogger(TabletFile.class);

  /**
   * Construct new tablet file using a Path. Used in the case where we had to use Path object to
   * qualify an absolute path or create a new file.
   */
  public TabletFile(Path metaPath) {
    this.metaPath = Objects.requireNonNull(metaPath);
    String errorMsg = "Missing or invalid part of tablet file metadata entry: " + metaPath;
    log.trace("Parsing TabletFile from {}", metaPath);

    // use Path object to step backwards from the filename through all the parts
    this.fileName = metaPath.getName();
    ValidationUtil.validateFileName(fileName);

    Path tabletDirPath = Objects.requireNonNull(metaPath.getParent(), errorMsg);

    Path tableIdPath = Objects.requireNonNull(tabletDirPath.getParent(), errorMsg);
    var id = tableIdPath.getName();

    Path tablePath = Objects.requireNonNull(tableIdPath.getParent(), errorMsg);
    String tpString = "/" + tablePath.getName();
    Preconditions.checkArgument(tpString.equals(HDFS_TABLES_DIR), errorMsg);

    Path volumePath = Objects.requireNonNull(tablePath.getParent(), errorMsg);
    Preconditions.checkArgument(volumePath.toUri().getScheme() != null, errorMsg);
    var volume = volumePath.toString();

    this.tabletDir = new TabletDirectory(volume, TableId.of(id), tabletDirPath.getName());
    this.normalizedPath = tabletDir.getNormalizedPath() + "/" + fileName;
  }

  public String getVolume() {
    return tabletDir.getVolume();
  }

  public TableId getTableId() {
    return tabletDir.getTableId();
  }

  public String getTabletDir() {
    return tabletDir.getTabletDir();
  }

  public String getFileName() {
    return fileName;
  }

  /**
   * Return a string for opening and reading the tablet file. Doesn't have to be exact string in
   * metadata.
   */
  public String getPathStr() {
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
   * New file was written to metadata so return a StoredTabletFile
   */
  public StoredTabletFile insert() {
    return new StoredTabletFile(normalizedPath);
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
