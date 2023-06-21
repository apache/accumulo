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

import java.net.URI;
import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.commons.lang3.StringUtils;
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

  public static class FileParts {

    // parts of an absolute URI, like "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"
    // volume: hdfs://1.2.3.4/accumulo
    // tableId: 2a
    // tabletDir: t-0003
    // fileName: C0004.rf
    // normalizedPath: hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf
    private final String volume;
    private final TableId tableId;
    private final String tabletDir;
    private final String fileName;
    private final String normalizedPath;

    public FileParts(String volume, TableId tableId, String tabletDir, String fileName,
        String normalizedPath) {
      this.volume = volume;
      this.tableId = tableId;
      this.tabletDir = tabletDir;
      this.fileName = fileName;
      this.normalizedPath = normalizedPath;
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

    public String getNormalizedPath() {
      return normalizedPath;
    }

  }

  private static String constructErrorMsg(Path filePath) {
    return "Missing or invalid part of tablet file metadata entry: " + filePath;
  }

  public static FileParts parsePath(Path filePath) {
    // File name construct: <volume>/<tablePath>/<tableId>/<tablet>/<file>
    // Example: hdfs://namenode:9020/accumulo/tables/1/default_tablet/F00001.rf
    final URI uri = filePath.toUri();

    // validate that this is a fully qualified uri
    Preconditions.checkArgument(uri.getScheme() != null, constructErrorMsg(filePath));

    final String path = uri.getPath(); // ex: /accumulo/tables/1/default_tablet/F00001.rf
    final String[] parts = path.split("/");
    final int numParts = parts.length; // should contain tables, 1, default_tablet, F00001.rf

    if (numParts < 4) {
      throw new IllegalArgumentException(constructErrorMsg(filePath));
    }

    final String fileName = parts[numParts - 1];
    final String tabletDirectory = parts[numParts - 2];
    final TableId tableId = TableId.of(parts[numParts - 3]);
    final String tablesPath = parts[numParts - 4];

    // determine where file path starts, the rest is the volume
    final String computedFilePath =
        HDFS_TABLES_DIR + "/" + tableId.canonical() + "/" + tabletDirectory + "/" + fileName;
    final String uriString = uri.toString();
    int idx = uriString.lastIndexOf(computedFilePath);

    if (idx == -1) {
      throw new IllegalArgumentException(constructErrorMsg(filePath));
    }

    // The volume is the beginning portion of the uri up to the start
    // of the file path.
    final String volume = uriString.substring(0, idx);

    if (StringUtils.isBlank(fileName) || StringUtils.isBlank(tabletDirectory)
        || StringUtils.isBlank(tablesPath) || StringUtils.isBlank(volume)) {
      throw new IllegalArgumentException(constructErrorMsg(filePath));
    }
    ValidationUtil.validateFileName(fileName);
    Preconditions.checkArgument(tablesPath.equals(HDFS_TABLES_DIR_NAME),
        "tables directory name is not " + HDFS_TABLES_DIR_NAME + ", is " + tablesPath);

    final String normalizedPath = volume + computedFilePath;

    if (!normalizedPath.equals(uriString)) {
      throw new RuntimeException("Error parsing file path, " + normalizedPath + " != " + uriString);
    }

    return new FileParts(volume, tableId, tabletDirectory, fileName, normalizedPath);

  }

  protected final Path metaPath;
  private final FileParts parts;

  private static final Logger log = LoggerFactory.getLogger(TabletFile.class);
  private static final String HDFS_TABLES_DIR_NAME = HDFS_TABLES_DIR.substring(1);

  /**
   * Construct new tablet file using a Path. Used in the case where we had to use Path object to
   * qualify an absolute path or create a new file.
   */
  public TabletFile(Path metaPath) {
    this.metaPath = Objects.requireNonNull(metaPath);
    log.trace("Parsing TabletFile from {}", metaPath);
    parts = parsePath(metaPath);
  }

  public String getVolume() {
    return parts.getVolume();
  }

  public TableId getTableId() {
    return parts.getTableId();
  }

  public String getTabletDir() {
    return parts.getTabletDir();
  }

  public String getFileName() {
    return parts.getFileName();
  }

  /**
   * Return a string for opening and reading the tablet file. Doesn't have to be exact string in
   * metadata.
   */
  public String getPathStr() {
    return parts.getNormalizedPath();
  }

  /**
   * Return a string for inserting a new tablet file.
   */
  public String getMetaInsert() {
    return parts.getNormalizedPath();
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
    return new StoredTabletFile(parts.getNormalizedPath());
  }

  public Path getPath() {
    return metaPath;
  }

  @Override
  public int compareTo(TabletFile o) {
    if (equals(o)) {
      return 0;
    } else {
      return parts.getNormalizedPath().compareTo(o.parts.getNormalizedPath());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TabletFile) {
      TabletFile that = (TabletFile) obj;
      return parts.getNormalizedPath().equals(that.parts.getNormalizedPath());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return parts.getNormalizedPath().hashCode();
  }

  @Override
  public String toString() {
    return parts.getNormalizedPath();
  }
}
