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
package org.apache.accumulo.core.metadata.schema;

import java.util.Objects;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Represents the exact string stored in the metadata table "file" column family. Currently the
 * metadata only contains a file reference but could be expanded in the future.
 */
public class TabletFileMetadataEntry implements Comparable<TabletFileMetadataEntry> {

  private final Path filePath;
  private final String filePathString;

  public TabletFileMetadataEntry(final Path filePath) {
    this.filePath = Objects.requireNonNull(filePath);
    // Cache the string value of the filePath, so we don't have to keep converting.
    this.filePathString = filePath.toString();
  }

  /**
   * The file path portion of the metadata
   *
   * @return The file path
   */
  public Path getFilePath() {
    return filePath;
  }

  /**
   * String representation of the file path
   *
   * @return file path string
   */
  public String getFilePathString() {
    return filePathString;
  }

  /**
   * Exact representation of what is in the Metadata table Currently this is just a file path but
   * may expand
   *
   * @return The exact metadata string
   */
  public String getMetaString() {
    return filePathString;
  }

  /**
   * Exact {@link Text} representation of the metadat Generated from {@link #getMetaString()}
   *
   * @return The exact metadata as Text
   */
  public Text getMetaText() {
    return new Text(filePathString);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletFileMetadataEntry that = (TabletFileMetadataEntry) o;
    return Objects.equals(filePathString, that.filePathString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePathString);
  }

  @Override
  public int compareTo(TabletFileMetadataEntry o) {
    return filePathString.compareTo(o.filePathString);
  }

  @Override
  public String toString() {
    return filePathString;
  }

  /**
   * Utility to create a new TabletFileMetadataEntry from the exact String in the Metadata table
   *
   * @param metadataEntry Exact string for the metadataEntry
   * @return A TabletFileMetadataEntry created from the metadata string
   */
  public static TabletFileMetadataEntry of(String metadataEntry) {
    return new TabletFileMetadataEntry(new Path(Objects.requireNonNull(metadataEntry)));
  }

}
