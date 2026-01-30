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
package org.apache.accumulo.core.gc;

import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.fs.Path;

/**
 * A GC reference used for streaming and delete markers. This type is a file. Subclass is a
 * directory.
 */
public class ReferenceFile implements Reference, Comparable<ReferenceFile> {
  // parts of an absolute URI, like "hdfs://1.2.3.4/accumulo/tables/2a/t-0003"
  public final TableId tableId; // 2a
  public final boolean isScan;
  public final boolean isDirectory;

  // the exact path from the file reference string that is stored in the metadata
  protected final String metadataPath;

  protected ReferenceFile(TableId tableId, String metadataPath, boolean isScan,
      boolean isDirectory) {
    this.tableId = Objects.requireNonNull(tableId);
    this.metadataPath = Objects.requireNonNull(metadataPath);
    this.isScan = isScan;
    this.isDirectory = isDirectory;
  }

  public static ReferenceFile forFile(TableId tableId, StoredTabletFile tabletFile) {
    return new ReferenceFile(tableId, tabletFile.getMetadataPath(), false, false);
  }

  public static ReferenceFile forFile(TableId tableId, Path metadataPathPath) {
    return new ReferenceFile(tableId, metadataPathPath.toString(), false, false);
  }

  public static ReferenceFile forScan(TableId tableId, ScanServerRefTabletFile tabletFile) {
    return new ReferenceFile(tableId, tabletFile.getNormalizedPathStr(), true, false);
  }

  public static ReferenceFile forScan(TableId tableId, StoredTabletFile tabletFile) {
    return new ReferenceFile(tableId, tabletFile.getMetadataPath(), true, false);
  }

  public static ReferenceFile forScan(TableId tableId, Path metadataPathPath) {
    return new ReferenceFile(tableId, metadataPathPath.toString(), true, false);
  }

  public static ReferenceFile forDirectory(TableId tableId, String dirName) {
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(dirName);
    return new ReferenceFile(tableId, dirName, false, true);
  }

  @Override
  public boolean isDirectory() {
    return isDirectory;
  }

  @Override
  public boolean isScan() {
    return isScan;
  }

  @Override
  public TableId getTableId() {
    return tableId;
  }

  @Override
  public String getMetadataPath() {
    return metadataPath;
  }

  @Override
  public int compareTo(ReferenceFile that) {
    if (equals(that)) {
      return 0;
    } else {
      return this.metadataPath.compareTo(that.metadataPath);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ReferenceFile other = (ReferenceFile) obj;
    return metadataPath.equals(other.metadataPath);
  }

  @Override
  public int hashCode() {
    return this.metadataPath.hashCode();
  }

  @Override
  public String toString() {
    return "Reference [id=" + tableId + ", ref=" + metadataPath + "]";
  }

}
