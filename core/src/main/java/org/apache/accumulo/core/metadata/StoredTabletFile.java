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

import java.util.Objects;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Suppliers;

/**
 * Object representing a tablet file entry stored in the metadata table. Keeps a string of the exact
 * entry of what is in the metadata table, which is important for updating and deleting metadata
 * entries. If the exact string is not used, erroneous entries can pollute the metadata table. The
 * main column qualifier used can be found:
 * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily}
 * The tablet file entry is also stored for scans and bulk imports.
 * <p>
 * As of 2.1, Tablet file paths should now be only absolute URIs with the removal of relative paths
 * in Upgrader9to10.upgradeRelativePaths()
 */
public class StoredTabletFile extends AbstractTabletFile<StoredTabletFile> {
  private final String metadataEntry;
  private final Supplier<ReferencedTabletFile> referencedTabletFile;

  /**
   * Construct a tablet file using the string read from the metadata. Preserve the exact string so
   * the entry can be deleted.
   * <p>
   * The {@link ReferencedTabletFile} contained in this class will not be created and validated
   * until the first time {@link StoredTabletFile#getTabletFile()} is called.
   */
  public StoredTabletFile(String metadataEntry) {
    this(metadataEntry, false);
  }

  /**
   * Construct a tablet file using the string read from the metadata. Preserve the exact string so
   * the entry can be deleted.
   * <p/>
   * The {@link ReferencedTabletFile} contained in this class will not be created and validated
   * until the first time {@link StoredTabletFile#getTabletFile()} is called.
   *
   * @param metadataEntry Exact string in metadata for the StoredTabletFile
   * @param eagerLoadRefTabletFile whether to immediately create the {@link ReferencedTabletFile}
   */
  public StoredTabletFile(String metadataEntry, boolean eagerLoadRefTabletFile) {
    super(new Path(metadataEntry));
    this.metadataEntry = metadataEntry;
    this.referencedTabletFile = Suppliers.memoize(() -> ReferencedTabletFile.of(getPath()));

    // Immediately load which will trigger validation
    if (eagerLoadRefTabletFile) {
      // Variable here is used to make SpotBugs happy
      var unused = this.referencedTabletFile.get();
    }
  }

  /**
   * Return the exact string that is stored in the metadata table. This is important for updating
   * and deleting metadata entries. If the exact string is not used, erroneous entries can pollute
   * the metadata table.
   */
  public String getMetaUpdateDelete() {
    return metadataEntry;
  }

  /**
   * Return a new Text object of {@link #getMetaUpdateDelete()}
   */
  public Text getMetaUpdateDeleteText() {
    return new Text(getMetaUpdateDelete());
  }

  public ReferencedTabletFile getTabletFile() {
    return referencedTabletFile.get();
  }

  public TableId getTableId() {
    return referencedTabletFile.get().getTableId();
  }

  public String getNormalizedPathStr() {
    return referencedTabletFile.get().getNormalizedPathStr();
  }

  /**
   * Validate that the provided reference matches what is in the metadata table.
   *
   * @param reference the relative path to check against
   */
  public void validate(String reference) {
    if (!metadataEntry.equals(reference)) {
      throw new IllegalStateException("The reference " + reference
          + " does not match what was in the metadata: " + metadataEntry);
    }
  }

  @Override
  public int compareTo(StoredTabletFile o) {
    if (equals(o)) {
      return 0;
    } else {
      return metadataEntry.compareTo(o.metadataEntry);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StoredTabletFile that = (StoredTabletFile) o;
    return Objects.equals(metadataEntry, that.metadataEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataEntry);
  }

  @Override
  public String toString() {
    return metadataEntry;
  }

  public static StoredTabletFile of(final String metadataEntry) {
    return new StoredTabletFile(metadataEntry);
  }

}
