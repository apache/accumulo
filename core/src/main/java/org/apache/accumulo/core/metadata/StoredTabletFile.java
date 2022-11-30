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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

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
public class StoredTabletFile extends TabletFile {
  private final String metadataEntry;

  /**
   * Construct a tablet file using the string read from the metadata. Preserve the exact string so
   * the entry can be deleted.
   */
  public StoredTabletFile(String metadataEntry) {
    super(new Path(metadataEntry));
    this.metadataEntry = metadataEntry;
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
}
