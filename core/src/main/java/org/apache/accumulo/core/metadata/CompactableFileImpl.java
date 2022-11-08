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

import java.net.URI;
import java.util.Objects;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;

public class CompactableFileImpl implements CompactableFile {

  private final StoredTabletFile storedTabletFile;
  private final DataFileValue dataFileValue;

  public CompactableFileImpl(URI uri, long size, long entries) {
    this.storedTabletFile = new StoredTabletFile(uri.toString());
    this.dataFileValue = new DataFileValue(size, entries);
  }

  public CompactableFileImpl(StoredTabletFile storedTabletFile, DataFileValue dataFileValue) {
    this.storedTabletFile = Objects.requireNonNull(storedTabletFile);
    this.dataFileValue = Objects.requireNonNull(dataFileValue);
  }

  @Override
  public URI getUri() {
    return storedTabletFile.getPath().toUri();
  }

  @Override
  public String getFileName() {
    return storedTabletFile.getFileName();
  }

  @Override
  public long getEstimatedSize() {
    return dataFileValue.getSize();
  }

  @Override
  public long getEstimatedEntries() {
    return dataFileValue.getNumEntries();
  }

  public StoredTabletFile getStoredTabletFile() {
    return storedTabletFile;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactableFileImpl) {
      var ocfi = (CompactableFileImpl) o;

      return storedTabletFile.equals(ocfi.storedTabletFile);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return storedTabletFile.hashCode();
  }

  public static StoredTabletFile toStoredTabletFile(CompactableFile cf) {
    if (cf instanceof CompactableFileImpl) {
      return ((CompactableFileImpl) cf).storedTabletFile;
    } else {
      throw new IllegalArgumentException("Can not convert " + cf.getClass());
    }
  }

  @Override
  public String toString() {
    return "[" + storedTabletFile.getFileName() + ", " + dataFileValue + "]";
  }
}
