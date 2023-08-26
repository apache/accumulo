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
package org.apache.accumulo.tserver.tablet;

import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;

public abstract class DatafileTransaction {

  protected final long ts = System.currentTimeMillis();

  public void apply(Set<StoredTabletFile> files) {}

  public Date getDate() {
    return Date.from(Instant.ofEpochSecond(ts));
  }

  public static class Compacted extends DatafileTransaction {
    private final Set<StoredTabletFile> compactedFiles = new HashSet<>();
    private final Optional<StoredTabletFile> destination;

    public Compacted(Set<StoredTabletFile> files, Optional<StoredTabletFile> destination) {
      this.compactedFiles.addAll(files);
      this.destination = destination;
    }

    @Override
    public void apply(Set<StoredTabletFile> files) {
      files.removeAll(compactedFiles);
      if (destination.isPresent()) {
        files.add(destination.orElseThrow());
      }
    }

    @Override
    public String toString() {
      return String.format("%s: Compacted %s into %s", getDate(), compactedFiles, destination);
    }
  }

  static class Flushed extends DatafileTransaction {
    private final Optional<StoredTabletFile> flushFile;

    public Flushed(Optional<StoredTabletFile> flushFile) {
      this.flushFile = flushFile;
    }

    public Flushed() {
      this.flushFile = null;
    }

    @Override
    public void apply(Set<StoredTabletFile> files) {
      if (flushFile.isPresent()) {
        files.add(flushFile.orElseThrow());
      }
    }

    @Override
    public String toString() {
      return String.format("%s: Flushed into %s", getDate(), flushFile);
    }
  }

  static class BulkImported extends DatafileTransaction {
    private final StoredTabletFile importFile;

    public BulkImported(StoredTabletFile importFile) {
      this.importFile = importFile;
    }

    @Override
    public void apply(Set<StoredTabletFile> files) {
      files.add(importFile);
    }

    @Override
    public String toString() {
      return String.format("%s: Imported %s", getDate(), importFile);
    }
  }
}
