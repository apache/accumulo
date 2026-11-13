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

import java.lang.Object;
import java.util.Comparator;
import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;

public class GcCandidate implements Comparable<GcCandidate> {
  private final Long uid;
  private final String path;
  private final Type type;

  public enum Type {
    TABLET_FILE, TABLET_DIR, TABLE_DIR, BULK_DIR;
  }

  public GcCandidate(String path, long uid, Type type) {
    this.path = path;
    this.uid = uid;
    this.type = type;
  }

  public GcCandidate(String path, Type type) {
    this.path = path;
    this.uid = null;
    this.type = type;
  }

  public static GcCandidate forTabletFile(StoredTabletFile file) {
    return new GcCandidate(file.getMetadataPath(), Type.TABLET_FILE);
  }

  public static GcCandidate forTabletDir(TableId tableId, String dir) {
    return new GcCandidate("/" + tableId + "/" + dir, Type.TABLET_DIR);
  }

  public static GcCandidate forBulkDir(String bulkDir) {
    return new GcCandidate(bulkDir, Type.BULK_DIR);
  }

  public static GcCandidate forTableDir(TableId tableId) {
    return new GcCandidate("/" + tableId, Type.TABLE_DIR);
  }

  public Type getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public long getUid() {
    return uid;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, uid, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof GcCandidate candidate) {
      return Objects.equals(this.uid, candidate.uid) && this.path.equals(candidate.getPath())
          && type == candidate.type;
    }
    return false;
  }

  private static final Comparator<Long> UID_CMP = Comparator.nullsFirst(Long::compareTo);

  @Override
  public int compareTo(GcCandidate candidate) {
    var cmp = type.compareTo(candidate.type);
    if (cmp == 0) {
      cmp = this.path.compareTo(candidate.getPath());
      if (cmp == 0) {
        cmp = UID_CMP.compare(this.uid, candidate.uid);
      }
    }
    return cmp;
  }

  @Override
  public String toString() {
    return "type: " + type + ", path: " + path + ", UUID: " + uid;
  }
}
